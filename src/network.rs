use std::io::Write;
use std::time::Duration;

use my_raft::bytes::WriteBytes;
use my_raft::config::Config;
use my_raft::network::{ClientRequest, MessageEvent, NetworkInterface};
use my_raft::state_machine::StateMachine;
use nix::errno::Errno;
use nix::sys::socket;
use nix::sys::socket::{AddressFamily, MsgFlags, SockAddr, SockFlag, SockType};
use nix::sys::socket::sockopt::ReceiveTimeout;
use nix::sys::time::{TimeVal, TimeValLike};
use serde::{Deserialize, Serialize};

use crate::{hash, network_name_to_num, num_to_network_name};
use crate::state_machine::{KvStateMachine, SetValueCommand};

const PACKET_SIZE: usize = 65527;

#[derive(Serialize, Deserialize, Debug)]
struct JsonMessage<'a> {
    src: &'a str,
    dst: &'a str,
    leader: &'a str,
    #[serde(flatten)]
    data: JsonMessageType<'a>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
enum JsonMessageType<'a> {
    Redirect { #[serde(rename(deserialize = "MID", serialize = "MID"))] mid: &'a str },
    Fail { #[serde(rename(deserialize = "MID", serialize = "MID"))] mid: &'a str },
    Get { #[serde(rename(deserialize = "MID", serialize = "MID"))] mid: &'a str, key: &'a str },
    #[serde(rename(deserialize = "ok", serialize = "ok"))]
    Ok { #[serde(rename(deserialize = "MID", serialize = "MID"))] mid: &'a str, #[serde(skip_serializing_if = "Option::is_none")] value: Option<&'a str> },
    Put { #[serde(rename(deserialize = "MID", serialize = "MID"))] mid: &'a str, key: &'a str, value: &'a str },
    #[serde(rename(serialize = "raft"))]
    RaftRef { data: &'a [u8] },
    #[serde(rename(deserialize = "raft"))]
    RaftOwned { data: Vec<u8> },
}

pub struct ReadValueRequest {
    key: String,
    mid: String,
}


pub struct Cs3700UnixNetwork {
    our_id: u32,
    our_name: String,
    socket_fd: i32,
    buffer: [u8; PACKET_SIZE],
}

impl Cs3700UnixNetwork {
    pub fn new(our_id: u32) -> Cs3700UnixNetwork {
        let our_name = num_to_network_name(our_id);
        let socket_fd = socket::socket(AddressFamily::Unix, SockType::SeqPacket, SockFlag::empty(), None).unwrap();
        socket::connect(socket_fd, &SockAddr::new_unix::<str>(&our_name).unwrap()).unwrap();
        Cs3700UnixNetwork {
            socket_fd,
            our_name,
            our_id,
            buffer: [0u8; PACKET_SIZE],
        }
    }

    fn send_message_to(&mut self, to: u32, leader_id: Option<u32>, data: JsonMessageType) {
        let leader_name = leader_id.map(|id| num_to_network_name(id));

        let mut writer = self.buffer.as_mut();
        serde_json::to_writer(&mut writer, &JsonMessage {
            src: self.our_name.as_str(),
            dst: &num_to_network_name(to),
            leader: leader_name.as_ref().map(|s| s.as_str()).unwrap_or("FFFF"),
            data,
        }).unwrap();

        let amt = PACKET_SIZE - writer.len();

        socket::send(self.socket_fd, &self.buffer[..amt], MsgFlags::empty()).unwrap();
    }
}

impl NetworkInterface<KvStateMachine> for Cs3700UnixNetwork {
    type ReadRequest = ReadValueRequest;

    fn on_config_update(&mut self, _config: &Config) {}

    fn wait_for_message(&mut self, timeout: Duration, raft_message: &mut Vec<u8>) -> MessageEvent<<KvStateMachine as StateMachine>::Command, Self::ReadRequest> {
        socket::setsockopt(self.socket_fd, ReceiveTimeout, &TimeVal::milliseconds(timeout.as_millis() as i64)).unwrap();

        let amt = match socket::recv(self.socket_fd, &mut self.buffer, MsgFlags::empty()) {
            Ok(amt) if amt == 0 => return MessageEvent::Fail,
            Ok(amt) => amt,
            Err(nix::Error::Sys(Errno::EAGAIN)) => return MessageEvent::Timeout,
            Err(_) => return MessageEvent::Fail
        };

        // let message: JsonMessage = serde_json::from_slice(&self.buffer[..amt]).expect("Invalid JSON message");
        let message: JsonMessage = match serde_json::from_slice(&self.buffer[..amt]) {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("Failed to decode: {}", e);
                eprintln!("{}", String::from_utf8_lossy(&self.buffer[..amt]));
                panic!()
            }
        };

        let src_id = network_name_to_num(message.src);

        match message.data {
            JsonMessageType::Get { mid, key } => {
                let request_id = hash(mid);
                MessageEvent::ClientRead(ClientRequest {
                    request_id,
                    client_id: src_id,
                    data: ReadValueRequest { key: key.to_string(), mid: mid.to_string() },
                })
            }
            JsonMessageType::Put { mid, key, value } => {
                let request_id = hash(mid);
                MessageEvent::ClientCommand(ClientRequest {
                    request_id,
                    client_id: src_id,
                    data: SetValueCommand { key: key.to_string(), value: value.to_string(), mid: mid.to_string() },
                })
            }
            JsonMessageType::RaftOwned { data } => {
                raft_message.write_all(&data).unwrap();
                MessageEvent::Node {
                    src_node_id: src_id,
                }
            }
            _ => unimplemented!()
        }
    }

    fn send_raft_message(&mut self, node: u32, leader_id: Option<u32>, msg: impl WriteBytes) {
        let mut data = [0u8; 4096];
        let amt = msg.write_bytes(data.as_mut()).unwrap();
        self.send_message_to(node, leader_id, JsonMessageType::RaftRef { data: &data[..amt] })
    }

    fn handle_command_applied(&mut self, req: ClientRequest<&<KvStateMachine as StateMachine>::Command>, _state_machine: &KvStateMachine) {
        self.send_message_to(req.client_id, Some(self.our_id), JsonMessageType::Ok { mid: &req.data.mid, value: None });
    }

    fn handle_ready_to_read(&mut self, req: ClientRequest<Self::ReadRequest>, state_machine: &KvStateMachine) {
        let value = Some(state_machine.0.get(&req.data.key).map(|s| s.as_str()).unwrap_or(""));
        self.send_message_to(req.client_id, Some(self.our_id), JsonMessageType::Ok { mid: &req.data.mid, value });
    }

    fn redirect_command_request(&mut self, leader_id: u32, req: ClientRequest<<KvStateMachine as StateMachine>::Command>) {
        self.send_message_to(req.client_id, Some(leader_id), JsonMessageType::Redirect { mid: &req.data.mid });
    }

    fn redirect_read_request(&mut self, leader_id: u32, req: ClientRequest<Self::ReadRequest>) {
        self.send_message_to(req.client_id, Some(leader_id), JsonMessageType::Redirect { mid: &req.data.mid });
    }
}

