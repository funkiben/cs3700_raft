use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use my_raft::config::{Config, NodeAddress};
use my_raft::core::Raft;
use my_raft::state_machine::RaftStateMachine;

use crate::network::Cs3700UnixNetwork;
use crate::state_machine::KvStateMachine;
use crate::storage::RamStorage;

mod storage;
mod state_machine;
mod network;


fn main() {
    let (our_id, nodes) = get_nodes_and_id();

    let init_state_machine = RaftStateMachine {
        inner: KvStateMachine::default(),
        config: Config {
            election_timeout_min: 750,
            election_timeout_range: 250,
            heartbeat_timeout: 500,
            rpc_response_timeout: 20,
            max_entries_in_append_entries: 100,
            max_bytes_in_install_snapshot: 100,
            next_index_decrease_rate: 100,
            snapshot_min_log_size: 200,//u32::max_value(),
            id: our_id,
            nodes,
        },
        client_last_command_ids: Default::default(),
    };

    let network = Cs3700UnixNetwork::new(our_id);

    let mut raft = Raft::new(RamStorage::new(init_state_machine), network);
    raft.start();
}

fn get_nodes_and_id() -> (u32, HashMap<u32, NodeAddress>) {
    let mut args = std::env::args();
    args.next();

    let this_name = args.next().unwrap();

    let mut nodes = HashMap::new();

    let this_id = network_name_to_num(&this_name);
    nodes.insert(this_id, NodeAddress::String(this_name));

    for name in args {
        nodes.insert(network_name_to_num(&name), NodeAddress::String(name));
    }

    (this_id, nodes)
}

pub fn network_name_to_num(name: &str) -> u32 {
    u32::from_str_radix(name, 16).unwrap()
}

pub fn num_to_network_name(n: u32) -> String {
    format!("{:0>4X}", n)
}

pub fn hash(s: &str) -> u32 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    (hasher.finish() >> 32) as u32
}


