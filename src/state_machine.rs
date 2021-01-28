use std::collections::HashMap;
use std::io;
use std::io::Write;

use my_raft::bytes::{BytesWriter, ReadBytes, TryFromBytes, WriteBytes};
use my_raft::state_machine::{RaftStateMachine, StateMachine};

pub struct SetValueCommand {
    pub key: String,
    pub value: String,
    pub mid: String,
}

#[derive(Clone, Default)]
pub struct KvStateMachine(pub HashMap<String, String>);

impl StateMachine for KvStateMachine {
    type Command = SetValueCommand;

    fn apply_command(&mut self, command: &Self::Command) {
        self.0.insert(command.key.clone(), command.value.clone());
    }
}

impl TryFromBytes for KvStateMachine {
    fn try_from_bytes(mut bytes: impl ReadBytes) -> Option<Self> {
        let mut map = HashMap::new();

        let len = bytes.next_u32()?;
        for _ in 0..len {
            let key_len = bytes.next_u32()?;
            let key = String::from_utf8(bytes.next_bytes(key_len as usize)?.to_vec()).unwrap();
            let value_len = bytes.next_u32()?;
            let value = String::from_utf8(bytes.next_bytes(value_len as usize)?.to_vec()).unwrap();
            map.insert(key, value);
        }
        Some(KvStateMachine(map))
    }
}

impl WriteBytes for KvStateMachine {
    fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()> {
        writer.write_u32(self.0.len() as u32)?;
        for (key, value) in &self.0 {
            writer.write_u32(key.len() as u32)?;
            writer.write(key.as_bytes())?;
            writer.write_u32(value.len() as u32)?;
            writer.write(value.as_bytes())?;
        }
        Ok(())
    }
}

impl TryFromBytes for SetValueCommand {
    fn try_from_bytes(mut bytes: impl ReadBytes) -> Option<Self> {
        let key_len = bytes.next_u32()?;
        let key = String::from_utf8(bytes.next_bytes(key_len as usize)?.to_vec()).unwrap();
        let value_len = bytes.next_u32()?;
        let value = String::from_utf8(bytes.next_bytes(value_len as usize)?.to_vec()).unwrap();
        let mid_len = bytes.next_u32()?;
        let mid = String::from_utf8(bytes.next_bytes(mid_len as usize)?.to_vec()).unwrap();
        Some(SetValueCommand { key, value, mid })
    }
}

impl WriteBytes for SetValueCommand {
    fn write_bytes<W: Write>(&self, writer: &mut BytesWriter<W>) -> io::Result<()> {
        writer.write_u32(self.key.len() as u32)?;
        writer.write(self.key.as_bytes())?;
        writer.write_u32(self.value.len() as u32)?;
        writer.write(self.value.as_bytes())?;
        writer.write_u32(self.mid.len() as u32)?;
        writer.write(self.mid.as_bytes())
    }
}

pub fn clone_state_machine<S: StateMachine + Clone>(state_machine: &RaftStateMachine<S>) -> RaftStateMachine<S> {
    RaftStateMachine {
        inner: state_machine.inner.clone(),
        config: state_machine.config.clone(),
        client_last_command_ids: state_machine.client_last_command_ids.clone(),
    }
}