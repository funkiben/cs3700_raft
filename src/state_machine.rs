use std::collections::HashMap;
use std::io;
use std::io::Write;

use my_raft::bytes::{ReadBytes, TryFromBytes, WriteBytes};
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
    fn write_bytes(&self, mut writer: impl Write) -> io::Result<usize> {
        let mut amt = writer.write(&(self.0.len() as u32).to_be_bytes())?;
        for (key, value) in &self.0 {
            amt += writer.write(&(key.len() as u32).to_be_bytes())?;
            amt += writer.write(key.as_bytes())?;
            amt += writer.write(&(value.len() as u32).to_be_bytes())?;
            amt += writer.write(value.as_bytes())?;
        }
        Ok(amt)
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
    fn write_bytes(&self, mut writer: impl Write) -> io::Result<usize> {
        let mut amt = writer.write(&(self.key.len() as u32).to_be_bytes())?;
        amt += writer.write(self.key.as_bytes())?;
        amt += writer.write(&(self.value.len() as u32).to_be_bytes())?;
        amt += writer.write(self.value.as_bytes())?;
        amt += writer.write(&(self.mid.len() as u32).to_be_bytes())?;
        amt += writer.write(self.mid.as_bytes())?;
        Ok(amt)
    }
}

pub fn clone_state_machine<S: StateMachine + Clone>(state_machine: &RaftStateMachine<S>) -> RaftStateMachine<S> {
    RaftStateMachine {
        inner: state_machine.inner.clone(),
        config: state_machine.config.clone(),
        client_last_command_ids: state_machine.client_last_command_ids.clone(),
    }
}