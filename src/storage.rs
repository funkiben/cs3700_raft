use my_raft::bytes::{BytesRef, TryFromBytes, WriteBytes};
use my_raft::state_machine::{RaftStateMachine, StateMachine};
use my_raft::storage::log::{LogEntry, LogEntryType};
use my_raft::storage::Storage;

use crate::state_machine::clone_state_machine;

pub struct RamStorage<S: StateMachine> {
    log: Vec<LogEntry<S::Command>>,
    current_term: u32,
    voted_for: Option<u32>,
    snapshot_bytes: Vec<u8>,
    snapshot_last_index: u32,
    snapshot_last_term: u32,
    snapshot_chunk_bytes: Vec<u8>,
    init_state_machine: RaftStateMachine<S>,
}

impl<S: StateMachine> RamStorage<S> {
    pub fn new(init_state_machine: RaftStateMachine<S>) -> RamStorage<S> {
        RamStorage {
            log: vec![],
            current_term: 0,
            voted_for: None,
            snapshot_bytes: vec![],
            snapshot_last_index: 0,
            snapshot_last_term: 0,
            snapshot_chunk_bytes: vec![],
            init_state_machine,
        }
    }
}

impl<S: StateMachine + Clone> Storage<S> for RamStorage<S> {
    fn add_log_entry(&mut self, entry: LogEntry<<S as StateMachine>::Command>) {
        self.log.push(entry)
    }

    fn remove_log_entries_before(&mut self, index: usize) {
        self.log.drain(..index);
    }

    fn remove_log_entries_starting_at(&mut self, index: usize) {
        self.log.drain(index..);
    }

    fn save_log(&mut self) {}

    fn log_entry(&self, index: usize) -> Option<&LogEntry<<S as StateMachine>::Command>> {
        self.log.get(index)
    }

    fn log_entries(&self, start_index: usize) -> &[LogEntry<<S as StateMachine>::Command>] {
        &self.log[start_index..]
    }

    fn get_index_of_last_config_in_log(&self) -> Option<usize> {
        self.log.iter()
            .enumerate()
            .rev()
            .filter_map(|(i, e)|
                if let LogEntryType::Config(_) = &e.entry_type {
                    Some(i)
                } else {
                    None
                })
            .next()
    }

    fn num_log_entries(&self) -> usize {
        self.log.len()
    }

    fn set_snapshot(&mut self, last_index: u32, last_term: u32, snapshot: &RaftStateMachine<S>) {
        self.snapshot_last_index = last_index;
        self.snapshot_last_term = last_term;

        self.snapshot_bytes.clear();
        snapshot.write_bytes_with_writer(&mut self.snapshot_bytes).unwrap();
    }

    fn snapshot(&self) -> RaftStateMachine<S> {
        let bytes = BytesRef::new(&self.snapshot_bytes);
        RaftStateMachine::try_from_bytes(bytes).unwrap_or_else(|| clone_state_machine(&self.init_state_machine))
    }

    fn snapshot_last_index(&self) -> u32 {
        self.snapshot_last_index
    }

    fn snapshot_last_term(&self) -> u32 {
        self.snapshot_last_term
    }

    fn add_new_snapshot_chunk(&mut self, offset: u32, data: &[u8]) {
        let start = offset as usize;
        let end = start + data.len();
        self.snapshot_chunk_bytes.resize_with(end, || 0u8);
        self.snapshot_chunk_bytes.splice(start..end, data.iter().map(|n| *n));
    }

    fn try_use_chunks_as_new_snapshot(&mut self, last_index: u32, last_term: u32) -> Option<RaftStateMachine<S>> {
        if let Some(snapshot) = RaftStateMachine::<S>::try_from_slice(&self.snapshot_chunk_bytes) {
            self.snapshot_bytes = std::mem::take(&mut self.snapshot_chunk_bytes);
            self.snapshot_last_index = last_index;
            self.snapshot_last_term = last_term;
            return Some(snapshot);
        }
        None
    }

    fn snapshot_chunk(&self, offset: u32, amt: u32) -> &[u8] {
        let start = offset as usize;
        let end = start + amt as usize;
        &self.snapshot_bytes[start..end]
    }

    fn total_snapshot_bytes(&self) -> u32 {
        self.snapshot_bytes.len() as u32
    }

    fn set_voted_for(&mut self, voted_for: Option<u32>) {
        self.voted_for = voted_for;
    }

    fn voted_for(&self) -> Option<u32> {
        self.voted_for
    }

    fn set_current_term(&mut self, current_term: u32) {
        self.current_term = current_term;
    }

    fn current_term(&self) -> u32 {
        self.current_term
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use my_raft::bytes::WriteBytes;
    use my_raft::config::Config;
    use my_raft::state_machine::RaftStateMachine;
    use my_raft::storage::Storage;

    use crate::state_machine::KvStateMachine;
    use crate::storage::RamStorage;

    fn get_empty_storage() -> RamStorage<KvStateMachine> {
        RamStorage::new(RaftStateMachine {
            inner: KvStateMachine(HashMap::new()),
            config: Config {
                election_timeout_min: 0,
                election_timeout_range: 0,
                heartbeat_timeout: 0,
                rpc_response_timeout: 0,
                max_entries_in_append_entries: 0,
                max_bytes_in_install_snapshot: 0,
                next_index_decrease_rate: 0,
                snapshot_min_log_size: 0,
                id: 0,
                nodes: Default::default(),
            },
            client_last_command_ids: Default::default(),
        })
    }

    #[test]
    fn snapshot_chunks() {
        let mut sm = KvStateMachine(HashMap::new());
        sm.0.insert("hello".to_string(), "goodbye".to_string());
        sm.0.insert("blue".to_string(), "red".to_string());
        sm.0.insert("hot".to_string(), "cold".to_string());

        let sm = RaftStateMachine {
            inner: sm,
            config: Config {
                election_timeout_min: 435,
                election_timeout_range: 30,
                heartbeat_timeout: 10,
                rpc_response_timeout: 324,
                max_entries_in_append_entries: 0,
                max_bytes_in_install_snapshot: 0,
                next_index_decrease_rate: 0,
                snapshot_min_log_size: 12,
                id: 0,
                nodes: Default::default(),
            },
            client_last_command_ids: Default::default(),
        };

        let mut bytes = vec![];
        sm.write_bytes_with_writer(&mut bytes).unwrap();

        let mut storage = get_empty_storage();

        let chunk_size = 10;

        for i in 0..((bytes.len() / chunk_size) + 1) {
            let offset = i * chunk_size;
            let amt = chunk_size.min(bytes.len() - offset);
            storage.add_new_snapshot_chunk(offset as u32, &bytes[offset..(offset + amt)]);
        }

        assert_eq!(storage.snapshot_chunk_bytes, bytes);

        storage.try_use_chunks_as_new_snapshot(5, 5).unwrap();
    }
}