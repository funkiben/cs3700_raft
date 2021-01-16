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
        snapshot.write_bytes(&mut self.snapshot_bytes).unwrap();
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
        let offset = offset as usize;
        self.snapshot_chunk_bytes.resize_with(offset, || 0u8);
        self.snapshot_chunk_bytes.splice(offset.., data.iter().map(|n| *n));
    }

    fn try_use_chunks_as_new_snapshot(&mut self, last_index: u32, last_term: u32) -> Option<RaftStateMachine<S>> {
        let bytes = BytesRef::new(&self.snapshot_chunk_bytes);
        if let Some(snapshot) = RaftStateMachine::<S>::try_from_bytes(bytes) {
            self.snapshot_bytes = std::mem::take(&mut self.snapshot_chunk_bytes);
            self.snapshot_last_term = last_term;
            self.snapshot_last_index = last_index;
            Some(snapshot);
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
