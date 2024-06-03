use std::collections::BTreeSet;

#[derive(Clone, PartialEq)]
pub enum TransactionState {
    InProgress,
    Aborted,
    Commited,
}

#[derive(Clone, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Snapshot,
    Serializable,
}

pub struct Value {
    tx_start_id: u64,
    tx_end_id: u64,
    value: String,
}

pub struct Transaction {
    isolation_level: IsolationLevel,
    pub id: u64,
    pub state: TransactionState,

    in_progress: BTreeSet<u64>,

    writeset: BTreeSet<String>,
    readset: BTreeSet<String>,
}

impl Transaction {

    pub fn new(id: u64, isolation_level: IsolationLevel, in_progress: BTreeSet<u64>) -> Self {
        Self {
            isolation_level,
            id,
            state: TransactionState::InProgress,
            in_progress,
            writeset: BTreeSet::new(),
            readset: BTreeSet::new(),
        }
    }
    
    pub fn set_state(&mut self, state: TransactionState) {
        self.state = state;
    }
}
