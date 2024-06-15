use std::collections::BTreeSet;

#[derive(Clone, PartialEq)]
pub enum TransactionState {
    InProgress,
    Aborted,
    Committed,
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
    pub tx_start_id: u64,
    pub tx_end_id: u64,
    pub value: String,
}

impl Value {
    pub fn new(tx_start_id: u64, value: String) -> Self {
        Self {
            tx_start_id,
            tx_end_id: 0,
            value,
        }
    }
}

#[derive(Clone)]
pub struct Transaction {
    pub isolation_level: IsolationLevel,
    pub id: u64,
    pub state: TransactionState,

    pub in_progress: BTreeSet<u64>,

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

    pub fn readset_insert(&mut self, key: String) {
        self.readset.insert(key);
    }

    pub fn writeset_insert(&mut self, key: String) {
        self.writeset.insert(key);
    }

    pub(crate) fn shares_writeset(&self, transaction2: &Transaction) -> bool {
        !self.writeset.is_disjoint(&transaction2.writeset)
    }
}
