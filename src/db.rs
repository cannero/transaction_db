use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::types::{IsolationLevel, Transaction, TransactionState, Value};

pub struct Database {
    default_isolation: IsolationLevel,
    store: HashMap<String, Value>,
    transactions: BTreeMap<u64, Transaction>,
    next_transaction_id: u64,
}

impl Database {
    pub fn new() -> Self {
        Self {
            default_isolation: IsolationLevel::ReadCommitted,
            store: HashMap::new(),
            transactions: BTreeMap::new(),
            next_transaction_id: 1,
        }
    }

    pub fn new_transaction(&mut self) -> u64 {
        let id = self.next_transaction_id;
        let transaction = Transaction::new(id,
                                            self.default_isolation.clone(),
                                            self.in_progress());
        self.next_transaction_id += 1;
        self.transactions.insert(transaction.id, transaction);
        id
    }

    pub fn complete(&mut self, transaction_id: u64, state: TransactionState) {
        if let Some(transaction) = self.transactions.get_mut(&transaction_id) {
            transaction.set_state(state);
        } else {
            panic!("transaction {} not found", transaction_id);
        }
    }

    pub fn in_progress(&self) -> BTreeSet<u64> {
        let mut result = BTreeSet::new();
        for (id, transaction) in &self.transactions{
            if transaction.state == TransactionState::InProgress {
                result.insert(*id);
            }
        }

        result
    }
}
