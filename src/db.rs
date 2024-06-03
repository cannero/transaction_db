use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::types::{IsolationLevel, Transaction, TransactionState, Value};

pub struct Database {
    default_isolation: IsolationLevel,
    store: HashMap<String, Vec<Value>>,
    transactions: BTreeMap<u64, Transaction>,
    next_transaction_id: u64,
}

impl Database {
    pub fn new(default_isolation: IsolationLevel) -> Self {
        Self {
            default_isolation,
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

    pub fn get(&mut self, transaction_id: u64, key: &str) -> Option<String> {
        match (self.transactions.get_mut(&transaction_id), self.store.get_mut(key)) {
            (Some(transaction), Some(values)) => {
                transaction.readset_insert(key.to_string());
                for value in values.iter().rev() {
                    if Self::is_visible(&transaction, value) {
                        return Some(value.value.clone());
                    }
                }

                None
            },
            _ => None,
        }
    }

    pub fn set(&mut self, transaction_id: u64, key: &str, value: &str) {
        match self.transactions.get_mut(&transaction_id) {
            Some(transaction) => {
                if let Some(values) = self.store.get_mut(key) {
                    for value in values.iter_mut().rev() {
                        if Self::is_visible(&transaction, value) {
                            value.tx_end_id = transaction_id;
                        }
                    }
                }

                transaction.writeset_insert(key.to_string());
                self.store.insert(key.to_string(),
                                  vec![Value::new(transaction_id, value.to_string())]);
            }
            _ => panic!("transaction not found"),
        }
    }

    pub fn delete(&mut self, transaction_id: u64, key: &str) {
        match self.transactions.get_mut(&transaction_id) {
            Some(transaction) => {
                if let Some(values) = self.store.get_mut(key) {
                    for value in values.iter_mut().rev() {
                        if Self::is_visible(&transaction, value) {
                            value.tx_end_id = transaction_id;
                        }
                    }
                }

                transaction.writeset_insert(key.to_string());
            }
            _ => panic!("transaction not found"),
        }
    }

    fn is_visible(transaction: &Transaction, value: &Value) -> bool {
        match transaction.isolation_level {
            IsolationLevel::ReadUncommitted => value.tx_end_id == 0,
            _ => panic!("isolation level not implemented"),
        }
    }
}
