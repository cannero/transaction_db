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
        let transaction = Transaction::new(id, self.default_isolation.clone(), self.in_progress());
        self.next_transaction_id += 1;
        self.transactions.insert(transaction.id, transaction);
        id
    }

    pub fn complete(&mut self, transaction_id: u64, state: TransactionState) -> Result<(), String> {
        let trans2 = self.transactions.clone();
        if let Some(transaction) = self.transactions.get_mut(&transaction_id) {
            if state == TransactionState::Committed {
                if transaction.isolation_level == IsolationLevel::Snapshot
                    && Self::has_conflict(transaction, &trans2, self.next_transaction_id)
                {
                    transaction.set_state(TransactionState::Aborted);
                    return Err("write-write conflict".to_string());
                }
            }

            transaction.set_state(state);
            Ok(())
        } else {
            Err(format!("transaction {} not found", transaction_id))
        }
    }

    pub fn in_progress(&self) -> BTreeSet<u64> {
        let mut result = BTreeSet::new();
        for (id, transaction) in &self.transactions {
            if transaction.state == TransactionState::InProgress {
                result.insert(*id);
            }
        }

        result
    }

    pub fn get(&mut self, transaction_id: u64, key: &str) -> Option<String> {
        let trans2 = self.transactions.clone();
        match (
            self.transactions.get_mut(&transaction_id),
            self.store.get(key),
        ) {
            (Some(transaction), Some(values)) => {
                transaction.readset_insert(key.to_string());
                for value in values.iter().rev() {
                    if Self::is_visible(&transaction, value, &trans2) {
                        return Some(value.value.clone());
                    }
                }

                None
            }
            _ => None,
        }
    }

    pub fn set(&mut self, transaction_id: u64, key: &str, value: &str) {
        let trans2 = self.transactions.clone();
        match self.transactions.get_mut(&transaction_id) {
            Some(transaction) => {
                if let Some(values) = self.store.get_mut(key) {
                    for value in values.iter_mut().rev() {
                        if Self::is_visible(&transaction, value, &trans2) {
                            value.tx_end_id = transaction_id;
                        }
                    }
                }

                transaction.writeset_insert(key.to_string());
                let values = self.store.entry(key.to_string()).or_insert(vec![]);
                values.push(Value::new(transaction_id, value.to_string()));
            }
            _ => panic!("transaction not found"),
        }
    }

    pub fn delete(&mut self, transaction_id: u64, key: &str) {
        let trans2 = self.transactions.clone();
        match self.transactions.get_mut(&transaction_id) {
            Some(transaction) => {
                if let Some(values) = self.store.get_mut(key) {
                    for value in values.iter_mut().rev() {
                        if Self::is_visible(&transaction, value, &trans2) {
                            value.tx_end_id = transaction_id;
                        }
                    }
                }

                transaction.writeset_insert(key.to_string());
            }
            _ => panic!("transaction not found"),
        }
    }

    fn has_conflict(
        transaction: &Transaction,
        transactions: &BTreeMap<u64, Transaction>,
        next_transaction_id: u64,
    ) -> bool {
        for t_in_progress in &transaction.in_progress {
            let transaction2 = transactions.get(&t_in_progress).unwrap();
            if transaction2.state == TransactionState::Committed {
                if transaction.shares_writeset(transaction2) {
                    println!(
                        "{} shares in_progress with {}",
                        transaction.id, transaction2.id
                    );
                    return true;
                }
            }
        }

        for id in transaction.id..next_transaction_id {
            match transactions.get(&id) {
                None => continue,
                Some(t2) => {
                    if t2.state == TransactionState::Committed && transaction.shares_writeset(t2) {
                        println!("{} shares writeset with {}", transaction.id, t2.id);
                        return true;
                    }
                }
            }
        }

        false
    }

    fn is_visible(
        transaction: &Transaction,
        value: &Value,
        transactions: &BTreeMap<u64, Transaction>,
    ) -> bool {
        match transaction.isolation_level {
            IsolationLevel::ReadUncommitted => value.tx_end_id == 0,
            IsolationLevel::ReadCommitted => {
                if value.tx_start_id != transaction.id
                    && transactions.get(&value.tx_start_id).unwrap().state
                        != TransactionState::Committed
                {
                    // created by another transaction and not committed
                    return false;
                }

                if value.tx_end_id == transaction.id {
                    // deleted in this transaction
                    return false;
                }

                if value.tx_end_id != 0
                    && transactions.get(&value.tx_end_id).unwrap().state
                        == TransactionState::Committed
                {
                    // value deleted in another transaction
                    return false;
                }

                return true;
            }
            IsolationLevel::RepeatableRead
            | IsolationLevel::Snapshot
            | IsolationLevel::Serializable => {
                if value.tx_start_id > transaction.id {
                    // created after transaction started
                    return false;
                }

                if transaction.in_progress.contains(&value.tx_start_id) {
                    // was in progress when transaction started
                    return false;
                }

                if value.tx_start_id != transaction.id
                    && transactions.get(&value.tx_start_id).unwrap().state
                        != TransactionState::Committed
                {
                    // created by another transaction and not committed
                    return false;
                }

                if value.tx_end_id == transaction.id {
                    // deleted in this transaction
                    return false;
                }

                if value.tx_end_id > 0
                    && value.tx_end_id < transaction.id
                    && transactions.get(&value.tx_end_id).unwrap().state
                        == TransactionState::Committed
                    && !transaction.in_progress.contains(&value.tx_end_id)
                {
                    return false;
                }

                return true;
            }
            _ => panic!("isolation level not implemented"),
        }
    }
}
