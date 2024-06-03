use std::{cell::{RefCell, RefMut}, rc::Rc};

use crate::{db::Database, types::TransactionState};


pub struct Connection{
    transaction_id: Option<u64>,
    db: Rc<RefCell<Database>>,
}

impl Connection {
    pub fn new(db: Rc<RefCell<Database>>) -> Self {
        Self {
            db,
            transaction_id: None,
        }
    }
    
    pub fn exec_command(&mut self, command: &str, args: &[&str]) -> Result<String, String> {
        match command {
            "begin" => {
                assert!(self.transaction_id.is_none(), "transaction already open");
                let new_id = self.get_db().new_transaction();
                self.transaction_id = Some(new_id);
                Ok(format!("transaction {}", new_id))
            }
            "abort" => {
                self.get_db().complete(self.transaction_id.unwrap(), TransactionState::Aborted);
                self.transaction_id = None;
                Ok("aborted".to_string())
            }
            "commit" => {
                self.get_db().complete(self.transaction_id.unwrap(), TransactionState::Committed);
                self.transaction_id = None;
                Ok("committed".to_string())
            }
            "get" => {
                let key = args[0];
                match self.get_db().get(self.transaction_id.unwrap(), key) {
                    Some(value) => Ok(value),
                    None => Err(format!("key {} not found", key)),
                }
            }
            "set" => {
                let key = args[0];
                let value = args[1];
                self.get_db().set(self.transaction_id.unwrap(), key, value);
                Ok("value set".to_string())
            }
            "delete" => {
                let key = args[0];
                self.get_db().delete(self.transaction_id.unwrap(), key);
                Ok("value deleted".to_string())
            }
            _ => Err(format!("unknown command `{}`", command))
        }
        
    }

    pub fn must_exec_command(&mut self, command: &str, args: &[&str]) -> String {
        self.exec_command(command, args).expect("command not possible")
    }

    fn get_db(&self) -> RefMut<Database> {
        self.db.borrow_mut()
    }
}
