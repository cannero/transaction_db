use std::rc::Rc;

use crate::db::Database;


struct Connection{
    transaction_id: Option<u64>,
    db: Rc<Database>,
}

impl Connection {
}
