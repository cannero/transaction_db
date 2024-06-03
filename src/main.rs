use std::{cell::RefCell, rc::Rc};

use connection::Connection;
use db::Database;

mod connection;
mod db;
mod types;

fn create_db() -> Rc<RefCell<Database>> {
    Rc::new(RefCell::new(Database::new(types::IsolationLevel::ReadUncommitted)))
}

fn create_con(db: &Rc<RefCell<Database>>) -> Connection {
    Connection::new(Rc::clone(db))
}

fn main() {
    let db = create_db();
    let mut conn1 = create_con(&db);
    let mut conn2 = create_con(&db);

    println!("{:?}", conn1.exec_command("begin", &vec![]));
    println!("{:?}", conn2.exec_command("begin", &vec![]));

    println!("in progress {:?}", db.borrow().in_progress());
    
    println!("{:?}", conn1.exec_command("abort", &vec![]));

    println!("in progress {:?}", db.borrow().in_progress());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_uncommited() {
        let db = create_db();
        let mut con1 = create_con(&db);
        let mut con2 = create_con(&db);

        con1.must_exec_command("begin", &vec![]);
        con2.must_exec_command("begin", &vec![]);

        con1.must_exec_command("set", &vec!["x", "hey"]);

        assert_eq!(&con1.must_exec_command("get", &vec!["x"]), "hey");
        assert_eq!(&con2.must_exec_command("get", &vec!["x"]), "hey");

        con1.must_exec_command("delete", &vec!["x"]);

        assert!(con1.exec_command("get", &vec!["x"]).is_err());
        assert!(con2.exec_command("get", &vec!["x"]).is_err());
    }
}
