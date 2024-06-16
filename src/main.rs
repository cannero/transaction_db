use std::{cell::RefCell, rc::Rc};

use connection::Connection;
use db::Database;
use types::IsolationLevel;

mod connection;
mod db;
mod types;

fn create_db_uncomitted() -> Rc<RefCell<Database>> {
    create_db(IsolationLevel::ReadUncommitted)
}

fn create_db(level: IsolationLevel) -> Rc<RefCell<Database>> {
    Rc::new(RefCell::new(Database::new(level)))
}

fn create_con(db: &Rc<RefCell<Database>>) -> Connection {
    Connection::new(Rc::clone(db))
}

fn create_open_con(db: &Rc<RefCell<Database>>) -> Connection {
    let mut con = create_con(&db);
    con.must_exec_command("begin", &vec![]);
    con
}

fn main() {
    let db = create_db_uncomitted();
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
    fn test_read_uncommitted() {
        let db = create_db_uncomitted();
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

    #[test]
    fn test_read_committed() {
        let db = create_db(IsolationLevel::ReadCommitted);
        let mut con1 = create_con(&db);
        let mut con2 = create_con(&db);

        con1.must_exec_command("begin", &vec![]);
        con2.must_exec_command("begin", &vec![]);

        con1.must_exec_command("set", &vec!["x", "hey"]);

        assert_eq!(&con1.must_exec_command("get", &vec!["x"]), "hey");
        assert!(con2.exec_command("get", &vec!["x"]).is_err());

        con1.must_exec_command("commit", &vec![]);

        assert_eq!(&con2.must_exec_command("get", &vec!["x"]), "hey");

        let mut con3 = create_con(&db);
        con3.must_exec_command("begin", &vec![]);
        con3.must_exec_command("set", &vec!["x", "other value"]);

        assert_eq!(&con3.must_exec_command("get", &vec!["x"]), "other value");
        assert_eq!(&con2.must_exec_command("get", &vec!["x"]), "hey");

        con3.must_exec_command("abort", &vec![]);

        assert_eq!(&con2.must_exec_command("get", &vec!["x"]), "hey");

        con2.must_exec_command("delete", &vec!["x"]);
        assert!(con2.exec_command("get", &vec!["x"]).is_err());

        con2.must_exec_command("commit", &vec![]);

        let mut con4 = create_con(&db);
        con4.must_exec_command("begin", &vec![]);
        assert!(con4.exec_command("get", &vec!["x"]).is_err());
    }

    #[test]
    fn test_read_repeatable() {
        let db = create_db(IsolationLevel::RepeatableRead);
        let mut con1 = create_open_con(&db);
        let mut con2 = create_open_con(&db);

        // Local change is visible locally.
        con1.must_exec_command("set", &vec!["x", "hey"]);
        assert_eq!(con1.must_exec_command("get", &vec!["x"]), "hey");

        // Update not available to this transaction since this is not
        // committed.
        assert!(con2.exec_command("get", &vec!["x"]).is_err());

        con1.must_exec_command("commit", &vec![]);

        // Not visible in existing transaction
        assert!(con2.exec_command("get", &vec!["x"]).is_err());

        let mut con3 = create_open_con(&db);
        assert_eq!(con3.must_exec_command("get", &vec!["x"]), "hey");

        con3.must_exec_command("set", &vec!["x", "yall"]);
        assert_eq!(con3.must_exec_command("get", &vec!["x"]), "yall");

        assert!(con2.exec_command("get", &vec!["x"]).is_err());

        con3.must_exec_command("abort", &vec![]);

        let mut con4 = create_open_con(&db);
        con4.must_exec_command("delete", &vec!["x"]);
        con4.must_exec_command("commit", &vec![]);

        let mut con5 = create_open_con(&db);
        assert!(con5.exec_command("get", &vec!["x"]).is_err());
    }

    #[test]
    fn test_snapshot_isolation_writewrite_conflict() {
        let db = create_db(IsolationLevel::Snapshot);
        let mut con1 = create_open_con(&db);
        let mut con2 = create_open_con(&db);
        let mut con3 = create_open_con(&db);

        con1.must_exec_command("set", &vec!["x", "hey"]);
        con1.must_exec_command("commit", &vec!["con1"]);

        con2.must_exec_command("set", &vec!["x", "hey"]);

        let res = con2.exec_command("commit", &vec![]);
        assert_eq!(res, Err("write-write conflict".to_string()));

        // But unrelated keys cause no conflict.
        con3.must_exec_command("set", &vec!["y", "no conflict"]);
        con3.must_exec_command("commit", &vec!["con3"]);
    }

    #[test]
    fn test_serialization_isolation_writewrite_conflict() {
        let db = create_db(IsolationLevel::Serializable);
        let mut con1 = create_open_con(&db);
        let mut con2 = create_open_con(&db);
        let mut con3 = create_open_con(&db);

        con1.must_exec_command("set", &vec!["x", "hey"]);
        con1.must_exec_command("commit", &vec!["con1"]);

        assert!(con2.exec_command("get", &vec!["x"]).is_err());

        let res = con2.exec_command("commit", &vec![]);
        assert_eq!(res, Err("read-write conflict".to_string()));

        // But unrelated keys cause no conflict.
        con3.must_exec_command("set", &vec!["y", "no conflict"]);
        con3.must_exec_command("commit", &vec!["con3"]);
    }
}
