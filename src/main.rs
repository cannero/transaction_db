use std::{cell::RefCell, rc::Rc};

use connection::Connection;
use db::Database;

mod connection;
mod db;
mod types;

fn main() {
    let db = Rc::new(RefCell::new(Database::new()));
    let mut conn1 = Connection::new(Rc::clone(&db));
    let mut conn2 = Connection::new(Rc::clone(&db));

    println!("{:?}", conn1.exec_command("begin", &vec![]));
    println!("{:?}", conn2.exec_command("begin", &vec![]));

    println!("in progress {:?}", db.borrow().in_progress());
    
    println!("{:?}", conn1.exec_command("abort", &vec![]));

    println!("in progress {:?}", db.borrow().in_progress());
}
