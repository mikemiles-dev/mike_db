use mike_db::dbms::DBMS;
use mike_db::init_log;

fn main() {
    init_log().unwrap();

    let dbms = DBMS::new();
}
