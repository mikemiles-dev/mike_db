use crate::dataspace::DataSpace;
use std::collections::HashMap;

struct DBMS {
    data_directory: String,
    dataspaces: HashMap<String, DataSpace>,
}

impl DBMS {
    pub fn new() -> DBMS {
        let mut dbms = DBMS {
            data_directory: env!("CARGO_MANIFEST_DIR").to_string(),
            dataspaces: HashMap::new(),
        };
        dbms.load_dataspaces();
        dbms
    }

    pub fn load_dataspaces(&mut self) {}

    pub fn save_dataspaces() {}
}
