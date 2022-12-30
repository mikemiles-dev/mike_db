use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

use log::error;

use crate::dataspace::DataSpace;

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

    fn info_file_path(&self) -> PathBuf {
        let mut path = PathBuf::new();
        path.push(self.data_directory.clone());
        path.push("info");
        path.set_extension("mike_db");
        path
    }

    fn create_info_file(&self) -> File {
        match File::create(self.info_file_path()) {
            Ok(file) => file,
            Err(e) => panic!("Could not create DBMS: {e}"),
        }
    }

    fn load_info_file(&self) -> File {
        match File::open(self.info_file_path()) {
            Ok(file) => file,
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => self.create_info_file(),
                std::io::ErrorKind::PermissionDenied => self.create_info_file(),
                _ => panic!("Could not load DBMS: {e}"),
            },
        }
    }

    pub fn load_dataspaces(&mut self) {}

    pub fn save_dataspaces() {}
}
