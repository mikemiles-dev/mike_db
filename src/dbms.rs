use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use log::error;

use crate::dataspace::DataSpace;

#[derive(Default)]
pub struct DBMS {
    data_directory: String,
    dataspaces: HashMap<String, DataSpace>,
}

impl DBMS {
    pub fn new() -> DBMS {
        let mut dbms = DBMS {
            data_directory: env::var("DATA_DIRECTORY").unwrap_or_else(|_| "data".to_string()),
            dataspaces: HashMap::new(),
        };
        dbms.load_dataspaces();
        dbms
    }

    fn info_file_path(&self) -> PathBuf {
        let mut path = PathBuf::new();
        path.push(self.data_directory.clone());
        path.push("data");
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

    fn get_dataspaces_to_load(&mut self, file: &File) -> Vec<String> {
        let reader = BufReader::new(file);
        let mut dataspaces = vec![];
        for line in reader.lines().flatten() {
            dataspaces.push(line);
        }
        dataspaces
    }

    fn load_dataspaces_from_disk(&mut self, dataspaces: Vec<String>) {}

    pub fn load_dataspaces(&mut self) {
        let info_file = self.load_info_file();
        let dataspaces_to_load = self.get_dataspaces_to_load(&info_file);
    }

    pub fn save_dataspaces() {}
}

#[cfg(test)]
mod tests {
    use crate::dataspace::{self, DataSpace};
    use crate::dbms::DBMS;
    use crate::fields::{Field, FieldType};
    use crate::rows::Row;
    use crate::tables::Table;
    use std::env;

    #[test]
    fn it_can_write_dbms_to_disk() {
        let field1 = Field::new(FieldType::String, "A String".as_bytes().to_vec());
        let field2 = Field::new(FieldType::String, "Another String".as_bytes().to_vec());
        let row = Row::new(vec![field1, field2]);
        let mut table = Table::new(vec![FieldType::String, FieldType::String]);
        let result = table.insert(vec![row]);
        println!("{:?}", result);
        println!("{:?}", table);
        assert!(false);
        //let dataspace = DataSpace::new
    }
}
