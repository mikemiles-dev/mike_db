use std::collections::HashMap;
use std::env;
use std::fs::{create_dir, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use log::{debug, error, info, warn};

use crate::dataspace::DataSpace;
use crate::tables::TableError;

pub type DataSpaceName = String;

#[derive(Default)]
pub struct DBMS {
    data_directory: String,
    dataspaces: HashMap<DataSpaceName, DataSpace>,
}

pub enum DBMSError {
    DataSpaceDoesNotExist,
    TableDoesNotExist,
    TableError(TableError),
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
        path.push("info");
        path.set_extension("mike_db");
        path
    }

    fn create_info_file(&self) {
        let _ = create_dir(self.data_directory.clone());
        if let Err(e) = File::create(self.info_file_path()) {
            panic!("Could not create DBMS: {e}")
        }
    }

    fn info_file(&self) -> File {
        match File::open(self.info_file_path()) {
            Ok(file) => file,
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    self.create_info_file();
                    self.info_file()
                }
                _ => panic!(
                    "Could not load DBMS info file {}: {:?}",
                    e,
                    self.info_file_path()
                ),
            },
        }
    }

    fn get_dataspaces_to_load(&mut self) -> Vec<String> {
        let reader = BufReader::new(self.info_file());
        reader.lines().flatten().collect()
    }

    pub fn load_dataspaces(&mut self) {
        let dataspaces_to_load = self.get_dataspaces_to_load();
        for dataspace_to_load in dataspaces_to_load.iter() {
            self.load_dataspace(dataspace_to_load.clone());
        }

    }

    pub fn load_dataspace(&mut self, dataspace: String) {
        info!("Loading dataspace: {}", dataspace);
    }

    pub fn load_tables(&mut self, table_name: String) {

    }

    pub fn insert_into_table(
        &mut self,
        table_name: String,
        dataspace_name: String,
        fields: Vec<Vec<u8>>,
    ) -> Result<(), DBMSError> {
        let dataspace = self
            .dataspaces
            .get_mut(&dataspace_name)
            .ok_or(DBMSError::DataSpaceDoesNotExist)?;
        let table = dataspace
            .tables
            .get_mut(&table_name)
            .ok_or(DBMSError::TableDoesNotExist)?;
        table.insert(fields).map_err(DBMSError::TableError)?;
        // Write to datafile here
        Ok(())
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
        let dbms = DBMS::new();
    }
}
