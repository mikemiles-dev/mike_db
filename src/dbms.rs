use std::collections::HashMap;
use std::env;
use std::fs::{create_dir, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use log::{debug, error, info, warn};

use crate::dataspace::DataSpace;
use crate::tables::{Table, TableError};

pub type DataSpaceName = String;

#[derive(Default)]
pub struct DBMS {
    data_directory: String,
    dataspaces: HashMap<DataSpaceName, DataSpace>,
}

#[derive(Debug)]
pub enum DBMSError {
    DataSpaceDoesNotExist,
    TableDoesNotExist,
    TableError(TableError),
    TableLoadError(String),
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

    fn data_path(&self) -> PathBuf {
        let mut path = PathBuf::new();
        path.push(self.data_directory.clone());
        path
    }

    fn info_file_path(&self) -> PathBuf {
        let mut path = self.data_path();
        path.push("info");
        path.set_extension("mike_db");
        path
    }

    fn dataspace_tables_path(&self, dataspace: String) -> PathBuf {
        let mut path = self.data_path();
        path.push(dataspace);
        path.set_extension("tables");
        path
    }

    fn table_file_path(&self, dataspace: String, table_name: String, table_page: u128) -> PathBuf {
        let mut path = self.data_path();
        path.push(format!("{}.{}.{}.table", dataspace, table_name, table_page));
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

    fn load_file(&self, file: PathBuf) -> File {
        match File::open(file.clone()) {
            Ok(file) => file,
            Err(e) => {
                panic!("Could not load file {}: {:?}", e, file.as_path().to_str());
            }
        }
    }

    fn load_data(&mut self, file: File) -> Vec<String> {
        let reader = BufReader::new(file);
        reader.lines().flatten().collect()
    }

    pub fn load_dataspaces(&mut self) {
        let dataspaces_to_load = self.load_data(self.info_file());
        for dataspace_to_load in dataspaces_to_load.iter() {
            self.load_dataspace(dataspace_to_load.clone());
        }
    }

    pub fn build_table_metadata(
        &mut self,
        dataspace: String,
        table_name: String,
    ) -> Result<Table, DBMSError> {
        info!("  |->  Loading Metadata for Table: {}", table_name);
        let table_metadata_path = self.table_file_path(dataspace, table_name, 0);
        let table_metadata_file = self.load_file(table_metadata_path.clone());
        let mut table_data = self.load_data(table_metadata_file);
        let mut table_metadata = table_data.iter_mut();
        // Get Page Count
        let file_metadata = table_metadata_path.as_path();
        let page_count = table_metadata
            .next()
            .ok_or_else(|| DBMSError::TableLoadError("Missing page count.".to_string()))?
            .parse::<u128>()
            .map_err(|_e| DBMSError::TableLoadError("Invalid page count.".to_string()))?;
        // Get Field Types
        let field_type_data = table_metadata.next().unwrap_or_else(|| {
            panic!(
                "Error: Datafile {:?} corrupted.  Missing field type data",
                file_metadata
            )
        });
        let field_types = Table::parse_field_types(field_type_data.to_string());
        Ok(Table::new(field_types, page_count))
    }

    pub fn load_dataspace(&mut self, dataspace: String) {
        info!("Loading Dataspace: {}...", dataspace);
        let path = self.dataspace_tables_path(dataspace.clone());
        let tables = self.load_data(self.load_file(path));
        for table in tables.iter() {
            let table1 = self.build_table_metadata(dataspace.clone(), table.clone());
            println!("{:?}", table1.unwrap());
        }
    }

    pub fn load_table(&mut self, dataspace: String, table_name: String, page_count: u128) {
        info!("Loading Table for {}: {}", dataspace, table_name);
        let table_file_path = self.table_file_path(dataspace, table_name, 1);
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
