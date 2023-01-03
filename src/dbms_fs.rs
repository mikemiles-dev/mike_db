use std::collections::HashMap;
use std::fs::{create_dir, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use hex;
use log::{debug, error, info};

use crate::dataspace::DataSpace;
use crate::dbms::{DBMSError, DBMS};
use crate::tables::Table;

pub trait FileSystem {
    // Path helper functions
    fn data_path(&self) -> PathBuf;
    fn dataspace_tables_path(&self, dataspace: String) -> PathBuf;
    fn table_file_path(&self, dataspace: String, table_name: String, table_page: u128) -> PathBuf;
    // Info file helpers
    fn create_info_file(&self);
    fn info_file_path(&self) -> PathBuf;
    fn info_file(&self) -> File;
    // File/Table Loaders
    fn load_file(&self, file: PathBuf) -> Result<File, DBMSError>;
    fn load_data(&mut self, file: File) -> Vec<String>;
    fn load_from_disk(&mut self);
    fn load_dataspace(&mut self, dataspace_name: String) -> DataSpace;
    fn load_table(&mut self, dataspace: String, table_name: String) -> Result<Table, DBMSError>;
    fn load_table_row(&mut self, row_data: Vec<String>, table: &mut Table);
    fn load_table_rows(&mut self, dataspace: String, table_name: String, table: &mut Table);
    fn load_table_indexes(&mut self, dataspace: String, table_name: String, table: &mut Table);
    fn load_table_index(&mut self, index_data: String, table: &mut Table) -> Result<(), DBMSError>;
    // File/Table Writter
    // <Todo>
}

impl FileSystem for DBMS {
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

    fn load_file(&self, file: PathBuf) -> Result<File, DBMSError> {
        File::open(file.clone()).map_err(|e| {
            DBMSError::FileReadError(format!(
                "Could not load file {}: {:?}",
                e,
                file.as_path().to_str()
            ))
        })
    }

    fn load_data(&mut self, file: File) -> Vec<String> {
        let reader = BufReader::new(file);
        reader.lines().flatten().collect()
    }

    fn load_from_disk(&mut self) {
        let dataspaces_to_load = self.load_data(self.info_file());
        for dataspace_to_load in dataspaces_to_load.iter() {
            let dataspace = self.load_dataspace(dataspace_to_load.clone());
            self.dataspaces.insert(dataspace_to_load.clone(), dataspace);
        }
    }

    fn load_table(&mut self, dataspace: String, table_name: String) -> Result<Table, DBMSError> {
        info!(" . Loading Table: {}", table_name);
        let table_metadata_path = self.table_file_path(dataspace, table_name, 0);
        let table_metadata_file = self.load_file(table_metadata_path).unwrap();
        let mut table_data = self.load_data(table_metadata_file);
        let mut table_metadata = table_data.iter_mut();
        // Get Page Count
        let page_count = table_metadata
            .next()
            .ok_or_else(|| DBMSError::TableLoadError("Missing page count.".to_string()))?
            .parse::<u128>()
            .map_err(|_e| DBMSError::TableLoadError("Invalid page count.".to_string()))?;
        // Get Field Types
        let field_type_data = table_metadata
            .next()
            .ok_or_else(|| DBMSError::TableLoadError("Missing field type data".to_string()))?;
        let field_types = Table::parse_field_types(field_type_data.to_string());
        // Get Field Names
        let field_names = table_metadata
            .next()
            .ok_or_else(|| DBMSError::TableLoadError("Missing field name data".to_string()))?
            .split(',')
            .map(|f| f.to_string())
            .collect::<Vec<String>>();
        // Load Indexes
        Ok(Table::new(field_types, field_names, page_count))
    }

    fn load_table_row(&mut self, row_data: Vec<String>, table: &mut Table) {
        for row in row_data {
            let mut fields = vec![];
            for field in row.split(',') {
                match hex::decode(field.trim()) {
                    Ok(decoded) => fields.push(decoded),
                    Err(e) => {
                        error!(". Could not load field {}: {:?}", field, e);
                        continue;
                    }
                }
            }
            debug!(" .   Inserting fields {:?}", fields);
            let _ = table.insert(fields);
        }
    }

    fn load_table_rows(&mut self, dataspace: String, table_name: String, table: &mut Table) {
        info!(" . Loading Rows for {}", table_name);
        // Load pages
        for count in 1..table.pagefile_size + 1 {
            let table_file_path =
                self.table_file_path(dataspace.clone(), table_name.clone(), count);
            let table_file = self.load_file(table_file_path).unwrap();
            table.current_file_size_in_bytes = table_file
                .metadata()
                .unwrap_or_else(|e| panic!("Could not get metadata! {}", e))
                .len();
            let row_data = self.load_data(table_file);
            self.load_table_row(row_data, table);
        }
    }

    fn load_table_index(&mut self, index_data: String, table: &mut Table) -> Result<(), DBMSError> {
        let mut index_data = index_data.split(':');
        let index_field_names = index_data.next().ok_or(DBMSError::CorruptedIndex)?;
        println!("INDEX {}", index_field_names);
        Ok(())
    }

    fn load_table_indexes(&mut self, dataspace: String, table_name: String, table: &mut Table) {
        let mut path = self.data_path();
        path.push(format!("{}.{}.indexes", dataspace, table_name));
        let indexes_file = match self.load_file(path.clone()) {
            Ok(indexes_file) => indexes_file,
            Err(_e) => return,
        };
        let indexes_data = self.load_data(indexes_file);
        for index_data in indexes_data {
            if let Err(e) = self.load_table_index(index_data, table) {
                error!("Error with file {:?}: {:?}", path.clone().to_str(), e);
            }
        }
    }

    fn load_dataspace(&mut self, dataspace_name: String) -> DataSpace {
        info!("Loading Dataspace: {}...", dataspace_name);
        let mut dataspace = DataSpace {
            tables: HashMap::new(),
        };
        let path = self.dataspace_tables_path(dataspace_name.clone());
        let tables = self.load_data(self.load_file(path).unwrap());
        for table_name in tables.iter() {
            let mut table = match self.load_table(dataspace_name.clone(), table_name.clone()) {
                Ok(table) => table,
                Err(e) => panic!("{:?}", e),
            };
            self.load_table_rows(dataspace_name.clone(), table_name.clone(), &mut table);
            self.load_table_indexes(dataspace_name.clone(), table_name.clone(), &mut table);
            dataspace.tables.insert(table_name.clone(), table);
        }
        dataspace
    }
}
