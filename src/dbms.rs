use std::collections::HashMap;
use std::env;

use crate::dataspace::DataSpace;
use crate::dbms_fs::*;
use crate::tables::TableError;

pub type DataSpaceName = String;

#[derive(Default, Debug)]
pub struct DBMS {
    pub data_directory: String,
    pub dataspaces: HashMap<DataSpaceName, DataSpace>,
}

#[derive(Debug)]
pub enum DBMSError {
    DataSpaceDoesNotExist,
    TableDoesNotExist,
    TableError(TableError),
    TableLoadError(String),
    FileReadError(String),
    CorruptedIndex,
}

impl DBMS {
    pub fn new() -> DBMS {
        let mut dbms = DBMS {
            data_directory: env::var("DATA_DIRECTORY").unwrap_or_else(|_| "data".to_string()),
            dataspaces: HashMap::new(),
        };
        dbms.load_from_disk();
        dbms
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
