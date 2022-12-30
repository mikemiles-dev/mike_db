use crate::globals::Tables;
use crate::rows::Row;

pub struct Table {
    name: String,
    rows: Vec<Row>,
}

pub enum TableError {
    TableAlreadyExists,
}

impl Table {
    async fn table_name_available(table_name: String, dataspace_name: String) -> bool {
        !Tables.read().await.contains_key(&table_name)
    }

    async fn new(table_name: String, dataspace_name: String) -> Result<Table, TableError> {
        if Self::table_name_available(table_name.clone(), dataspace_name).await {
            Err(TableError::TableAlreadyExists)
        } else {
            Ok(Table {
                name: table_name,
                rows: vec![],
            })
        }
    }
}

#[cfg(test)]
mod tests {}
