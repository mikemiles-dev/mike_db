use std::collections::HashMap;

use crate::fields::{Field, FieldType};
use crate::rows::Row;

#[derive(Default, Debug)]
pub struct Table {
    pub column_types: Vec<FieldType>,
    pub column_names: Vec<String>,
    pub indexes: HashMap<Vec<String>, Row>,
    pub rows: Vec<Row>,
    pub pagefile_size: u128,
    pub current_file_size_in_bytes: u64,
}

#[derive(Debug)]
pub enum TableError {
    InsertionError(String),
}

impl Table {
    pub fn new(
        column_types: Vec<FieldType>,
        column_names: Vec<String>,
        pagefile_size: u128,
    ) -> Table {
        Table {
            column_types,
            column_names,
            indexes: HashMap::new(),
            rows: vec![],
            pagefile_size,
            current_file_size_in_bytes: 0,
        }
    }

    pub fn parse_field_types(field_type_string: String) -> Vec<FieldType> {
        let mut field_types = vec![];
        for i in field_type_string.split(',') {
            match i.trim().parse::<FieldType>() {
                Ok(f) => field_types.push(f),
                Err(_e) => panic!("Invalid Field Type {}", i),
            }
        }
        field_types
    }

    pub fn select_by_rowid(&self, id: String) -> Option<Row> {
        let row = self
            .rows
            .iter()
            .filter(|f| f.id == id)
            .cloned()
            .collect::<Vec<Row>>()
            .first()
            .cloned();
        row
    }

    pub fn insert(&mut self, fields: Vec<Vec<u8>>) -> Result<Row, TableError> {
        // Verify Length
        if fields.len() != self.column_types.len() {
            return Err(TableError::InsertionError(
                "Row length does not match column length".to_string(),
            ));
        }
        // Add
        let mut row = vec![];
        let mut columns = self.column_types.iter();
        for field in fields.iter() {
            if let Some(column) = columns.next() {
                let field = Field::new(column.clone(), field.to_vec());
                row.push(field);
            }
        }
        let row = Row::new(row);
        self.rows.push(row.clone());
        Ok(row)
    }
}

#[cfg(test)]
mod tests {}
