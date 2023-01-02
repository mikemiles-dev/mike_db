use crate::fields::{Field, FieldType};
use crate::rows::Row;

#[derive(Default, Debug)]
pub struct Table {
    columns: Vec<FieldType>,
    rows: Vec<Row>,
    current_pagefile: u128,
}

#[derive(Debug)]
pub enum TableError {
    InsertionError(String),
}

impl Table {
    pub fn new(columns: Vec<FieldType>) -> Table {
        Table {
            columns,
            rows: vec![],
            current_pagefile: 1,
        }
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
        if fields.len() != self.columns.len() {
            return Err(TableError::InsertionError(
                "Row length does not match column length".to_string(),
            ));
        }
        // Add
        let mut row = vec![];
        let mut columns = self.columns.iter();
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
