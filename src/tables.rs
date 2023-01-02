use crate::fields::{Field, FieldType};
use crate::rows::Row;

#[derive(Default, Debug)]
pub struct Table {
    columns: Vec<FieldType>,
    pub rows: Vec<Row>,
    pub pagefile_size: u128,
}

#[derive(Debug)]
pub enum TableError {
    InsertionError(String),
}

impl Table {
    pub fn new(columns: Vec<FieldType>, pagefile_size: u128) -> Table {
        Table {
            columns,
            rows: vec![],
            pagefile_size,
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
