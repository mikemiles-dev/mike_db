use crate::fields::FieldType;
use crate::rows::Row;

#[derive(Default, Debug)]
pub struct Table {
    columns: Vec<FieldType>,
    rows: Vec<Row>,
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
        }
    }

    pub fn insert(&mut self, rows: Vec<Row>) -> Result<(), TableError> {
        for row in rows.iter() {
        // Verify Length
            if row.fields.len() != self.columns.len() {
                return Err(TableError::InsertionError(
                    "Row length does not match column length".to_string(),
                ));
            }
        // Verify Column Integrity
            let mut field = row.fields.iter();
            for column in self.columns.iter() {
                if let Some(field) = field.next() {
                    if field.field_type != *column {
                        return Err(TableError::InsertionError(
                            ("Field Types do not match Table".to_string()),
                        ));
                    }
                }
            }
        }
        for row in rows.into_iter() {
            self.rows.push(row);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
