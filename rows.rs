use uuid::Uuid;

use crate::fields::Field;

#[derive(Debug)]
pub struct Row {
    id: String,
    fields: Vec<Field>,
}

impl Row {
    pub fn new(fields: Vec<Field>) -> Row {
        Row {
            id: Uuid::new_v4().to_string(),
            fields: fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Row;
    use crate::fields::{Field, FieldType};

    #[test]
    fn it_creates_row_with_string() {
        let data = "A String!".as_bytes().to_vec();
        let field1 = Field::new(FieldType::String, data);
        let fields = vec![field1];
        let row = Row::new(fields);
        assert_eq!(
            row.fields.first().unwrap().data,
            vec![65, 32, 83, 116, 114, 105, 110, 103, 33]
        );
    }
}
