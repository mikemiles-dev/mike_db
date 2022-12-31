#[derive(Debug, PartialEq)]
pub enum FieldType {
    String,
}

#[derive(Debug)]
pub struct Field {
    pub field_type: FieldType,
    pub data: Vec<u8>,
}

impl Field {
    pub fn new(field_type: FieldType, data: Vec<u8>) -> Field {
        Field { field_type, data }
    }
}
