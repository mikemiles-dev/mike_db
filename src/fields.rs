use std::{fmt, str, str::FromStr};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FieldType {
    String,
    Integer,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub field_type: FieldType,
    pub data: Vec<u8>,
}

impl FromStr for FieldType {
    type Err = ();

    fn from_str(input: &str) -> Result<FieldType, Self::Err> {
        match input {
            "String" => Ok(FieldType::String),
            "Int" => Ok(FieldType::Integer),
            _ => Err(()),
        }
    }
}

impl Field {
    pub fn new(field_type: FieldType, data: Vec<u8>) -> Field {
        Field { field_type, data }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.field_type == FieldType::String {
            let data = str::from_utf8(&self.data);
            match data {
                Ok(data) => write!(f, "{}", data),
                Err(_e) => write!(f, "{:?}", data),
            }
        } else {
            write!(f, "{:?}", self.data)
        }
    }
}
