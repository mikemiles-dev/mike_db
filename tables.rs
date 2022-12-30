use crate::rows::Row;
use crate::dataspace::DataSpace;

pub struct Table {
    name: String,
    rows: Vec<Row>,
}

impl Table {
    fn can_table_name_be_used(table_name: String, dataspace_name: String) -> bool {
        false
    }

    fn new(table_name: String, dataspace_name: String) -> Table {
        Table { 
            name: table_name,
            rows: vec![],
        }
    }
}

#[cfg(test)]
mod tests {


}
