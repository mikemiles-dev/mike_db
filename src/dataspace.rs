use std::collections::HashMap;

use crate::tables::Table;

#[derive(Default, Debug)]
pub struct DataSpace {
    pub tables: HashMap<String, Table>,
}

impl DataSpace {
    pub fn create_table() {}
}
