use std::collections::HashMap;

use crate::tables::Table;

#[derive(Default)]
pub struct DataSpace {
    pub tables: HashMap<String, Table>,
}

impl DataSpace {
    pub fn create_table() {}
}
