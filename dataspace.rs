use std::collections::HashMap;

use crate::tables::Table;

pub struct DataSpace {
    pub tables: HashMap<String, Table>,
}
