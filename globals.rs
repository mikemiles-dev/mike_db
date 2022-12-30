use std::collections::HashMap;
use tokio::sync::RwLock;
use lazy_static::*;

use crate::tables::Table;
use crate::dataspace::DataSpace;

lazy_static! {
    pub static ref dataspace_names: RwLock<Vec<String>> = RwLock::new(Vec::new());
    pub static ref tables: RwLock<HashMap<String, Table>> = RwLock::new(HashMap::new());
}
