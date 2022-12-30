use lazy_static::*;
use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::dataspace::DataSpace;
use crate::tables::Table;

lazy_static! {
    pub static ref dataspace_names: RwLock<Vec<String>> = RwLock::new(Vec::new());
    pub static ref tables: RwLock<HashMap<String, Table>> = RwLock::new(HashMap::new());
}
