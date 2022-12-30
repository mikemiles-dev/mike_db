use lazy_static::*;
use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::tables::Table;

lazy_static! {
    pub static ref DataspaceNames: RwLock<Vec<String>> = RwLock::new(Vec::new());
    pub static ref Tables: RwLock<HashMap<String, Table>> = RwLock::new(HashMap::new());
}
