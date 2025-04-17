use wasm_bindgen::prelude::*;
use sled::{Db, Config};
use rust_db_logic::{
    self as logic,
    DbConfig as LogicDbConfig,
    BatchSetItem,
    TransactionOperation,
    QueryNode,
    DbError,
};
use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::collections::HashSet;
use js_sys::{Promise, Function};
use wasm_bindgen_futures::future_to_promise;
use tracing_wasm::WASMLayerConfigBuilder;
use tracing::{info, error, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// --- Error Mapping ---

#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmDbError {
    message: String,
    code: Option<u16>, // Optional HTTP-like status code
}

#[wasm_bindgen]
impl WasmDbError {
    #[wasm_bindgen(getter)]
    pub fn message(&self) -> String {
        self.message.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn code(&self) -> Option<u16> {
        self.code
    }

    // This is an internal Rust function, no need for wasm_bindgen here
    fn new(message: String, code: Option<u16>) -> WasmDbError {
        WasmDbError { message, code }
    }
}

// Removed #[wasm_bindgen] from this impl block
impl From<WasmDbError> for JsValue {
    fn from(err: WasmDbError) -> JsValue {
        let obj = js_sys::Object::new();
        js_sys::Reflect::set(&obj, &JsValue::from_str("message"), &JsValue::from_str(&err.message)).unwrap();
        if let Some(code) = err.code {
            js_sys::Reflect::set(&obj, &JsValue::from_str("code"), &JsValue::from_f64(code as f64)).unwrap();
        }
        JsValue::from(obj)
    }
}

fn map_logic_error(err: DbError) -> WasmDbError {
    error!("Logic Error: {}", err); // Log the error
    let (message, code) = match err {
        DbError::NotFound => ("Key not found".to_string(), Some(404)),
        DbError::Serde(e) => (format!("Serialization/Deserialization error: {}", e), Some(400)),
        DbError::MissingData(s) => (format!("Missing data: {}", s), Some(400)),
        DbError::InvalidComparisonValue(s) => (format!("Invalid comparison value: {}", s), Some(400)),
        DbError::NotAnObject => ("Value is not an object".to_string(), Some(400)),
        DbError::FieldNotFound(s) => (format!("Field not found: {}", s), Some(400)),
        DbError::NotAGeoPoint(s) => (format!("Not a GeoPoint: {}", s), Some(400)),
        DbError::AstQueryError(s) => (format!("Query error: {}", s), Some(400)),
        DbError::InvalidPath(s) => (format!("Invalid path: {}", s), Some(400)),
        DbError::TransactionOperationFailed(s) => (format!("Transaction failed: {}", s), Some(409)), // Conflict
        DbError::Sled(e) => (format!("Database internal error: {}", e), Some(500)),
        DbError::Geohash(e) => (format!("Geohash error: {}", e), Some(500)),
        DbError::ImportError(e) => (format!("Import error: {}", e), Some(400)),
        DbError::CasRetryLimit(e) => (format!("Concurrency error: {}", e), Some(500)),
        DbError::Utf8Error(e) => (format!("UTF8 error: {}", e), Some(400)),
        DbError::HexError(e) => (format!("Hex error: {}", e), Some(500)),
        DbError::TryFromSlice(e) => (format!("Byte conversion error: {}", e), Some(500)),
        DbError::Transaction(e) => (format!("Transaction error: {}", e), Some(500)),
        DbError::Io(e) => (format!("IO error: {}", e), Some(500)),
        DbError::InvalidFieldIndexKey(e) => (format!("Invalid field index key: {}", e), Some(500)),
        DbError::InvalidGeoSortedKey(e) => (format!("Invalid geo sorted key: {}", e), Some(500)), // Added missing arm
    };
    WasmDbError::new(message, code)
}

fn map_serde_error(err: serde_json::Error) -> WasmDbError {
     error!("Serde Error: {}", err);
     WasmDbError::new(format!("JSON processing error: {}", err), Some(400))
}

fn map_sled_error(err: sled::Error) -> WasmDbError {
     error!("Sled Error: {}", err);
     WasmDbError::new(format!("Database internal error: {}", err), Some(500))
}

// --- Database Wrapper ---

#[wasm_bindgen]
pub struct Database {
    db: Arc<Db>,
    db_config: Arc<Mutex<LogicDbConfig>>,
}

#[wasm_bindgen]
impl Database {
    #[wasm_bindgen(constructor)]
    pub fn new(db_name: String) -> Result<Database, WasmDbError> {
        // Initialize tracing
        let wasm_layer_config = WASMLayerConfigBuilder::new().set_max_level(tracing::Level::INFO).build();
        tracing_subscriber::registry()
            .with(tracing_wasm::WASMLayer::new(wasm_layer_config))
            .init();

        info!("Opening database: {}", db_name);
        let db = Config::default()
            .path(db_name)
            .use_compression(true)
            .open()
            .map_err(map_sled_error)?;

        let db_config = Arc::new(Mutex::new(LogicDbConfig::default()));
        info!("Initialized with default DbConfig: {:?}", db_config);

        Ok(Database {
            db: Arc::new(db),
            db_config,
        })
    }

    #[wasm_bindgen]
    pub fn set(&self, key: String, value: JsValue) -> Result<(), WasmDbError> {
        info!("Setting key: {}", key);
        let val: Value = serde_wasm_bindgen::from_value(value).map_err(|e| WasmDbError::new(format!("Failed to deserialize value: {}", e), Some(400)))?;
        let db_config_guard = self.db_config.lock().unwrap();
        logic::set_key(&self.db, &key, val, &db_config_guard).map_err(map_logic_error)
    }

    #[wasm_bindgen]
    pub fn get(&self, key: String) -> Result<JsValue, WasmDbError> {
        info!("Getting key: {}", key);
        let value = logic::get_key(&self.db, &key).map_err(map_logic_error)?;
        serde_wasm_bindgen::to_value(&value).map_err(|e| WasmDbError::new(format!("Failed to serialize value: {}", e), Some(500)))
    }

     #[wasm_bindgen(js_name = getPartial)]
     pub fn get_partial(&self, key: String, fields: Vec<String>) -> Result<JsValue, WasmDbError> {
         info!("Getting partial key: {}, fields: {:?}", key, fields);
         let value = logic::get_partial_key(&self.db, &key, &fields).map_err(map_logic_error)?;
         serde_wasm_bindgen::to_value(&value).map_err(|e| WasmDbError::new(format!("Failed to serialize partial value: {}", e), Some(500)))
     }

    #[wasm_bindgen]
    pub fn delete(&self, key: String) -> Promise {
        info!("Deleting key: {}", key);
        let db_arc = Arc::clone(&self.db);
        let config_clone = self.db_config.lock().unwrap().clone(); // Clone config
        let key_clone = key.clone();

        future_to_promise(async move {
            logic::delete_key(&db_arc, &key_clone, &config_clone)
                .await
                .map(|_| JsValue::UNDEFINED)
                .map_err(|e| JsValue::from(map_logic_error(e)))
        })
    }

     #[wasm_bindgen(js_name = batchSet)]
     pub fn batch_set(&self, items_js: JsValue) -> Result<(), WasmDbError> {
         info!("Performing batch set");
         let items: Vec<BatchSetItem> = serde_wasm_bindgen::from_value(items_js).map_err(|e| WasmDbError::new(format!("Failed to deserialize batch items: {}", e), Some(400)))?;
         let db_config_guard = self.db_config.lock().unwrap();
         logic::batch_set(&self.db, &items, &db_config_guard).map_err(map_logic_error)
     }

     #[wasm_bindgen]
     pub fn transaction(&self, operations_js: JsValue) -> Result<(), WasmDbError> {
         info!("Executing transaction");
         let operations: Vec<TransactionOperation> = serde_wasm_bindgen::from_value(operations_js).map_err(|e| WasmDbError::new(format!("Failed to deserialize transaction operations: {}", e), Some(400)))?;
         let db_config_guard = self.db_config.lock().unwrap();
         logic::execute_transaction(&self.db, &operations, &db_config_guard).map_err(map_logic_error)
     }

     #[wasm_bindgen(js_name = clearPrefix)]
     pub fn clear_prefix(&self, prefix: String) -> Result<usize, WasmDbError> {
         info!("Clearing prefix: {}", prefix);
         let db_config_guard = self.db_config.lock().unwrap();
         logic::clear_prefix(&self.db, &prefix, &db_config_guard).map_err(map_logic_error)
     }

     #[wasm_bindgen(js_name = dropDatabase)]
     pub fn drop_database(&self) -> Result<usize, WasmDbError> {
         info!("Dropping database");
         let db_config_guard = self.db_config.lock().unwrap();
         logic::drop_database(&self.db, &db_config_guard).map_err(map_logic_error)
     }

    #[wasm_bindgen(js_name = queryAst)]
    pub fn query_ast(&self, query_js: JsValue, projection_js: JsValue, limit_js: JsValue, offset_js: JsValue) -> Result<JsValue, WasmDbError> {
        info!("Executing AST query");
        let query_node: QueryNode = serde_wasm_bindgen::from_value(query_js).map_err(|e| WasmDbError::new(format!("Failed to deserialize query AST: {}", e), Some(400)))?;
        let projection: Option<Vec<String>> = serde_wasm_bindgen::from_value(projection_js).ok();
        let limit: Option<usize> = serde_wasm_bindgen::from_value(limit_js).ok();
        let offset: Option<usize> = serde_wasm_bindgen::from_value(offset_js).ok();

        // Dynamic Indexing Logic (similar to server)
        let config_clone = {
            let mut db_config_guard = self.db_config.lock().unwrap();
            if let Some(field) = extract_eq_field_wasm(&query_node) { // Use WASM-specific helper
                 if db_config_guard.hash_indexed_fields.insert(field.clone()) {
                     info!("Dynamically indexing field (WASM): {}", field);
                 }
            }
            db_config_guard.clone()
        };


        let results = logic::execute_ast_query(&self.db, query_node, projection, limit, offset, &config_clone).map_err(map_logic_error)?; // Pass cloned config
        serde_wasm_bindgen::to_value(&results).map_err(|e| WasmDbError::new(format!("Failed to serialize query results: {}", e), Some(500)))
    }

    #[wasm_bindgen(js_name = exportData)]
    pub fn export_data(&self) -> Result<String, WasmDbError> {
        info!("Exporting data");
        logic::export_data(&self.db).map_err(map_logic_error)
    }

    #[wasm_bindgen(js_name = importData)]
    pub fn import_data(&self, data: String) -> Result<(), WasmDbError> {
        info!("Importing data");
        let db_config_guard = self.db_config.lock().unwrap();
        logic::import_data(&self.db, &data, &db_config_guard).map_err(map_logic_error)
    }
}

// Helper for dynamic indexing in WASM context
fn extract_eq_field_wasm(query_node: &QueryNode) -> Option<String> {
    match query_node {
        QueryNode::Eq(field, _, _) => Some(field.clone()),
        QueryNode::And(left, right) => extract_eq_field_wasm(left).or_else(|| extract_eq_field_wasm(right)),
        QueryNode::Or(left, right) => extract_eq_field_wasm(left).or_else(|| extract_eq_field_wasm(right)),
        QueryNode::Not(node) => extract_eq_field_wasm(node),
        _ => None,
    }
}