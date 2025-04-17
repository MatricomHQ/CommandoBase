use wasm_bindgen::prelude::*;
use rust_db_logic::{
    self as logic,
    export_data as logic_export_data,
    import_data as logic_import_data,
    DbConfig as LogicDbConfig,
    BatchSetItem, // Import new types
    TransactionOperation,
};
use sled::{Db, Config};
use serde_json::Value;
use std::sync::OnceLock;
use serde_wasm_bindgen;

static DB: OnceLock<Db> = OnceLock::new();
static DB_CONFIG: OnceLock<LogicDbConfig> = OnceLock::new();

fn get_db() -> Result<&'static Db, JsValue> {
    DB.get().ok_or_else(|| JsValue::from_str("Database not initialized. Call init_db first."))
}

fn get_db_config() -> Result<&'static LogicDbConfig, JsValue> {
    DB_CONFIG.get_or_init(|| {
        LogicDbConfig::default()
    });
    DB_CONFIG.get().ok_or_else(|| JsValue::from_str("DbConfig not initialized"))
}


fn map_logic_error(err: logic::DbError) -> JsValue {
    JsValue::from_str(&err.to_string())
}

fn map_serde_error(err: serde_wasm_bindgen::Error) -> JsValue {
    JsValue::from_str(&format!("Serde WASM Bindgen error: {}", err))
}

fn map_json_error(err: serde_json::Error) -> JsValue {
    JsValue::from_str(&format!("JSON parse error: {}", err))
}


#[wasm_bindgen(start)]
pub fn main() -> Result<(), JsValue> {
    let _ = get_db_config()?;
    Ok(())
}

#[wasm_bindgen]
pub fn init_db(path: &str) -> Result<(), JsValue> {
    let db = Config::default()
        .path(path)
        .use_compression(true)
        .open()
        .map_err(|e| JsValue::from_str(&format!("Sled error: {}", e)))?;
    match DB.set(db) {
        Ok(()) => Ok(()),
        Err(_) => Err(JsValue::from_str("Database already initialized.")),
    }
}

#[wasm_bindgen]
pub fn set_key(key: &str, value_str: &str) -> Result<(), JsValue> {
    let db = get_db()?;
    let config = get_db_config()?;
    let value: Value = serde_json::from_str(value_str).map_err(map_json_error)?;
    logic::set_key(db, key, value, config).map_err(map_logic_error)?;
    Ok(())
}

#[wasm_bindgen]
pub fn get_key(key: &str) -> Result<JsValue, JsValue> {
    let db = get_db()?;
    let value = logic::get_key(db, key).map_err(map_logic_error)?;
    serde_wasm_bindgen::to_value(&value).map_err(map_serde_error)
}

#[wasm_bindgen(js_name = getPartialKey)]
pub fn get_partial_key(key: &str, fields_js: JsValue) -> Result<JsValue, JsValue> {
    let db = get_db()?;
    let fields: Vec<String> = serde_wasm_bindgen::from_value(fields_js)?;
    let value = logic::get_partial_key(db, key, &fields).map_err(map_logic_error)?;
    serde_wasm_bindgen::to_value(&value).map_err(map_serde_error)
}


#[wasm_bindgen]
pub async fn delete_key(key: &str) -> Result<(), JsValue> {
    let db = get_db()?;
    let config = get_db_config()?;
    logic::delete_key(db, key, config).await.map_err(map_logic_error)?;
    Ok(())
}

#[wasm_bindgen(js_name = batchSet)]
pub fn batch_set(items_str: &str) -> Result<(), JsValue> {
    let db = get_db()?;
    let config = get_db_config()?;
    let items: Vec<BatchSetItem> = serde_json::from_str(items_str).map_err(map_json_error)?;
    logic::batch_set(db, &items, config).map_err(map_logic_error)?;
    Ok(())
}

#[wasm_bindgen(js_name = executeTransaction)]
pub fn execute_transaction(operations_str: &str) -> Result<(), JsValue> {
    let db = get_db()?;
    let config = get_db_config()?;
    let operations: Vec<TransactionOperation> = serde_json::from_str(operations_str).map_err(map_json_error)?;
    logic::execute_transaction(db, &operations, config).map_err(map_logic_error)?;
    Ok(())
}

#[wasm_bindgen(js_name = clearPrefix)]
pub fn clear_prefix(prefix: &str) -> Result<usize, JsValue> {
    let db = get_db()?;
    let config = get_db_config()?;
    logic::clear_prefix(db, prefix, config).map_err(map_logic_error)
}

#[wasm_bindgen(js_name = dropDatabase)]
pub fn drop_database() -> Result<usize, JsValue> {
    let db = get_db()?;
    let config = get_db_config()?;
    logic::drop_database(db, config).map_err(map_logic_error)
}


#[wasm_bindgen]
pub fn export_data() -> Result<String, JsValue> {
    let db = get_db()?;
    logic_export_data(db).map_err(map_logic_error)
}

#[wasm_bindgen]
pub fn import_data(data: &str) -> Result<(), JsValue> {
    let db = get_db()?;
    let config = get_db_config()?;
    logic_import_data(db, data, config).map_err(map_logic_error)?;
    Ok(())
}

#[wasm_bindgen(js_name = queryWithinRadius)]
pub fn query_within_radius(field: &str, lat: f64, lon: f64, radius: f64) -> Result<JsValue, JsValue> {
    let db = get_db()?;
    let results = logic::query_within_radius_simplified(db, field, lat, lon, radius).map_err(map_logic_error)?;
    serde_wasm_bindgen::to_value(&results).map_err(map_serde_error)
}

#[wasm_bindgen(js_name = queryInBox)]
pub fn query_in_box(field: &str, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> Result<JsValue, JsValue> {
    let db = get_db()?;
    let results = logic::query_in_box(db, field, min_lat, min_lon, max_lat, max_lon).map_err(map_logic_error)?;
    serde_wasm_bindgen::to_value(&results).map_err(map_serde_error)
}

#[wasm_bindgen(js_name = queryAnd)]
pub fn query_and(conditions_str: &str) -> Result<JsValue, JsValue> {
    let db = get_db()?;
    let conditions: Vec<(String, String, String)> = serde_json::from_str(conditions_str)
        .map_err(|e| JsValue::from_str(&format!("Failed to parse conditions: {}", e)))?;

    let conditions_owned: Vec<(&str, &str, &str)> = conditions.iter()
        .map(|(f, o, v)| (f.as_str(), o.as_str(), v.as_str()))
        .collect();

    let results = logic::query_and(db, conditions_owned).map_err(map_logic_error)?;
    serde_wasm_bindgen::to_value(&results).map_err(map_serde_error)
}

#[wasm_bindgen(js_name = queryAst)]
pub fn query_ast(
    query_node_str: &str,
    projection_str: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>
) -> Result<JsValue, JsValue> {
    let db = get_db()?;
    let query_node: logic::QueryNode = serde_json::from_str(query_node_str)
        .map_err(|e| JsValue::from_str(&format!("Failed to parse AST query: {}", e)))?;

    let projection: Option<Vec<String>> = projection_str
        .map(|s| serde_json::from_str(&s).ok())
        .flatten();

    let results = logic::execute_ast_query(db, query_node, projection, limit, offset).map_err(map_logic_error)?;
    serde_wasm_bindgen::to_value(&results).map_err(map_serde_error)
}

#[wasm_bindgen]
pub fn backup() -> Result<String, JsValue> {
    let db = get_db()?;
    db.flush().map_err(|e| JsValue::from_str(&format!("Sled error: {}", e)))?;
    Ok("Backup placeholder: DB flushed. Manual directory copy needed.".to_string())
}

#[wasm_bindgen]
pub fn sync(_node_data: &str) -> Result<(), JsValue> {
    Ok(())
}