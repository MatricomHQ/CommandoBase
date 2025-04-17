use wasm_bindgen::prelude::*;
use rust_db_logic::{self as logic, export_data as logic_export_data, import_data as logic_import_data};
use sled::Db;
use serde_json::Value;
use std::sync::OnceLock;
use serde_wasm_bindgen;

static DB: OnceLock<Db> = OnceLock::new();

fn get_db() -> Result<&'static Db, JsValue> {
    DB.get().ok_or_else(|| JsValue::from_str("Database not initialized. Call init_db first."))
}

fn map_logic_error(err: logic::DbError) -> JsValue {
    JsValue::from_str(&err.to_string())
}

#[wasm_bindgen(start)]
pub fn main() -> Result<(), JsValue> {
    Ok(())
}

#[wasm_bindgen]
pub fn init_db(path: &str) -> Result<(), JsValue> {
    let db = sled::open(path).map_err(|e| JsValue::from_str(&format!("Sled error: {}", e)))?;
    match DB.set(db) {
        Ok(()) => Ok(()),
        Err(_) => Err(JsValue::from_str("Database already initialized.")),
    }
}

#[wasm_bindgen]
pub fn set_key(key: &str, value_str: &str) -> Result<(), JsValue> {
    let db = get_db()?;
    let value: Value = serde_json::from_str(value_str).map_err(|e| JsValue::from_str(&format!("Serde error: {}", e)))?;
    logic::set_key(db, key, value).map_err(map_logic_error)?;
    Ok(())
}

#[wasm_bindgen]
pub fn get_key(key: &str) -> Result<JsValue, JsValue> {
    let db = get_db()?;
    let value = logic::get_key(db, key).map_err(map_logic_error)?;
    serde_wasm_bindgen::to_value(&value).map_err(|e| JsValue::from_str(&format!("SerdeWasmBindgen error: {}", e)))
}

#[wasm_bindgen]
pub async fn delete_key(key: &str) -> Result<(), JsValue> {
    let db = get_db()?;
    logic::delete_key(db, key).await.map_err(map_logic_error)?;
    Ok(())
}

#[wasm_bindgen]
pub fn export_data() -> Result<String, JsValue> {
    let db = get_db()?;
    logic_export_data(db).map_err(map_logic_error)
}

#[wasm_bindgen]
pub fn import_data(data: &str) -> Result<(), JsValue> {
    let db = get_db()?;
    logic_import_data(db, data).map_err(map_logic_error)?;
    Ok(())
}

#[wasm_bindgen(js_name = queryWithinRadius)]
pub fn query_within_radius(field: &str, lat: f64, lon: f64, radius: f64) -> Result<JsValue, JsValue> {
    let db = get_db()?;
    let results = logic::query_within_radius_simplified(db, field, lat, lon, radius).map_err(map_logic_error)?;
    serde_wasm_bindgen::to_value(&results).map_err(|e| JsValue::from_str(&format!("SerdeWasmBindgen error: {}", e)))
}

#[wasm_bindgen(js_name = queryInBox)]
pub fn query_in_box(field: &str, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> Result<JsValue, JsValue> {
    let db = get_db()?;
    let results = logic::query_in_box(db, field, min_lat, min_lon, max_lat, max_lon).map_err(map_logic_error)?;
    serde_wasm_bindgen::to_value(&results).map_err(|e| JsValue::from_str(&format!("SerdeWasmBindgen error: {}", e)))
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
    serde_wasm_bindgen::to_value(&results).map_err(|e| JsValue::from_str(&format!("SerdeWasmBindgen error: {}", e)))
}

#[wasm_bindgen(js_name = queryAst)]
pub fn query_ast(query_node_str: &str) -> Result<JsValue, JsValue> {
    let db = get_db()?;
    let query_node: logic::QueryNode = serde_json::from_str(query_node_str)
        .map_err(|e| JsValue::from_str(&format!("Failed to parse AST query: {}", e)))?;
    // Handle the Result from build_query_plan
    let plan = logic::build_query_plan(db, query_node).map_err(map_logic_error)?;
    // Pass the unwrapped plan to execute_query_plan
    let results = logic::execute_query_plan(db, plan).map_err(map_logic_error)?;
    serde_wasm_bindgen::to_value(&results).map_err(|e| JsValue::from_str(&format!("SerdeWasmBindgen error: {}", e)))
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