use wasm_bindgen::prelude::*;
use rust_db_logic as logic; // Use the logic crate
use sled::Db;
use serde_json::Value;
use std::sync::OnceLock;
// use wasm_bindgen_futures::JsFuture; // Needed for async operations in WASM

// --- Global DB Instance ---
static DB: OnceLock<Db> = OnceLock::new();

// Helper to get a reference to the initialized DB
fn get_db() -> Result<&'static Db, JsValue> {
    DB.get().ok_or_else(|| JsValue::from_str("Database not initialized. Call init_db first."))
}

// Helper to convert logic::DbError to JsValue
fn map_logic_error(err: logic::DbError) -> JsValue {
    JsValue::from_str(&err.to_string())
}

// --- WASM Exposed Functions ---

#[wasm_bindgen(start)]
pub fn main() -> Result<(), JsValue> {
    // Optional: set up logging or panic hooks here if needed
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

// Make the WASM delete_key async as well
#[wasm_bindgen]
pub async fn delete_key(key: &str) -> Result<(), JsValue> { // Removed .await
    let db = get_db()?;
    // Await the async logic function before mapping the error
    logic::delete_key(db, key).await.map_err(map_logic_error)?;
    Ok(())
}

#[wasm_bindgen]
pub fn export_data() -> Result<String, JsValue> {
    let db = get_db()?;
    logic::export_data(db).map_err(map_logic_error)
}

#[wasm_bindgen]
pub fn import_data(data: &str) -> Result<(), JsValue> {
    let db = get_db()?;
    logic::import_data(db, data).map_err(map_logic_error)?;
    Ok(())
}

// --- Geo Query Wrappers ---

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

// --- Placeholder Wrappers ---

#[wasm_bindgen]
pub fn backup() -> Result<String, JsValue> {
     let db = get_db()?;
     db.flush().map_err(|e| JsValue::from_str(&format!("Sled error: {}", e)))?;
    Ok("Backup placeholder: DB flushed. Manual directory copy needed.".to_string())
}

#[wasm_bindgen]
pub fn sync(_node_data: &str) -> Result<(), JsValue> {
    // Placeholder
    Ok(())
}