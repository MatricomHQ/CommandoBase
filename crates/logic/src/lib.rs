use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use sled::{Db, transaction::{TransactionalTree, TransactionError, ConflictableTransactionError, UnabortableTransactionError}}; // Removed Transactional
use std::collections::HashMap;
use thiserror::Error;
use tracing::{error, debug, warn, info};
// Removed unused std::io imports
// Removed unused std::convert::Infallible import

use geo::{Coord, Point, Rect, prelude::*}; // Updated geo imports
use geohash::{encode, neighbors}; // Removed decode
use geo::HaversineDistance; // Import HaversineDistance trait


pub const GEO_INDEX_PREFIX: &str = "__geo__";
pub const GEOHASH_PRECISION: usize = 9;
pub const CAS_RETRY_LIMIT: u32 = 10;
pub const DEFAULT_DB_PATH: &str = "database_data_server";
pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:3000";


#[derive(Error, Debug)]
pub enum DbError {
    #[error("Sled database error: {0}")]
    Sled(#[from] sled::Error),
    #[error("Serialization/Deserialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Geohash error: {0}")]
    Geohash(String),
    #[error("Import error: {0}")]
    ImportError(String),
    #[error("CAS retry limit exceeded: {0}")]
    CasRetryLimit(String),
    #[error("UTF8 conversion error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
    #[error("Key not found")]
    NotFound,
    #[error("Missing request data: {0}")]
    MissingData(String),
    #[error("Sled transaction error: {0}")]
    Transaction(String), // Keep this for potential future use if needed
    #[error("IO Error during operation: {0}")]
    Io(#[from] std::io::Error), // Keep this for potential future use if needed
}


impl From<TransactionError<DbError>> for DbError {
    fn from(e: TransactionError<DbError>) -> Self {
        match e {
            TransactionError::Abort(db_err) => db_err,
            TransactionError::Storage(sled_err) => DbError::Sled(sled_err),
        }
    }
}

// Corrected implementation again - matching the Storage variant
impl From<UnabortableTransactionError> for DbError {
    fn from(e: UnabortableTransactionError) -> Self {
        match e {
            UnabortableTransactionError::Storage(sled_err) => DbError::Sled(sled_err),
            // Abort variant uses Infallible, so this match arm is technically unreachable
             _ => DbError::Sled(sled::Error::Unsupported("Unreachable Abort in UnabortableTransactionError".into())),
        }
    }
}


pub type DbResult<T> = Result<T, DbError>;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GeoPoint {
    pub lat: f64,
    pub lon: f64,
}

impl From<GeoPoint> for Point<f64> {
    fn from(gp: GeoPoint) -> Self { Point::new(gp.lon, gp.lat) }
}
impl From<GeoPoint> for Coord<f64> {
    fn from(gp: GeoPoint) -> Self { Coord { x: gp.lon, y: gp.lat } }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GeoEntry {
    key: String,
    geohash: String,
    point: GeoPoint,
}


fn get_geo_index_key(field: &str, geohash: &str) -> String {
    format!("{}{}:{}", GEO_INDEX_PREFIX, field, geohash)
}


pub fn set_key(db: &Db, key: &str, value: Value) -> DbResult<()> {
    debug!(key = key, "Setting key transactionally");
    let serialized_value = serde_json::to_vec(&value)?; // Error handled outside transaction
    let key_bytes = key.as_bytes();

    db.transaction(|tx_db| {
        let old_value_opt = tx_db.get(key_bytes)?; // Propagates sled::Error

        if let Some(old_ivec) = old_value_opt {
             // Ignore serde errors when reading old value for cleanup
             if let Ok(old_val) = serde_json::from_slice::<Value>(&old_ivec) {
                 if let Value::Object(map) = &old_val {
                    for (field_name, field_value) in map {
                        // Ignore serde errors when reading old geo points for cleanup
                        if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(field_value.clone()) {
                            // Handle DbResult from helper explicitly
                            if let Err(e) = remove_geospatial_index(tx_db, key, field_name, &geo_point) {
                                return Err(ConflictableTransactionError::Abort(e));
                            }
                        }
                    }
                }
            }
        }

        tx_db.insert(key_bytes, serialized_value.clone())?; // Propagates sled::Error

        if let Value::Object(map) = &value {
            for (field_name, field_value) in map {
                 // Ignore serde errors when reading new geo points for indexing
                if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(field_value.clone()) {
                     // Handle DbResult from helper explicitly
                     if let Err(e) = index_geospatial_field(tx_db, key, field_name, &geo_point) {
                         return Err(ConflictableTransactionError::Abort(e));
                     }
                }
            }
        }
        Ok(()) // Transaction succeeded
    })?; // Converts ConflictableTransactionError into DbError

    debug!(key = key, "Successfully set key transactionally");
    Ok(())
}

pub fn get_key(db: &Db, key: &str) -> DbResult<Value> {
     debug!(key = key, "Getting key");
     match db.get(key.as_bytes())? {
        Some(ivec) => {
            let value: Value = serde_json::from_slice(&ivec)?;
            Ok(value)
        }
        None => Err(DbError::NotFound),
    }
}

// Revert to async delete_key with flush
pub async fn delete_key(db: &Db, key: &str) -> DbResult<()> {
    debug!(key = key, "Deleting key");
    let value_opt = db.get(key.as_bytes())?;

    if let Some(ivec) = value_opt {
        // Use a transaction just for the index removal part
        // Ignore errors during index removal to ensure main key deletion happens
        // Add type annotation here
        let _ : Result<(), TransactionError<DbError>> = db.transaction(|tx_db| {
            if let Ok(val) = serde_json::from_slice::<Value>(&ivec) {
                 if let Value::Object(map) = &val {
                    for (field_name, field_value) in map {
                        if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(field_value.clone()) {
                            if let Err(e) = remove_geospatial_index(tx_db, key, field_name, &geo_point) {
                                // Log the error but don't abort the main delete
                                error!(key=key, field=field_name, error=%e, "Failed to remove geo index during delete transaction");
                                // Optionally return Abort here if index removal failure should prevent key deletion
                                // return Err(ConflictableTransactionError::Abort(e));
                            }
                        }
                    }
                }
            }
            Ok(()) // Index removal transaction succeeded or was skipped/errored
        }); // Ignore storage errors from the index removal transaction as well

        // Remove the main key outside the index transaction
        db.remove(key.as_bytes())?;
        // Explicitly flush after removal
        db.flush_async().await?;
        debug!(key = key, "Successfully deleted and flushed key");
        Ok(())
    } else {
        warn!(key = key, "Attempted to delete non-existent key");
        Ok(())
    }
}


pub fn export_data(db: &Db) -> DbResult<String> {
    info!("Exporting database data");
    let mut data = Vec::new();
    for result in db.iter() {
        let (key, value) = result?;
         if !key.starts_with(GEO_INDEX_PREFIX.as_bytes()) {
            let key_str = String::from_utf8(key.to_vec())?;
            let value_json: Value = serde_json::from_slice(&value)?;
            data.push(json!({ "key": key_str, "value": value_json }));
         }
    }
    info!("Exported {} records", data.len());
    Ok(serde_json::to_string(&data)?)
}

pub fn import_data(db: &Db, data: &str) -> DbResult<()> {
    info!("Importing database data");
     let json_data: Vec<Value> = serde_json::from_str(data)?; // Error handled outside transaction
     let record_count = json_data.len();

    // Use set_key which is already transactional
    for item in json_data {
         let key = item.get("key")
             .and_then(Value::as_str)
             .ok_or_else(|| DbError::ImportError("Invalid key format".to_string()))?;
         let value_json = item.get("value")
              .ok_or_else(|| DbError::ImportError("Missing value".to_string()))?;
         set_key(db, key, value_json.clone())?;
    }


    info!("Successfully imported {} records", record_count);
    Ok(())
}


// Helper returns DbResult now
fn index_geospatial_field(tx_db: &TransactionalTree, key: &str, field: &str, point: &GeoPoint) -> DbResult<()> {
    let coord: Coord<f64> = point.clone().into(); // Changed GeoCoordinate to Coord
    // Map geohash error to DbError::Geohash
    let hash = encode(coord, GEOHASH_PRECISION).map_err(|e| DbError::Geohash(e.to_string()))?;
    debug!(key=key, field=field, hash=hash, "Indexing geo field (transactional)");

    let geo_entry = GeoEntry {
        key: key.to_string(),
        geohash: hash.clone(),
        point: point.clone(),
    };

    let index_key = get_geo_index_key(field, &hash);
    let index_key_bytes = index_key.as_bytes();

    // Use ? directly, relying on DbError's From<UnabortableTransactionError>
    let current_value_opt = tx_db.get(index_key_bytes)?;
    let mut entries = current_value_opt
        .as_ref()
        // Map serde error
        .and_then(|ivec| serde_json::from_slice::<Vec<GeoEntry>>(ivec).ok())
        .unwrap_or_default();

    let key_exists = entries.iter().any(|entry| entry.key == key);
    if !key_exists {
        entries.push(geo_entry.clone());
    } else {
         debug!(key=key, field=field, hash=hash, "Key already exists in geo index, skipping update (transactional)");
         return Ok(());
    }

    // Map serde error
    let serialized_entries = serde_json::to_vec(&entries)?;
    // Use ? directly, relying on DbError's From<UnabortableTransactionError>
    tx_db.insert(index_key_bytes, serialized_entries)?;
    debug!(key=key, field=field, hash=hash, "Successfully updated geo index (transactional)");

    Ok(())
}

// Helper returns DbResult now
fn remove_geospatial_index(tx_db: &TransactionalTree, key: &str, field: &str, point: &GeoPoint) -> DbResult<()> {
     let coord: Coord<f64> = point.clone().into();
     // Map geohash error to DbError::Geohash
     let hash = encode(coord, GEOHASH_PRECISION).map_err(|e| DbError::Geohash(e.to_string()))?;
     let index_key = get_geo_index_key(field, &hash);
     let index_key_bytes = index_key.as_bytes();
     debug!(key=key, field=field, hash=hash, "Removing geo index (transactional)");

     // Use ? directly, relying on DbError's From<UnabortableTransactionError>
     let current_value_opt = tx_db.get(index_key_bytes)?;

     if let Some(ivec) = current_value_opt {
         // Ignore serde error during cleanup attempt
         if let Ok(mut entries) = serde_json::from_slice::<Vec<GeoEntry>>(&ivec) {
             let initial_len = entries.len();
             entries.retain(|entry| entry.key != key);

             if entries.len() < initial_len {
                 if entries.is_empty() {
                     // Use ? directly, relying on DbError's From<UnabortableTransactionError>
                     tx_db.remove(index_key_bytes)?;
                     debug!(key=key, field=field, hash=hash, "Successfully removed entry and index key from geo index (transactional)");
                 } else {
                     // Map serde error
                     let next_value_bytes = serde_json::to_vec(&entries)?;
                     // Use ? directly, relying on DbError's From<UnabortableTransactionError>
                     tx_db.insert(index_key_bytes, next_value_bytes)?;
                     debug!(key=key, field=field, hash=hash, "Successfully removed entry from geo index (transactional)");
                 }
             } else {
                  debug!(key=key, field=field, hash=hash, "Key not found in geo index entry, nothing to remove (transactional)");
             }
         } else {
              warn!(key=key, field=field, hash=hash, "Failed to deserialize geo index entry during removal, skipping (transactional)");
         }
     } else {
          debug!(key=key, field=field, hash=hash, "Geo index key not found, nothing to remove (transactional)");
     }

     Ok(())
}


pub fn query_within_radius_simplified(db: &Db, field: &str, center_lat: f64, center_lon: f64, radius_meters: f64) -> DbResult<Vec<Value>> {
    debug!(field=field, lat=center_lat, lon=center_lon, radius=radius_meters, "Querying within radius (simplified)");
    let center_point_geo: Point<f64> = GeoPoint{ lat: center_lat, lon: center_lon }.into();
    let center_coord_geo: Coord<f64> = GeoPoint{ lat: center_lat, lon: center_lon }.into(); // Changed GeoCoordinate to Coord
    let center_hash = encode(center_coord_geo, GEOHASH_PRECISION).map_err(|e| DbError::Geohash(e.to_string()))?;

    let mut results_map: HashMap<String, Value> = HashMap::new();
    let index_key = get_geo_index_key(field, &center_hash);
    debug!(field=field, hash=center_hash, "Checking central hash");

    if let Some(ivec) = db.get(&index_key)? {
        if let Ok(entries) = serde_json::from_slice::<Vec<GeoEntry>>(&ivec) {
            for entry in entries {
                let entry_point: Point<f64> = entry.point.into();
                let distance = entry_point.haversine_distance(&center_point_geo);
                if distance <= radius_meters {
                     debug!(key=entry.key, distance=distance, "Found match within radius");
                     if !results_map.contains_key(&entry.key) {
                        match get_key(db, &entry.key) {
                            Ok(value) => { results_map.insert(entry.key.clone(), value); },
                            Err(DbError::NotFound) => warn!(key=entry.key, "Geo index points to non-existent key"),
                            Err(e) => return Err(e),
                        }
                     }
                }
            }
        } else {
             warn!(index_key=index_key, "Failed to deserialize geo index entry");
        }
    } else {
         debug!(index_key=index_key, "Central hash index key not found");
    }
    warn!("Radius query currently only checks the central geohash, results may be incomplete");
    Ok(results_map.into_values().collect())
}

pub fn query_in_box(db: &Db, field: &str, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> DbResult<Vec<Value>> {
    debug!(field=field, min_lat=min_lat, min_lon=min_lon, max_lat=max_lat, max_lon=max_lon, "Querying in box");
    let bounding_box = Rect::new(
        Coord { x: min_lon, y: min_lat }, // Changed GeoCoordinate to Coord
        Coord { x: max_lon, y: max_lat }, // Changed GeoCoordinate to Coord
    );

    let prefix = format!("{}{}:", GEO_INDEX_PREFIX, field);
    let mut results_map: HashMap<String, Value> = HashMap::new();

    for item_result in db.scan_prefix(prefix.as_bytes()) {
         let (_key, value_ivec) = item_result?;
         if let Ok(entries) = serde_json::from_slice::<Vec<GeoEntry>>(&value_ivec) {
             for entry in entries {
                 let entry_point: Point<f64> = entry.point.into();
                 if bounding_box.contains(&entry_point) {
                      debug!(key=entry.key, "Found match within box");
                     if !results_map.contains_key(&entry.key) {
                         match get_key(db, &entry.key) {
                            Ok(value) => { results_map.insert(entry.key.clone(), value); },
                            Err(DbError::NotFound) => warn!(key=entry.key, "Geo index points to non-existent key"),
                            Err(e) => return Err(e),
                        }
                     }
                 }
             }
         } else {
              warn!("Failed to deserialize geo index entry during box query scan");
         }
    }
    Ok(results_map.into_values().collect())
}