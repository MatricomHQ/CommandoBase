use serde::{Serialize, Deserialize, de::Error as SerdeError};
use serde_json::{Value, json, Map};
use sled::{Db, IVec, Batch, transaction::{TransactionError, UnabortableTransactionError, ConflictableTransactionError, TransactionalTree}};
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tracing::{error, debug, warn};
use geo::{Coord, Point, Rect, prelude::*};
use geohash::{encode, neighbors as geohash_neighbors, Neighbors}; // Removed decode_bbox
use std::convert::TryInto;
use std::cmp::Ordering;
use hex;
use lazy_static::lazy_static;
use regex::Regex;
// Removed TypeId
use std::ops::Bound;
use std::hash::{Hash, Hasher};
// Removed Arc
// Removed FromIterator

pub const GEO_SORTED_INDEX_PREFIX: &str = "__geo_sorted__";
pub const GEOHASH_PRECISION: usize = 9;
pub const CAS_RETRY_LIMIT: u32 = 10;
pub const DEFAULT_DB_PATH: &str = "database_data_server";
pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:3000";
pub const FIELD_INDEX_PREFIX: &str = "__field_index__";
pub const FIELD_SORTED_INDEX_PREFIX: &str = "__field_sorted__";

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
    #[error("Hex decoding error: {0}")]
    HexError(#[from] hex::FromHexError),
    #[error("Byte conversion error: {0}")]
    TryFromSlice(#[from] std::array::TryFromSliceError),
    #[error("Key not found")]
    NotFound,
    #[error("Missing request data: {0}")]
    MissingData(String),
    #[error("Sled transaction error: {0}")]
    Transaction(String),
    #[error("IO Error during operation: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid value for comparison: {0}")]
    InvalidComparisonValue(String),
    #[error("Value is not an object, cannot retrieve partial fields")]
    NotAnObject,
    #[error("Field not found in object: {0}")]
    FieldNotFound(String),
    #[error("Field is not a valid GeoPoint: {0}")]
    NotAGeoPoint(String),
    #[error("Invalid Geo Sorted Index Key format: {0}")]
    InvalidGeoSortedKey(String),
    #[error("AST Query Error: {0}")]
    AstQueryError(String),
    #[error("Invalid path for projection or nested query: {0}")]
    InvalidPath(String),
    #[error("Transaction operation failed: {0}")]
    TransactionOperationFailed(String),
    #[error("Invalid Field Index Key format: {0}")] // Added
    InvalidFieldIndexKey(String),
}

impl From<TransactionError<DbError>> for DbError {
    fn from(e: TransactionError<DbError>) -> Self {
        match e {
            TransactionError::Abort(db_err) => db_err,
            TransactionError::Storage(sled_err) => DbError::Sled(sled_err),
        }
    }
}

impl From<UnabortableTransactionError> for DbError {
    fn from(e: UnabortableTransactionError) -> Self {
        match e {
            UnabortableTransactionError::Storage(sled_err) => DbError::Sled(sled_err),
            _ => DbError::Sled(sled::Error::Unsupported("Unreachable Abort in UnabortableTransactionError".into())),
        }
    }
}

pub type DbResult<T> = Result<T, DbError>;

#[derive(Debug, Clone, Default)]
pub struct DbConfig {
    pub hash_indexed_fields: HashSet<String>,
    pub sorted_indexed_fields: HashSet<String>,
    pub geo_indexed_fields: HashSet<String>,
}

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

fn get_geo_sorted_index_key(field_path: &str, geohash: &str, key: &str) -> String {
    format!("{}{}:{}:{}", GEO_SORTED_INDEX_PREFIX, field_path, geohash, key)
}

fn get_geo_sorted_index_prefix_for_hash(field_path: &str, geohash: &str) -> String {
    format!("{}{}:{}:", GEO_SORTED_INDEX_PREFIX, field_path, geohash)
}

fn get_geo_sorted_index_prefix_for_field(field_path: &str) -> String {
    format!("{}{}:", GEO_SORTED_INDEX_PREFIX, field_path)
}


// Modified: Include primary_key
fn get_field_index_key(field_path: &str, value: &str, primary_key: &str) -> String {
    format!("{}{}:{}:{}", FIELD_INDEX_PREFIX, field_path, value, primary_key)
}

// Added: Prefix for scanning hash index
fn get_field_index_prefix(field_path: &str, value: &str) -> String {
    format!("{}{}:{}:", FIELD_INDEX_PREFIX, field_path, value)
}

fn get_field_sorted_index_key(field_path: &str, encoded_value: &[u8], key: &str) -> String {
    format!("{}{}:{}:{}", FIELD_SORTED_INDEX_PREFIX, field_path, hex::encode(encoded_value), key)
}

fn get_field_sorted_index_prefix(field_path: &str) -> String {
    format!("{}{}:", FIELD_SORTED_INDEX_PREFIX, field_path)
}

fn encode_sorted_value(value: &Value) -> DbResult<Vec<u8>> {
    let mut buf = Vec::new();
    match value {
        Value::Number(num) => {
            if let Some(i) = num.as_i64() {
                buf.push(0x01);
                buf.extend_from_slice(&i.to_be_bytes());
            } else if let Some(u) = num.as_u64() {
                buf.push(0x02);
                buf.extend_from_slice(&u.to_be_bytes());
            } else if let Some(f) = num.as_f64() {
                buf.push(0x03);
                buf.extend_from_slice(&f.to_be_bytes());
            } else {
                return Err(DbError::Serde(serde_json::Error::custom("Unsupported number type")));
            }
        }
        Value::String(s) => {
            buf.push(0x04);
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Bool(b) => {
            buf.push(0x05);
            buf.push(if *b { 1 } else { 0 });
        }
        _ => return Err(DbError::Serde(serde_json::Error::custom("Unsupported type for sorted index"))),
    }
    Ok(buf)
}

fn decode_sorted_value(encoded: &[u8]) -> DbResult<Value> {
    if encoded.is_empty() {
        return Err(DbError::Serde(serde_json::Error::custom("Empty encoded value")));
    }
    match encoded[0] {
        0x01 => {
            if encoded.len() < 9 { return Err(DbError::Serde(serde_json::Error::custom("Invalid i64 encoding length"))); }
            let num = i64::from_be_bytes(encoded[1..9].try_into()?);
            Ok(Value::Number(num.into()))
        }
        0x02 => {
            if encoded.len() < 9 { return Err(DbError::Serde(serde_json::Error::custom("Invalid u64 encoding length"))); }
            let num = u64::from_be_bytes(encoded[1..9].try_into()?);
            Ok(Value::Number(num.into()))
        }
        0x03 => {
            if encoded.len() < 9 { return Err(DbError::Serde(serde_json::Error::custom("Invalid f64 encoding length"))); }
            let num = f64::from_be_bytes(encoded[1..9].try_into()?);
            Ok(Value::Number(serde_json::Number::from_f64(num).ok_or_else(|| DbError::Serde(serde_json::Error::custom("Invalid f64")))?) )
        }
        0x04 => {
            let s = String::from_utf8(encoded[1..].to_vec())?;
            Ok(Value::String(s))
        }
        0x05 => {
            if encoded.len() < 2 { return Err(DbError::Serde(serde_json::Error::custom("Invalid bool encoding length"))); }
            Ok(Value::Bool(encoded[1] != 0))
        }
        _ => Err(DbError::Serde(serde_json::Error::custom("Unknown type byte"))),
    }
}

lazy_static! {
    static ref NUM_RE: Regex = Regex::new(r"^-?\d+(\.\d+)?$").unwrap();
}

fn parse_value(value_str: &str) -> DbResult<Value> {
    if value_str == "true" {
        Ok(Value::Bool(true))
    } else if value_str == "false" {
        Ok(Value::Bool(false))
    } else if NUM_RE.is_match(value_str) {
        if let Ok(i) = value_str.parse::<i64>() {
            Ok(Value::Number(i.into()))
        } else if let Ok(f) = value_str.parse::<f64>() {
            Ok(Value::Number(serde_json::Number::from_f64(f).ok_or_else(|| DbError::InvalidComparisonValue(format!("Invalid f64 format: {}", value_str)))?))
        } else {
            Err(DbError::InvalidComparisonValue(format!("Could not parse number: {}", value_str)))
        }
    } else {
        Ok(Value::String(value_str.trim_matches('"').to_string()))
    }
}

fn compare_values(v1: &Value, v2: &Value) -> Option<Ordering> {
    match (v1, v2) {
        (Value::Number(n1), Value::Number(n2)) => {
            if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                f1.partial_cmp(&f2)
            } else {
                None
            }
        }
        (Value::String(s1), Value::String(s2)) => s1.partial_cmp(s2),
        (Value::Bool(b1), Value::Bool(b2)) => b1.partial_cmp(b2),
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        _ => {
            if std::mem::discriminant(v1) != std::mem::discriminant(v2) {
                 None
            } else {
                 None
            }
        }
    }
}

fn index_value_recursive(
    tx_db: &TransactionalTree,
    key: &str, // primary key
    current_path: &str,
    value: &Value,
    config: &DbConfig,
    batch: &mut Batch,
) -> DbResult<()> {
    match value {
        Value::Object(map) => {
            for (field_name, field_value) in map {
                let new_path = if current_path.is_empty() {
                    field_name.clone()
                } else {
                    format!("{}.{}", current_path, field_name)
                };

                if config.geo_indexed_fields.contains(&new_path) {
                    if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(field_value.clone()) {
                        index_geospatial_field(tx_db, key, &new_path, &geo_point)?;
                    } else if !field_value.is_null() {
                         warn!(key=key, path=%new_path, "Field configured for geo indexing is not a valid GeoPoint or null");
                    }
                }

                index_value_recursive(tx_db, key, &new_path, field_value, config, batch)?;
            }
        }
        Value::Array(arr) => {
            for (index, elem) in arr.iter().enumerate() {
                let index_path = format!("{}.{}", current_path, index); // Path to the element itself
                index_value_recursive(tx_db, key, &index_path, elem, config, batch)?;

                // Index primitive values within the array against the array's path
                if config.hash_indexed_fields.contains(current_path) {
                     if !elem.is_object() && !elem.is_array() { // Only index primitives directly
                         let elem_str = elem.to_string().trim_matches('"').to_string();
                         // Modified: Use new key format, insert empty value
                         let index_key = get_field_index_key(current_path, &elem_str, key);
                         batch.insert(index_key.as_bytes(), vec![]);
                     }
                }
                 // Index sortable primitive values within the array against the array's path
                 if config.sorted_indexed_fields.contains(current_path) {
                     if let Ok(encoded) = encode_sorted_value(elem) {
                         let sorted_index_key = get_field_sorted_index_key(current_path, &encoded, key);
                         batch.insert(sorted_index_key.as_bytes(), vec![]);
                     }
                 }
            }
        }
        _ => { // Primitive value
            if config.hash_indexed_fields.contains(current_path) {
                let value_str = value.to_string().trim_matches('"').to_string();
                // Modified: Use new key format, insert empty value
                let index_key = get_field_index_key(current_path, &value_str, key);
                batch.insert(index_key.as_bytes(), vec![]);
            }
            if config.sorted_indexed_fields.contains(current_path) {
                if let Ok(encoded) = encode_sorted_value(value) {
                    let sorted_index_key = get_field_sorted_index_key(current_path, &encoded, key);
                    batch.insert(sorted_index_key.as_bytes(), vec![]);
                }
            }
        }
    }
    Ok(())
}

fn remove_indices_recursive(
    tx_db: &TransactionalTree,
    key: &str, // primary key
    current_path: &str,
    value: &Value,
    config: &DbConfig,
    batch: &mut Batch,
) -> DbResult<()> {
     match value {
        Value::Object(map) => {
            for (field_name, field_value) in map {
                let new_path = if current_path.is_empty() {
                    field_name.clone()
                } else {
                    format!("{}.{}", current_path, field_name)
                };

                if config.geo_indexed_fields.contains(&new_path) {
                    if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(field_value.clone()) {
                         remove_geospatial_index(tx_db, key, &new_path, &geo_point)?;
                    }
                }

                remove_indices_recursive(tx_db, key, &new_path, field_value, config, batch)?;
            }
        }
        Value::Array(arr) => {
            for (index, elem) in arr.iter().enumerate() {
                let index_path = format!("{}.{}", current_path, index);
                remove_indices_recursive(tx_db, key, &index_path, elem, config, batch)?;

                 if config.hash_indexed_fields.contains(current_path) {
                     if !elem.is_object() && !elem.is_array() {
                         let elem_str = elem.to_string().trim_matches('"').to_string();
                         // Modified: Use new key format for removal
                         let index_key = get_field_index_key(current_path, &elem_str, key);
                         batch.remove(index_key.as_bytes());
                     }
                 }
                 if config.sorted_indexed_fields.contains(current_path) {
                     if let Ok(encoded) = encode_sorted_value(elem) {
                         let sorted_index_key = get_field_sorted_index_key(current_path, &encoded, key);
                         batch.remove(sorted_index_key.as_bytes());
                     }
                 }
            }
        }
        _ => { // Primitive value
            if config.hash_indexed_fields.contains(current_path) {
                let value_str = value.to_string().trim_matches('"').to_string();
                // Modified: Use new key format for removal
                let index_key = get_field_index_key(current_path, &value_str, key);
                batch.remove(index_key.as_bytes());
            }
            if config.sorted_indexed_fields.contains(current_path) {
                if let Ok(encoded) = encode_sorted_value(value) {
                    let sorted_index_key = get_field_sorted_index_key(current_path, &encoded, key);
                    batch.remove(sorted_index_key.as_bytes());
                }
            }
        }
    }
    Ok(())
}


fn set_key_internal(tx_db: &TransactionalTree, key: &str, value: &Value, config: &DbConfig) -> DbResult<()> { // Take value by reference
    let serialized_value = serde_json::to_vec(value)?;
    let key_bytes = key.as_bytes();
    let mut removal_batch = Batch::default();
    let mut creation_batch = Batch::default();

    if let Some(old_ivec) = tx_db.get(key_bytes)? {
        if let Ok(old_val) = serde_json::from_slice::<Value>(&old_ivec) {
             remove_indices_recursive(tx_db, key, "", &old_val, config, &mut removal_batch)?;
        }
    }

    tx_db.apply_batch(&removal_batch)?;
    tx_db.insert(key_bytes, serialized_value.clone())?;
    index_value_recursive(tx_db, key, "", value, config, &mut creation_batch)?; // Pass reference
    tx_db.apply_batch(&creation_batch)?;
    Ok(())
}

pub fn set_key(db: &Db, key: &str, value: Value, config: &DbConfig) -> DbResult<()> {
    db.transaction(|tx_db| {
        // Clone value here as it's moved into the closure
        set_key_internal(tx_db, key, &value, config).map_err(ConflictableTransactionError::Abort)
    })?;
    Ok(())
}

// Modified: Make fields public
#[derive(Deserialize, Debug)]
pub struct BatchSetItem {
    pub key: String,
    pub value: Value,
}

pub fn batch_set(db: &Db, items: &[BatchSetItem], config: &DbConfig) -> DbResult<()> { // Take slice
     db.transaction(|tx_db| {
         for item in items { // Iterate over slice
             set_key_internal(tx_db, &item.key, &item.value, config) // Pass references
                 .map_err(|e| ConflictableTransactionError::Abort(DbError::TransactionOperationFailed(format!("Batch set failed for key '{}': {}", item.key, e))))?;
         }
         Ok(())
     })?;
     Ok(())
}

fn delete_key_internal(tx_db: &TransactionalTree, key: &str, config: &DbConfig) -> DbResult<()> {
    let key_bytes = key.as_bytes();
    if let Some(ivec) = tx_db.get(key_bytes)? {
        let mut removal_batch = Batch::default();
        if let Ok(val) = serde_json::from_slice::<Value>(&ivec) {
             remove_indices_recursive(tx_db, key, "", &val, config, &mut removal_batch)?;
        }
        removal_batch.remove(key_bytes);
        tx_db.apply_batch(&removal_batch)?;
    }
    Ok(())
}

pub async fn delete_key(db: &Db, key: &str, config: &DbConfig) -> DbResult<()> {
    db.transaction(|tx_db| {
        delete_key_internal(tx_db, key, config).map_err(ConflictableTransactionError::Abort)
    })?;
    db.flush_async().await?;
    Ok(())
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum TransactionOperation {
    #[serde(rename = "set")]
    Set { key: String, value: Value },
    #[serde(rename = "delete")]
    Delete { key: String },
}

pub fn execute_transaction(db: &Db, operations: &[TransactionOperation], config: &DbConfig) -> DbResult<()> { // Take slice
    db.transaction(|tx_db| {
        for op in operations { // Iterate over slice
            match op {
                TransactionOperation::Set { key, value } => {
                    set_key_internal(tx_db, key, value, config) // Pass references
                        .map_err(|e| ConflictableTransactionError::Abort(DbError::TransactionOperationFailed(format!("Set failed for key '{}': {}", key, e))))?;
                }
                TransactionOperation::Delete { key } => {
                    delete_key_internal(tx_db, key, config)
                         .map_err(|e| ConflictableTransactionError::Abort(DbError::TransactionOperationFailed(format!("Delete failed for key '{}': {}", key, e))))?;
                }
            }
        }
        Ok(())
    })?;
    Ok(())
}


pub fn get_key(db: &Db, key: &str) -> DbResult<Value> {
    match db.get(key.as_bytes())? {
        Some(ivec) => {
            let value: Value = serde_json::from_slice(&ivec)?;
            Ok(value)
        }
        None => Err(DbError::NotFound),
    }
}

fn get_value_by_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for part in path.split('.') {
        if let Some(obj) = current.as_object() {
            current = obj.get(part)?;
        } else if let Some(arr) = current.as_array() {
            if let Ok(index) = part.parse::<usize>() {
                current = arr.get(index)?;
            } else {
                return None;
            }
        } else {
            return None;
        }
    }
    Some(current)
}

fn insert_value_by_path(target: &mut Value, path_parts: &[&str], value_to_insert: Value) -> DbResult<()> {
    if path_parts.is_empty() {
        return Err(DbError::InvalidPath("Empty path for insertion".to_string()));
    }

    let key = path_parts[0];

    if path_parts.len() == 1 {
        if let Some(obj) = target.as_object_mut() {
            obj.insert(key.to_string(), value_to_insert);
            Ok(())
        } else if let Some(arr) = target.as_array_mut() {
             if let Ok(index) = key.parse::<usize>() {
                 if index == arr.len() {
                     arr.push(value_to_insert);
                 } else if index < arr.len() {
                     arr[index] = value_to_insert;
                 } else {
                     return Err(DbError::InvalidPath(format!("Index {} out of bounds for array in path", index)));
                 }
                 Ok(())
             } else {
                 Err(DbError::InvalidPath(format!("Cannot insert object key '{}' into array", key)))
             }
        } else {
            Err(DbError::InvalidPath(format!("Cannot insert into non-container type at path step '{}'", key)))
        }
    } else {
        let next_target = if let Some(obj) = target.as_object_mut() {
            obj.entry(key.to_string())
               .or_insert_with(|| {
                   if path_parts.get(1).map_or(false, |p| p.parse::<usize>().is_ok()) {
                       Value::Array(vec![])
                   } else {
                       Value::Object(Map::new())
                   }
               })
        } else if let Some(arr) = target.as_array_mut() {
             if let Ok(index) = key.parse::<usize>() {
                 if index < arr.len() {
                     &mut arr[index]
                 } else if index == arr.len() {
                      let new_val = if path_parts.get(1).map_or(false, |p| p.parse::<usize>().is_ok()) {
                           Value::Array(vec![])
                       } else {
                           Value::Object(Map::new())
                       };
                      arr.push(new_val);
                      arr.last_mut().unwrap()
                 } else {
                      return Err(DbError::InvalidPath(format!("Index {} out of bounds for array creation in path", index)));
                 }
             } else {
                  return Err(DbError::InvalidPath(format!("Cannot access object key '{}' in array", key)));
             }
        } else {
             return Err(DbError::InvalidPath(format!("Cannot traverse non-container type at path step '{}'", key)));
        };

        insert_value_by_path(next_target, &path_parts[1..], value_to_insert)
    }
}


fn apply_projection(documents: Vec<Value>, projection: &Vec<String>) -> DbResult<Vec<Value>> {
    if projection.is_empty() {
        return Ok(documents);
    }

    let mut projected_results = Vec::new();
    for doc in documents {
        let mut projected_doc = Value::Object(Map::new());
        for path in projection {
             if let Some(value) = get_value_by_path(&doc, path) {
                 let parts: Vec<&str> = path.split('.').collect();
                 insert_value_by_path(&mut projected_doc, &parts, value.clone())?;
             } else {
                  let parts: Vec<&str> = path.split('.').collect();
                  if parts.len() > 1 {
                      let parent_path = parts[..parts.len()-1].join(".");
                      let last_part = parts.last().unwrap();
                      if let Some(Value::Array(arr)) = get_value_by_path(&doc, &parent_path) {
                          let projected_array_values: Vec<Value> = arr.iter()
                              .filter_map(|elem| elem.get(*last_part).cloned())
                              .collect();
                          if !projected_array_values.is_empty() {
                               // Projecting array elements needs careful path handling in insert_value_by_path
                               // For now, let's insert the array at the parent path
                               let parent_parts: Vec<&str> = parent_path.split('.').collect();
                               insert_value_by_path(&mut projected_doc, &parent_parts, Value::Array(projected_array_values))?;
                          }
                      } else {
                           warn!("Projection path '{}' not found in document (array check)", path);
                      }
                  } else {
                       warn!("Projection path '{}' not found in document", path);
                  }
             }
        }
         if projected_doc.as_object().map_or(false, |m| !m.is_empty()) || doc.as_object().map_or(false, |m| m.is_empty()) {
             projected_results.push(projected_doc);
         } else if !doc.is_object() && !doc.is_null() {
              warn!("Projection applied to non-object document, skipping result.");
         } else {
              projected_results.push(Value::Object(Map::new()));
         }
    }
    Ok(projected_results)
}


pub fn get_partial_key(db: &Db, key: &str, fields: &[String]) -> DbResult<Value> {
    let full_value = get_key(db, key)?;
    let projection_paths: Vec<String> = fields.iter().cloned().collect();
    let projected_docs = apply_projection(vec![full_value], &projection_paths)?;
    projected_docs.into_iter().next().ok_or(DbError::NotFound)
}


pub fn query_and(db: &Db, conditions: Vec<(&str, &str, &str)>) -> DbResult<Vec<Value>> {

    let mut key_sets: Vec<HashSet<String>> = Vec::new();

    for (field, operator, value_str) in &conditions {
        let mut current_keys = HashSet::new();
        match *operator {
            "===" | "includes" => {
                let value_parsed = parse_value(value_str)?;
                // Modified: Use fetch_keys_hash_index
                current_keys = fetch_keys_hash_index(db, field, &value_parsed)?;
            }
            ">" | "<" | ">=" | "<=" | "!=" => {
                let value = parse_value(value_str)?;

                let keys = fetch_keys_sorted_index(db, field, operator, &value, &DataType::Number)?;
                current_keys.extend(keys);
            }
            _ => return Err(DbError::MissingData(format!("Unsupported operator: {}", operator))),
        }
        key_sets.push(current_keys);
    }

    let common_keys = key_sets.into_iter()
        .fold(None::<HashSet<String>>, |acc, set| match acc {
            None => Some(set),
            Some(a) => Some(a.intersection(&set).cloned().collect()),
        })
        .unwrap_or_default();


    let results: DbResult<Vec<Value>> = common_keys.into_iter()
        .map(|k| get_key(db, &k))
        .collect();

    results
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub enum DataType {
    String,
    Number,
    Bool,
}


#[derive(Debug, Deserialize)]
pub enum QueryNode {
    Eq(String, Value, DataType),
    Includes(String, Value, DataType),
    Gt(String, Value, DataType),
    Lt(String, Value, DataType),
    Gte(String, Value, DataType),
    Lte(String, Value, DataType),
    Ne(String, Value, DataType),
    And(Box<QueryNode>, Box<QueryNode>),
    Or(Box<QueryNode>, Box<QueryNode>),
    Not(Box<QueryNode>),
    GeoWithinRadius { field: String, lat: f64, lon: f64, radius: f64 },
    GeoInBox { field: String, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64 },
}


// Modified: Fetch keys by scanning prefix and parsing primary key from index key
fn fetch_keys_hash_index(db: &Db, field_path: &str, value: &Value) -> DbResult<HashSet<String>> {
    let value_str = value.to_string().trim_matches('"').to_string();
    let prefix = get_field_index_prefix(field_path, &value_str);
    let mut primary_keys = HashSet::new();

    for result in db.scan_prefix(prefix.as_bytes()) {
        let (index_key_bytes, _) = result?;
        let index_key_str = String::from_utf8_lossy(&index_key_bytes);

        // Extract primary key from the end of the index key string
        // Format: __field_index__:<field_path>:<value_str>:<primary_key>
        if let Some(primary_key) = index_key_str.split(':').last() {
            primary_keys.insert(primary_key.to_string());
        } else {
             warn!("Invalid field index key format encountered during scan: {}", index_key_str);
             // Optionally return an error:
             return Err(DbError::InvalidFieldIndexKey(index_key_str.into_owned()));
        }
    }
    Ok(primary_keys)
}

fn fetch_keys_sorted_index(db: &Db, field_path: &str, operator: &str, value: &Value, _expected_type: &DataType) -> DbResult<HashSet<String>> {
    let mut current_keys = HashSet::new();
    let encoded_value = encode_sorted_value(value)?;
    let value_type_byte = encoded_value.first().copied();

    let prefix = get_field_sorted_index_prefix(field_path);
    let prefix_bytes = prefix.as_bytes();

    let start_key_gt = get_field_sorted_index_key(field_path, &encoded_value, "");
    let start_key_gte = get_field_sorted_index_key(field_path, &encoded_value, "");
    let end_key_lt = get_field_sorted_index_key(field_path, &encoded_value, "");
    let end_key_lte = get_field_sorted_index_key(field_path, &encoded_value, "\u{FFFF}");

    let range: (Bound<&[u8]>, Bound<&[u8]>) = match operator {
         ">" => (Bound::Excluded(start_key_gt.as_bytes()), Bound::Unbounded),
         ">=" => (Bound::Included(start_key_gte.as_bytes()), Bound::Unbounded),
         "<" => (Bound::Included(prefix_bytes), Bound::Excluded(end_key_lt.as_bytes())),
         "<=" => (Bound::Included(prefix_bytes), Bound::Included(end_key_lte.as_bytes())),
         "!=" => (Bound::Unbounded, Bound::Unbounded),
         _ => return Err(DbError::AstQueryError(format!("Unsupported operator for sorted index: {}", operator))),
     };

    let iterator = if operator == "!=" {
        Box::new(db.scan_prefix(prefix_bytes)) as Box<dyn Iterator<Item = Result<(IVec, IVec), sled::Error>>>
    } else {
        Box::new(db.range::<&[u8], _>(range)) as Box<dyn Iterator<Item = Result<(IVec, IVec), sled::Error>>>
    };

    for item_result in iterator {
        let (k, _) = item_result?;
        let key_str = String::from_utf8_lossy(&k);

        let parts: Vec<&str> = key_str.splitn(4, ':').collect();
        if parts.len() < 4 { continue; }


        let stored_field_path = parts[1];
        if stored_field_path != field_path { continue; }

        let stored_encoded_hex = parts[2];
        let primary_key = parts[3];

        if let Ok(stored_encoded) = hex::decode(stored_encoded_hex) {
             if let Some(query_type) = value_type_byte {
                 if stored_encoded.is_empty() || stored_encoded[0] != query_type {
                     continue;
                 }
             }

             if let Ok(stored_value) = decode_sorted_value(&stored_encoded) {
                 let comparison_result = compare_values(&stored_value, value);

                 let matches = match operator {
                     ">" => comparison_result == Some(Ordering::Greater),
                     "<" => comparison_result == Some(Ordering::Less),
                     ">=" => comparison_result == Some(Ordering::Greater) || comparison_result == Some(Ordering::Equal),
                     "<=" => comparison_result == Some(Ordering::Less) || comparison_result == Some(Ordering::Equal),
                     "!=" => comparison_result != Some(Ordering::Equal),
                     _ => false,
                 };

                 if matches {
                     current_keys.insert(primary_key.to_string());
                 }
             } else {
                  warn!("Failed to decode sorted value for key: {}", key_str);
             }
        } else {
             warn!("Failed to decode hex for sorted key: {}", key_str);
        }
    }
    Ok(current_keys)
}

fn fetch_documents(db: &Db, keys: HashSet<String>) -> DbResult<Vec<Value>> {
    keys.into_iter()
        .map(|k| get_key(db, &k))
        .collect()
}

#[derive(Clone, Debug, Eq)]
struct HashableValue(Value);

impl PartialEq for HashableValue {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for HashableValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let canonical_string = serde_json::to_string(&self.0).unwrap_or_default();
        canonical_string.hash(state);
    }
}


fn evaluate_condition_on_doc(doc: &Value, field_path: &str, operator: &str, query_value: &Value) -> bool {
     if let Some(doc_value) = get_value_by_path(doc, field_path) {
         match operator {
             "Eq" => doc_value == query_value,
             "Includes" => {
                 if let Some(arr) = doc_value.as_array() {
                     arr.contains(query_value)
                 } else {
                     doc_value == query_value
                 }
             }
             "Gt" | "Lt" | "Gte" | "Lte" | "Ne" => {
                 let comparison_result = compare_values(doc_value, query_value);
                 match operator {
                     "Gt" => comparison_result == Some(Ordering::Greater),
                     "Lt" => comparison_result == Some(Ordering::Less),
                     "Gte" => comparison_result == Some(Ordering::Greater) || comparison_result == Some(Ordering::Equal),
                     "Lte" => comparison_result == Some(Ordering::Less) || comparison_result == Some(Ordering::Equal),
                     "Ne" => comparison_result != Some(Ordering::Equal),
                     _ => false,
                 }
             }
             _ => false,
         }
     } else {
         let parts: Vec<&str> = field_path.split('.').collect();
         if parts.len() > 1 {
             let parent_path = parts[..parts.len()-1].join(".");
             if let Some(Value::Array(arr)) = get_value_by_path(doc, &parent_path) {
                 let last_part = parts.last().unwrap();
                 return arr.iter().any(|elem| {
                     if let Some(nested_val) = elem.get(*last_part) {
                         evaluate_condition_on_doc(nested_val, "", operator, query_value)
                     } else { false }
                 });
             }
         }
         false
     }
}

fn get_all_keys(db: &Db) -> DbResult<HashSet<String>> {
     let mut keys = HashSet::new();
     for result in db.iter() {
         let (key_bytes, _) = result?;
         if !key_bytes.starts_with(GEO_SORTED_INDEX_PREFIX.as_bytes()) &&
            !key_bytes.starts_with(FIELD_INDEX_PREFIX.as_bytes()) &&
            !key_bytes.starts_with(FIELD_SORTED_INDEX_PREFIX.as_bytes()) {
             if let Ok(key_str) = String::from_utf8(key_bytes.to_vec()) {
                 keys.insert(key_str);
             } else {
                 warn!("Found non-UTF8 key in database during get_all_keys");
             }
         }
     }
     Ok(keys)
 }


pub fn execute_ast_query(
    db: &Db,
    query_node: QueryNode,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    offset: Option<usize>,
    config: &DbConfig, // Added config parameter
) -> DbResult<Vec<Value>> {

    let mut results = match query_node {
        QueryNode::Eq(ref field, ref value, _) => { // Borrow field and value
            let keys = fetch_keys_hash_index(db, field, value)?;
            if keys.is_empty() && config.hash_indexed_fields.contains(field) {
                // Fallback for dynamically indexed field with missing entries
                warn!("Index entries missing for dynamically indexed field '{}'. Falling back to full scan.", field);
                let all_keys = get_all_keys(db)?;
                let all_docs = fetch_documents(db, all_keys)?;
                all_docs.into_iter()
                    .filter(|doc| evaluate_condition_on_doc(doc, field, "Eq", value))
                    .collect()
            } else {
                fetch_documents(db, keys)?
            }
        }
        QueryNode::Includes(ref field, ref value, _) => { // Borrow field and value
             let keys = fetch_keys_hash_index(db, field, value)?;
             // Fallback logic similar to Eq could be added here if needed,
             // but Includes often requires post-filtering anyway.
             let docs = fetch_documents(db, keys)?;
             docs.into_iter()
                 .filter(|doc| evaluate_condition_on_doc(doc, field, "Includes", value))
                 .collect()
         }
        QueryNode::Gt(field, value, expected_type) => {
            let keys = fetch_keys_sorted_index(db, &field, ">", &value, &expected_type)?;
            fetch_documents(db, keys)?
        }
        QueryNode::Lt(field, value, expected_type) => {
            let keys = fetch_keys_sorted_index(db, &field, "<", &value, &expected_type)?;
            fetch_documents(db, keys)?
        }
        QueryNode::Gte(field, value, expected_type) => {
            let keys = fetch_keys_sorted_index(db, &field, ">=", &value, &expected_type)?;
            fetch_documents(db, keys)?
        }
        QueryNode::Lte(field, value, expected_type) => {
            let keys = fetch_keys_sorted_index(db, &field, "<=", &value, &expected_type)?;
            fetch_documents(db, keys)?
        }
        QueryNode::Ne(field, value, expected_type) => {
            let keys = fetch_keys_sorted_index(db, &field, "!=", &value, &expected_type)?;
            fetch_documents(db, keys)?
        }
        QueryNode::And(left, right) => {
            let left_results = execute_ast_query(db, *left, None, None, None, config)?; // Pass config
            let right_results = execute_ast_query(db, *right, None, None, None, config)?; // Pass config

            let left_set: HashSet<HashableValue> = left_results.into_iter().map(HashableValue).collect();
            let right_set: HashSet<HashableValue> = right_results.into_iter().map(HashableValue).collect();

            left_set.intersection(&right_set).cloned().map(|hv| hv.0).collect()
        }
         QueryNode::Or(left, right) => {
             let left_results = execute_ast_query(db, *left, None, None, None, config)?; // Pass config
             let right_results = execute_ast_query(db, *right, None, None, None, config)?; // Pass config

             let mut combined_set: HashSet<HashableValue> = left_results.into_iter().map(HashableValue).collect();
             for val in right_results {
                 combined_set.insert(HashableValue(val));
             }

             combined_set.into_iter().map(|hv| hv.0).collect()
         }
         QueryNode::Not(child_node) => {
             // Inefficient NOT implementation: Fetch all, fetch excluded, filter
             let all_docs = get_all_keys(db)?.into_iter()
                 .map(|k| get_key(db, &k))
                 .collect::<DbResult<Vec<Value>>>()?;

             let excluded_docs = execute_ast_query(db, *child_node, None, None, None, config)?; // Pass config
             let excluded_set: HashSet<HashableValue> = excluded_docs.into_iter().map(HashableValue).collect();

             all_docs.into_iter()
                 .filter(|doc| !excluded_set.contains(&HashableValue(doc.clone()))) // Clone needed for check
                 .collect()
         }
         QueryNode::GeoWithinRadius { field, lat, lon, radius } => {
              query_within_radius_simplified(db, &field, lat, lon, radius)?
         }
         QueryNode::GeoInBox { field, min_lat, min_lon, max_lat, max_lon } => {
              query_in_box(db, &field, min_lat, min_lon, max_lat, max_lon)?
         }
    };

    // Apply Pagination
    let start = offset.unwrap_or(0);
    // let _end = start + limit.unwrap_or(usize::MAX); // _end is unused
    if start < results.len() {
         let limit_count = limit.unwrap_or(results.len() - start);
         results = results.into_iter().skip(start).take(limit_count).collect();
    } else {
         results = vec![];
    }


    // Apply Projection
    if let Some(proj_paths) = projection {
        apply_projection(results, &proj_paths)
    } else {
        Ok(results)
    }
}


pub fn export_data(db: &Db) -> DbResult<String> {
    let mut data = Vec::new();
    for result in db.iter() {
        let (key, value) = result?;
        if !key.starts_with(GEO_SORTED_INDEX_PREFIX.as_bytes()) &&
           !key.starts_with(FIELD_INDEX_PREFIX.as_bytes()) &&
           !key.starts_with(FIELD_SORTED_INDEX_PREFIX.as_bytes()) {
            let key_str = String::from_utf8(key.to_vec())?;
            let value_json: Value = serde_json::from_slice(&value)?;
            data.push(json!({ "key": key_str, "value": value_json }));
        }
    }
    Ok(serde_json::to_string(&data)?)
}

pub fn import_data(db: &Db, data: &str, config: &DbConfig) -> DbResult<()> {
    let json_data: Vec<Value> = serde_json::from_str(data)?;
    for item in json_data {
        let key = item.get("key")
            .and_then(Value::as_str)
            .ok_or_else(|| DbError::ImportError("Invalid key format".to_string()))?;
        let value_json = item.get("value")
            .ok_or_else(|| DbError::ImportError("Missing value".to_string()))?;

        set_key(db, key, value_json.clone(), config)?;
    }
    Ok(())
}

fn index_geospatial_field(tx_db: &TransactionalTree, key: &str, field_path: &str, point: &GeoPoint) -> DbResult<()> {
    let coord: Coord<f64> = point.clone().into();
    let hash = encode(coord, GEOHASH_PRECISION).map_err(|e| DbError::Geohash(e.to_string()))?;
    let index_key = get_geo_sorted_index_key(field_path, &hash, key);
    debug!(key=key, field_path=field_path, hash=hash, index_key=%index_key, "Indexing geo field (transactional)");
    tx_db.insert(index_key.as_bytes(), vec![])?;
    debug!(key=key, field_path=field_path, hash=hash, index_key=%index_key, "Successfully inserted geo sorted index (transactional)");
    Ok(())
}

fn remove_geospatial_index(tx_db: &TransactionalTree, key: &str, field_path: &str, point: &GeoPoint) -> DbResult<()> {
    let coord: Coord<f64> = point.clone().into();
    let hash = encode(coord, GEOHASH_PRECISION).map_err(|e| DbError::Geohash(e.to_string()))?;
    let index_key = get_geo_sorted_index_key(field_path, &hash, key);
    debug!(key=key, field_path=field_path, hash=hash, index_key=%index_key, "Removing geo sorted index (transactional)");
    tx_db.remove(index_key.as_bytes())?;
    debug!(key=key, field_path=field_path, hash=hash, index_key=%index_key, "Successfully removed geo sorted index (transactional)");
    Ok(())
}

pub fn query_within_radius_simplified(db: &Db, field_path: &str, center_lat: f64, center_lon: f64, radius_meters: f64) -> DbResult<Vec<Value>> {
    // use geo::prelude::Distance; // Import the trait for .distance() // Removed unused import

    let center_point_geo: Point<f64> = GeoPoint { lat: center_lat, lon: center_lon }.into();
    let center_coord_geo: Coord<f64> = GeoPoint { lat: center_lat, lon: center_lon }.into();
    let center_hash = encode(center_coord_geo, GEOHASH_PRECISION).map_err(|e| DbError::Geohash(e.to_string()))?;

    let neighbors: Neighbors = geohash_neighbors(&center_hash).map_err(|e| DbError::Geohash(e.to_string()))?;
    let mut hashes_to_check = vec![center_hash.clone()];
    hashes_to_check.extend([neighbors.n, neighbors.ne, neighbors.e, neighbors.se, neighbors.s, neighbors.sw, neighbors.w, neighbors.nw]);

    let mut results_map: HashMap<String, Value> = HashMap::new();


    for hash in hashes_to_check {
        let prefix = get_geo_sorted_index_prefix_for_hash(field_path, &hash);
        for item_result in db.scan_prefix(prefix.as_bytes()) {
            let (index_key_bytes, _) = item_result?;
            let index_key_str = String::from_utf8_lossy(&index_key_bytes);
            let parts: Vec<&str> = index_key_str.split(':').collect();

            if parts.len() < 4 {
                 warn!("Invalid geo sorted index key format: {}", index_key_str);
                 continue;
            }
            let stored_field_path = parts[1];
            if stored_field_path != field_path { continue; }

            if let Some(primary_key) = parts.last() {
                 if results_map.contains_key(*primary_key) {
                     continue;
                 }

                 match get_key(db, primary_key) {
                     Ok(value) => {
                         if let Some(point_val) = get_value_by_path(&value, field_path) {
                             if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(point_val.clone()) {
                                 let entry_point: Point<f64> = geo_point.into();

                                 // Use Distance trait method
                                 let distance = entry_point.haversine_distance(&center_point_geo);
                                 if distance <= radius_meters {
                                     results_map.insert(primary_key.to_string(), value);
                                 }

                             } else {
                                 warn!(key = primary_key, field_path = field_path, "Field is not a valid GeoPoint");
                             }
                         } else {
                              warn!(key = primary_key, field_path = field_path, "Geo field not found in document");
                         }
                     },
                     Err(DbError::NotFound) => warn!(key = primary_key, "Geo index points to non-existent key"),
                     Err(e) => return Err(e),
                 }
            } else {
                 warn!("Invalid geo sorted index key format (missing primary key?): {}", index_key_str);
            }
        }
    }
    Ok(results_map.into_values().collect())
}

pub fn query_in_box(db: &Db, field_path: &str, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> DbResult<Vec<Value>> {

    let bounding_box = Rect::new(
        Coord { x: min_lon, y: min_lat },
        Coord { x: max_lon, y: max_lat },
    );
    let prefix = get_geo_sorted_index_prefix_for_field(field_path);
    let mut results_map: HashMap<String, Value> = HashMap::new();

    for item_result in db.scan_prefix(prefix.as_bytes()) {
        let (index_key_bytes, _) = item_result?;
        let index_key_str = String::from_utf8_lossy(&index_key_bytes);
        let parts: Vec<&str> = index_key_str.split(':').collect();

         if parts.len() < 4 {
              warn!("Invalid geo sorted index key format: {}", index_key_str);
              continue;
         }
         let stored_field_path = parts[1];
         if stored_field_path != field_path { continue; }


         if let Some(primary_key) = parts.last() {
             if results_map.contains_key(*primary_key) {
                 continue;
             }

             match get_key(db, primary_key) {
                 Ok(value) => {
                     if let Some(point_val) = get_value_by_path(&value, field_path) {
                         if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(point_val.clone()) {
                             let entry_point: Point<f64> = geo_point.into();
                             if bounding_box.contains(&entry_point) {
                                 results_map.insert(primary_key.to_string(), value);
                             }
                         } else {
                             warn!(key = primary_key, field_path = field_path, "Field is not a valid GeoPoint");
                         }
                     } else {
                          warn!(key = primary_key, field_path = field_path, "Geo field not found in document");
                     }
                 },
                 Err(DbError::NotFound) => warn!(key = primary_key, "Geo index points to non-existent key"),
                 Err(e) => return Err(e),
             }
        } else {
             warn!("Invalid geo sorted index key format (missing primary key?): {}", index_key_str);
        }
    }
    Ok(results_map.into_values().collect())
}

// Simulates deleting a "table" by removing all keys with a given prefix
pub fn clear_prefix(db: &Db, prefix: &str, config: &DbConfig) -> DbResult<usize> {
    let keys_to_delete: Vec<String> = db.scan_prefix(prefix.as_bytes())
        .keys()
        .filter_map(|res| res.ok())
        .filter_map(|key_bytes| String::from_utf8(key_bytes.to_vec()).ok())
        .filter(|key_str| {
            !key_str.starts_with(GEO_SORTED_INDEX_PREFIX) &&
            !key_str.starts_with(FIELD_INDEX_PREFIX) &&
            !key_str.starts_with(FIELD_SORTED_INDEX_PREFIX)
        })
        .collect();

    let count = keys_to_delete.len();

    if count > 0 {
        db.transaction(|tx_db| {
            for key in &keys_to_delete {
                delete_key_internal(tx_db, key, config)
                    .map_err(|e| ConflictableTransactionError::Abort(DbError::TransactionOperationFailed(format!("Clear prefix failed for key '{}': {}", key, e))))?;
            }
            Ok(())
        })?;
    }

    Ok(count)
}

// Clears all user data from the database
pub fn drop_database(db: &Db, config: &DbConfig) -> DbResult<usize> {
    let all_keys = get_all_keys(db)?;
    let count = all_keys.len();

    if count > 0 {
        db.transaction(|tx_db| {
            for key in &all_keys {
                delete_key_internal(tx_db, key, config)
                    .map_err(|e| ConflictableTransactionError::Abort(DbError::TransactionOperationFailed(format!("Drop database failed for key '{}': {}", key, e))))?;
            }
            Ok(())
        })?;
    }

    Ok(count)
}
