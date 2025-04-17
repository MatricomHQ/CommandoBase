use serde::{Serialize, Deserialize, de::Error as SerdeError};
use serde_json::{Value, json, Map};
use sled::{Db, transaction::{TransactionError, UnabortableTransactionError, ConflictableTransactionError}};
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tracing::{error, debug, warn};
use geo::{Coord, Point, Rect, prelude::*};
use geohash::encode;
use geo::HaversineDistance;
use std::convert::{Infallible, TryInto};
use std::cmp::Ordering;
use hex;
use lazy_static::lazy_static;
use regex::Regex;

// Constants
pub const GEO_INDEX_PREFIX: &str = "__geo__";
pub const GEOHASH_PRECISION: usize = 9;
pub const CAS_RETRY_LIMIT: u32 = 10;
pub const DEFAULT_DB_PATH: &str = "database_data_server";
pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:3000";
pub const FIELD_INDEX_PREFIX: &str = "__field_index__";
pub const FIELD_SORTED_INDEX_PREFIX: &str = "__field_sorted__";

// Error Handling
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

// GeoPoint Definition
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

struct DebuggableFilter(Box<dyn Fn(&Value) -> bool + Send + Sync + 'static>);

impl DebuggableFilter {
    fn new(filter: Box<dyn Fn(&Value) -> bool + Send + Sync + 'static>) -> Self {
        DebuggableFilter(filter)
    }
}

impl std::fmt::Debug for DebuggableFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter")
    }
}

fn get_geo_index_key(field: &str, geohash: &str) -> String {
    format!("{}{}:{}", GEO_INDEX_PREFIX, field, geohash)
}

fn get_field_index_key(field: &str, value: &str) -> String {
    format!("{}{}:{}", FIELD_INDEX_PREFIX, field, value)
}

fn get_field_sorted_index_key(field: &str, encoded_value: &[u8], key: &str) -> String {
    format!("{}{}:{}:{}", FIELD_SORTED_INDEX_PREFIX, field, hex::encode(encoded_value), key)
}

fn get_field_sorted_index_prefix(field: &str) -> String {
    format!("{}{}:", FIELD_SORTED_INDEX_PREFIX, field)
}

fn encode_sorted_value(value: &Value) -> DbResult<Vec<u8>> {
    let mut buf = Vec::new();
    match value {
        Value::Number(num) => {
            if let Some(i) = num.as_i64() {
                buf.push(0x01); // Type byte for i64
                buf.extend_from_slice(&i.to_be_bytes());
            } else if let Some(u) = num.as_u64() {
                // Note: u64 comparison might behave unexpectedly with i64/f64 in compare_values
                buf.push(0x02); // Type byte for u64
                buf.extend_from_slice(&u.to_be_bytes());
            } else if let Some(f) = num.as_f64() {
                buf.push(0x03); // Type byte for f64
                buf.extend_from_slice(&f.to_be_bytes());
            } else {
                return Err(DbError::Serde(serde_json::Error::custom("Unsupported number type")));
            }
        }
        Value::String(s) => {
            buf.push(0x04); // Type byte for string
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Bool(b) => {
            buf.push(0x05); // Type byte for bool
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
        0x01 => { // i64
            if encoded.len() < 9 { return Err(DbError::Serde(serde_json::Error::custom("Invalid i64 encoding length"))); }
            let num = i64::from_be_bytes(encoded[1..9].try_into()?);
            Ok(Value::Number(num.into()))
        }
        0x02 => { // u64
            if encoded.len() < 9 { return Err(DbError::Serde(serde_json::Error::custom("Invalid u64 encoding length"))); }
            let num = u64::from_be_bytes(encoded[1..9].try_into()?);
            Ok(Value::Number(num.into()))
        }
        0x03 => { // f64
            if encoded.len() < 9 { return Err(DbError::Serde(serde_json::Error::custom("Invalid f64 encoding length"))); }
            let num = f64::from_be_bytes(encoded[1..9].try_into()?);
            Ok(Value::Number(serde_json::Number::from_f64(num).ok_or_else(|| DbError::Serde(serde_json::Error::custom("Invalid f64")))?) )
        }
        0x04 => { // String
            let s = String::from_utf8(encoded[1..].to_vec())?;
            Ok(Value::String(s))
        }
        0x05 => { // Bool
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
        // Try parsing as i64 first, then f64
        if let Ok(i) = value_str.parse::<i64>() {
            Ok(Value::Number(i.into()))
        } else if let Ok(f) = value_str.parse::<f64>() {
            Ok(Value::Number(serde_json::Number::from_f64(f).ok_or_else(|| DbError::InvalidComparisonValue(format!("Invalid f64 format: {}", value_str)))?))
        } else {
            Err(DbError::InvalidComparisonValue(format!("Could not parse number: {}", value_str)))
        }
    } else {
        // Treat as a plain string if not boolean or number
        // Remove surrounding quotes if present (common from JSON stringification)
        Ok(Value::String(value_str.trim_matches('"').to_string()))
    }
}

// Helper function to compare serde_json::Value (replaces impl PartialOrd)
fn compare_values(v1: &Value, v2: &Value) -> Option<Ordering> {
    match (v1, v2) {
        (Value::Number(n1), Value::Number(n2)) => {
            // Prioritize f64 comparison for broader compatibility
            if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                f1.partial_cmp(&f2)
            } else {
                // Fallback or more specific integer comparison could be added here if needed
                None
            }
        }
        (Value::String(s1), Value::String(s2)) => s1.partial_cmp(s2),
        (Value::Bool(b1), Value::Bool(b2)) => b1.partial_cmp(b2),
        // Add other type comparisons if needed, otherwise return None
        // Comparing different types is generally not meaningful -> None
        _ => None,
    }
}


// Core CRUD Operations
pub fn set_key(db: &Db, key: &str, value: Value) -> DbResult<()> {
    let serialized_value = serde_json::to_vec(&value)?;
    let key_bytes = key.as_bytes();

    db.transaction(|tx_db| {
        // --- Remove old indexes ---
        if let Some(old_ivec) = tx_db.get(key_bytes)? {
            if let Ok(old_val) = serde_json::from_slice::<Value>(&old_ivec) {
                if let Value::Object(map) = &old_val {
                    for (field_name, field_value) in map {
                        // Remove hash index entries
                        match field_value {
                            Value::Array(arr) => {
                                for elem in arr {
                                    let elem_str = elem.to_string().trim_matches('"').to_string(); // Handle potential quotes
                                    let index_key = get_field_index_key(field_name, &elem_str);
                                    tx_db.remove(index_key.as_bytes())?;
                                }
                            }
                            _ => {
                                let index_key = get_field_index_key(field_name, &field_value.to_string().trim_matches('"'));
                                tx_db.remove(index_key.as_bytes())?;
                            }
                        }

                        // Remove sorted index entry
                        if let Ok(encoded) = encode_sorted_value(field_value) {
                            let sorted_index_key = get_field_sorted_index_key(field_name, &encoded, key);
                            tx_db.remove(sorted_index_key.as_bytes())?;
                        }

                        // Remove geo index entry
                        if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(field_value.clone()) {
                             if let Err(e) = remove_geospatial_index(tx_db, key, field_name, &geo_point).map_err(ConflictableTransactionError::Abort) {
                                 return Err(e);
                             }
                         }
                    }
                }
            }
        }

        // --- Insert new value ---
        tx_db.insert(key_bytes, serialized_value.clone())?;

        // --- Create new indexes ---
        if let Value::Object(map) = &value {
            for (field_name, field_value) in map {
                // Create hash index entries
                match field_value {
                    Value::Array(arr) => {
                        for elem in arr {
                            let elem_str = elem.to_string().trim_matches('"').to_string(); // Handle potential quotes
                            let index_key = get_field_index_key(field_name, &elem_str);
                            tx_db.insert(index_key.as_bytes(), key_bytes.to_vec())?;
                        }
                    }
                    _ => {
                        let index_key = get_field_index_key(field_name, &field_value.to_string().trim_matches('"'));
                        tx_db.insert(index_key.as_bytes(), key_bytes.to_vec())?;
                    }
                }

                // Create sorted index entry
                if let Ok(encoded) = encode_sorted_value(field_value) {
                    let sorted_index_key = get_field_sorted_index_key(field_name, &encoded, key);
                    tx_db.insert(sorted_index_key.as_bytes(), vec![])?; // Store empty value for sorted index
                }

                // Create geo index entry
                if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(field_value.clone()) {
                     if let Err(e) = index_geospatial_field(tx_db, key, field_name, &geo_point).map_err(ConflictableTransactionError::Abort) {
                         return Err(e);
                     }
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

pub async fn delete_key(db: &Db, key: &str) -> DbResult<()> {
    let key_bytes = key.as_bytes();
    if let Some(ivec) = db.get(key_bytes)? {
        db.transaction(|tx_db| {
            if let Ok(val) = serde_json::from_slice::<Value>(&ivec) {
                if let Value::Object(map) = &val {
                    for (field_name, field_value) in map {
                        // Remove hash index entries
                        match field_value {
                            Value::Array(arr) => {
                                for elem in arr {
                                    let elem_str = elem.to_string().trim_matches('"').to_string();
                                    let index_key = get_field_index_key(field_name, &elem_str);
                                    tx_db.remove(index_key.as_bytes())?;
                                }
                            }
                            _ => {
                                let index_key = get_field_index_key(field_name, &field_value.to_string().trim_matches('"'));
                                tx_db.remove(index_key.as_bytes())?;
                            }
                        }

                        // Remove sorted index entry
                        if let Ok(encoded) = encode_sorted_value(field_value) {
                            let sorted_index_key = get_field_sorted_index_key(field_name, &encoded, key);
                            tx_db.remove(sorted_index_key.as_bytes())?;
                        }

                        // Remove geo index entry
                        if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(field_value.clone()) {
                            if let Err(e) = remove_geospatial_index(tx_db, key, field_name, &geo_point).map_err(|e| ConflictableTransactionError::Abort(e)) {
                                return Err(e);
                            }
                        }
                    }
                }
            }
            // Remove the main key entry
            tx_db.remove(key_bytes)?;
            Ok(())
        })?;
        db.flush_async().await?;
    } else {
         // Key doesn't exist, nothing to delete
         return Ok(());
    }
    Ok(())
}


// Query AND Conditions
pub fn query_and(db: &Db, conditions: Vec<(&str, &str, &str)>) -> DbResult<Vec<Value>> {
    let mut key_sets: Vec<HashSet<String>> = Vec::new();

    for (field, operator, value_str) in &conditions {
        let mut current_keys = HashSet::new();
        match *operator {
            "===" | "includes" => {
                // Use hash index for exact match and includes (now works for array elements)
                let value_parsed = parse_value(value_str)?; // Parse to handle quotes correctly if needed
                let index_key = get_field_index_key(field, &value_parsed.to_string().trim_matches('"'));
                current_keys = db.scan_prefix(index_key.as_bytes())
                    .filter_map(|res| res.ok())
                    .filter_map(|(_, v)| String::from_utf8(v.to_vec()).ok())
                    .collect::<HashSet<_>>();
            }
            ">" | "<" | ">=" | "<=" | "!=" => {
                // Use sorted index for range and inequality queries
                let value = parse_value(value_str)?; // Parse the query value string
                let prefix = get_field_sorted_index_prefix(field);

                for item_result in db.scan_prefix(prefix.as_bytes()) {
                    let (k, _) = item_result?;
                    let key_str = String::from_utf8_lossy(&k);
                    // Expected format: __field_sorted__{field}:{hex_encoded_value}:{primary_key}
                    let parts: Vec<&str> = key_str.splitn(4, ':').collect(); // Split by ':'
                    if parts.len() < 4 { continue; } // Need prefix, field, encoded_val, primary_key

                    let stored_encoded_hex = parts[2];
                    let primary_key = parts[3];

                    if let Ok(stored_encoded) = hex::decode(stored_encoded_hex) {
                         if let Ok(stored_value) = decode_sorted_value(&stored_encoded) {
                             // Use the compare_values helper function
                             let comparison_result = compare_values(&stored_value, &value);

                             // Check if the comparison matches the operator
                             let matches = match *operator { // Match on the operator string directly
                                 ">" => comparison_result == Some(Ordering::Greater),
                                 "<" => comparison_result == Some(Ordering::Less),
                                 ">=" => comparison_result == Some(Ordering::Greater) || comparison_result == Some(Ordering::Equal),
                                 "<=" => comparison_result == Some(Ordering::Less) || comparison_result == Some(Ordering::Equal),
                                 "!=" => comparison_result != Some(Ordering::Equal),
                                 _ => false, // Should not happen based on outer match
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
            }
            _ => return Err(DbError::MissingData(format!("Unsupported operator: {}", operator))),
        }
        key_sets.push(current_keys);
    }

    // Find intersection of all key sets
    let common_keys = key_sets.into_iter()
        .fold(None::<HashSet<String>>, |acc, set| match acc {
            None => Some(set),
            Some(a) => Some(a.intersection(&set).cloned().collect()),
        })
        .unwrap_or_default();


    // Fetch corresponding values
    let results: DbResult<Vec<Value>> = common_keys.into_iter()
        .map(|k| get_key(db, &k)) // Fetch the full value
        .collect(); // Collect into DbResult<Vec<Value>>

    results
}


// AST-Based Query Planner
#[derive(Debug, Deserialize)]
pub enum QueryNode {
    Eq(String, Value),      // Field, Value
    Includes(String, Value), // Field, Value
    Gt(String, Value),       // Field, Value
    Lt(String, Value),       // Field, Value
    Gte(String, Value),      // Field, Value
    Lte(String, Value),      // Field, Value
    Ne(String, Value),       // Field, Value (Not Equal)
    And(Box<QueryNode>, Box<QueryNode>),
    Or(Box<QueryNode>, Box<QueryNode>),
    Not(Box<QueryNode>),
}

#[derive(Debug)]
pub struct QueryPlan {
    indexes: Vec<String>, // Can be hash index keys or sorted index prefixes
    filters: Vec<DebuggableFilter>,
}

// Need a helper to clone QueryNode for filter closures
fn clone_query_node_for_filter(node: &QueryNode) -> Option<(String, Value, &'static str)> {
    match node {
        QueryNode::Gt(f, v) => Some((f.clone(), v.clone(), ">")),
        QueryNode::Lt(f, v) => Some((f.clone(), v.clone(), "<")),
        QueryNode::Gte(f, v) => Some((f.clone(), v.clone(), ">=")),
        QueryNode::Lte(f, v) => Some((f.clone(), v.clone(), "<=")),
        QueryNode::Ne(f, v) => Some((f.clone(), v.clone(), "!=")),
        _ => None,
    }
}


pub fn build_query_plan(db: &Db, query_node: QueryNode) -> DbResult<QueryPlan> {
    match query_node {
        QueryNode::Eq(field, value) => {
            let index_key = get_field_index_key(&field, &value.to_string().trim_matches('"'));
            Ok(QueryPlan {
                indexes: vec![index_key], // Use hash index
                filters: vec![DebuggableFilter::new(Box::new(move |v| {
                    v.get(&field).map_or(false, |val| val == &value) // Simple equality check
                }))],
            })
        }
        QueryNode::Includes(field, value) => {
             let index_key = get_field_index_key(&field, &value.to_string().trim_matches('"'));
             Ok(QueryPlan {
                 indexes: vec![index_key], // Use hash index (points to keys containing the element)
                 filters: vec![DebuggableFilter::new(Box::new(move |v| {
                     v.get(&field).map_or(false, |val| { // Post-filter to confirm array contains value
                         if let Value::Array(arr) = val {
                             arr.contains(&value)
                         } else {
                             false // Field exists but is not an array
                         }
                     })
                 }))],
             })
         }
        QueryNode::Gt(_, _) | QueryNode::Lt(_, _) | QueryNode::Gte(_, _) | QueryNode::Lte(_, _) | QueryNode::Ne(_, _) => {
            // Clone necessary parts for the closure
            if let Some((field, value, operator)) = clone_query_node_for_filter(&query_node) {
                let prefix = get_field_sorted_index_prefix(&field);
                Ok(QueryPlan {
                    indexes: vec![prefix], // Use sorted index prefix
                    filters: vec![DebuggableFilter::new(Box::new(move |v| {
                        v.get(&field).map_or(false, |val| {
                            let comparison = compare_values(val, &value); // Use helper
                            match operator { // Use the captured operator string
                                ">" => comparison == Some(Ordering::Greater),
                                "<" => comparison == Some(Ordering::Less),
                                ">=" => comparison == Some(Ordering::Greater) || comparison == Some(Ordering::Equal),
                                "<=" => comparison == Some(Ordering::Less) || comparison == Some(Ordering::Equal),
                                "!=" => comparison != Some(Ordering::Equal),
                                _ => false,
                            }
                        })
                    }))],
                })
            } else {
                 // Should not happen if called from the outer match arms
                 Err(DbError::MissingData("Internal error cloning query node".to_string()))
            }
        }
        QueryNode::And(left, right) => {
            let left_plan = build_query_plan(db, *left)?;
            let right_plan = build_query_plan(db, *right)?;
            let mut combined_indexes = left_plan.indexes;
            combined_indexes.extend(right_plan.indexes);
            let mut combined_filters = left_plan.filters;
            combined_filters.extend(right_plan.filters);
            Ok(QueryPlan {
                indexes: combined_indexes, // Combine indexes (might need smarter combination later)
                filters: combined_filters,
            })
        }
         QueryNode::Or(_, _) => {
             // OR is more complex. Simplest approach: execute both sub-plans and union results.
             // More advanced: cost-based analysis to pick the best plan or index intersection/union.
             // For now, let's return an error or a basic plan that requires full scan + filtering.
             error!("OR queries in AST are not fully implemented yet.");
             Err(DbError::MissingData("OR queries in AST not fully implemented".to_string()))
         }
         QueryNode::Not(_) => {
             // NOT is also complex. Often requires scanning and filtering.
             error!("NOT queries in AST are not fully implemented yet.");
             Err(DbError::MissingData("NOT queries in AST not fully implemented".to_string()))
         }
    }
}

pub fn execute_query_plan(db: &Db, plan: QueryPlan) -> DbResult<Vec<Value>> {
    let mut candidate_keys = HashSet::new();
    let mut first_index = true;

    for index_or_prefix in plan.indexes {
        let mut current_keys = HashSet::new();
        // Check if it's a hash index key or a sorted index prefix
        if index_or_prefix.starts_with(FIELD_INDEX_PREFIX) {
            // Hash index lookup (points to a single primary key)
             if let Some(key_bytes) = db.get(&index_or_prefix)? {
                 if let Ok(key_str) = String::from_utf8(key_bytes.to_vec()) {
                     current_keys.insert(key_str);
                 }
             }
        } else if index_or_prefix.starts_with(FIELD_SORTED_INDEX_PREFIX) {
            // Sorted index scan (prefix scan yields multiple index entries)
            for item_result in db.scan_prefix(index_or_prefix.as_bytes()) {
                 let (k, _) = item_result?;
                 let key_str = String::from_utf8_lossy(&k);
                 // Format: __field_sorted__{field}:{hex_encoded_value}:{primary_key}
                 let parts: Vec<&str> = key_str.split(':').collect();
                 if let Some(primary_key) = parts.last() { // Primary key is the last part
                     current_keys.insert(primary_key.to_string());
                 }
            }
        } else {
             warn!("Unknown index/prefix type in query plan: {}", index_or_prefix);
             // Potentially treat as full scan trigger if needed
        }


        if first_index {
            candidate_keys = current_keys;
            first_index = false;
        } else {
            // Intersect keys for AND logic (implicit in current plan structure)
            candidate_keys = candidate_keys.intersection(&current_keys).cloned().collect();
        }
    }

    // Fetch and filter results
    let mut final_results = Vec::new();
    for key in candidate_keys {
        if let Ok(value) = get_key(db, &key) {
            let mut passes_filters = true;
            for filter in &plan.filters {
                if !(filter.0)(&value) { // Apply the filter closure
                    passes_filters = false;
                    break;
                }
            }
            if passes_filters {
                final_results.push(value);
            }
        } else {
             warn!("Key {} found in index but not in main store.", key);
        }
    }

    Ok(final_results)
}


pub fn export_data(db: &Db) -> DbResult<String> {
    let mut data = Vec::new();
    for result in db.iter() {
        let (key, value) = result?;
        // Exclude all index entries
        if !key.starts_with(GEO_INDEX_PREFIX.as_bytes()) &&
           !key.starts_with(FIELD_INDEX_PREFIX.as_bytes()) &&
           !key.starts_with(FIELD_SORTED_INDEX_PREFIX.as_bytes()) {
            let key_str = String::from_utf8(key.to_vec())?;
            let value_json: Value = serde_json::from_slice(&value)?;
            data.push(json!({ "key": key_str, "value": value_json }));
        }
    }
    Ok(serde_json::to_string(&data)?)
}

pub fn import_data(db: &Db, data: &str) -> DbResult<()> {
    let json_data: Vec<Value> = serde_json::from_str(data)?;
    for item in json_data {
        let key = item.get("key")
            .and_then(Value::as_str)
            .ok_or_else(|| DbError::ImportError("Invalid key format".to_string()))?;
        let value_json = item.get("value")
            .ok_or_else(|| DbError::ImportError("Missing value".to_string()))?;
        // Use set_key to ensure indexes are rebuilt during import
        set_key(db, key, value_json.clone())?;
    }
    Ok(())
}

fn index_geospatial_field(tx_db: &sled::transaction::TransactionalTree, key: &str, field: &str, point: &GeoPoint) -> DbResult<()> {
    let coord: Coord<f64> = point.clone().into();
    let hash = encode(coord, GEOHASH_PRECISION).map_err(|e| DbError::Geohash(e.to_string()))?;
    debug!(key=key, field=field, hash=hash, "Indexing geo field (transactional)");
    let geo_entry = GeoEntry {
        key: key.to_string(),
        geohash: hash.clone(),
        point: point.clone(),
    };
    let index_key = get_geo_index_key(field, &hash);
    let index_key_bytes = index_key.as_bytes();
    let current_value_opt = tx_db.get(index_key_bytes)?;
    let mut entries = current_value_opt
        .as_ref()
        .and_then(|ivec| serde_json::from_slice::<Vec<GeoEntry>>(ivec).ok())
        .unwrap_or_default();
    if !entries.iter().any(|entry| entry.key == key) {
        entries.push(geo_entry.clone());
    } else {
        debug!(key=key, field=field, hash=hash, "Key already exists in geo index, skipping update (transactional)");
        return Ok(());
    }
    let serialized_entries = serde_json::to_vec(&entries)?;
    tx_db.insert(index_key_bytes, serialized_entries)?;
    debug!(key=key, field=field, hash=hash, "Successfully updated geo index (transactional)");
    Ok(())
}

fn remove_geospatial_index(tx_db: &sled::transaction::TransactionalTree, key: &str, field: &str, point: &GeoPoint) -> DbResult<()> {
    let coord: Coord<f64> = point.clone().into();
    let hash = encode(coord, GEOHASH_PRECISION).map_err(|e| DbError::Geohash(e.to_string()))?;
    let index_key = get_geo_index_key(field, &hash);
    let index_key_bytes = index_key.as_bytes();
    debug!(key=key, field=field, hash=hash, "Removing geo index (transactional)");
    let current_value_opt = tx_db.get(index_key_bytes)?;
    if let Some(ivec) = current_value_opt {
        if let Ok(mut entries) = serde_json::from_slice::<Vec<GeoEntry>>(&ivec) {
            let initial_len = entries.len();
            entries.retain(|entry| entry.key != key);
            if entries.len() < initial_len {
                if entries.is_empty() {
                    tx_db.remove(index_key_bytes)?;
                    debug!(key=key, field=field, hash=hash, "Successfully removed entry and index key from geo index (transactional)");
                } else {
                    let next_value_bytes = serde_json::to_vec(&entries)?;
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

// Geospatial Queries
pub fn query_within_radius_simplified(db: &Db, field: &str, center_lat: f64, center_lon: f64, radius_meters: f64) -> DbResult<Vec<Value>> {
    let center_point_geo: Point<f64> = GeoPoint { lat: center_lat, lon: center_lon }.into();
    let center_coord_geo: Coord<f64> = GeoPoint { lat: center_lat, lon: center_lon }.into();
    // Note: This simplified version only checks the geohash bucket of the center point.
    // A full implementation would check neighboring geohashes as well.
    let center_hash = encode(center_coord_geo, GEOHASH_PRECISION).map_err(|e| DbError::Geohash(e.to_string()))?;
    let mut results_map: HashMap<String, Value> = HashMap::new();
    let index_key = get_geo_index_key(field, &center_hash);
    if let Some(ivec) = db.get(&index_key)? {
        if let Ok(entries) = serde_json::from_slice::<Vec<GeoEntry>>(&ivec) {
            for entry in entries {
                let entry_point: Point<f64> = entry.point.into();
                let distance = entry_point.haversine_distance(&center_point_geo);
                if distance <= radius_meters {
                    if !results_map.contains_key(&entry.key) {
                        match get_key(db, &entry.key) {
                            Ok(value) => { results_map.insert(entry.key.clone(), value); },
                            Err(DbError::NotFound) => warn!(key=entry.key, "Geo index points to non-existent key"),
                            Err(e) => return Err(e),
                        }
                    }
                }
            }
        }
    }
    Ok(results_map.into_values().collect())
}

pub fn query_in_box(db: &Db, field: &str, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> DbResult<Vec<Value>> {
    let bounding_box = Rect::new(
        Coord { x: min_lon, y: min_lat },
        Coord { x: max_lon, y: max_lat },
    );
    let prefix = format!("{}{}:", GEO_INDEX_PREFIX, field);
    let mut results_map: HashMap<String, Value> = HashMap::new();
    // Note: This scans ALL geo index entries for the field.
    // A more optimized version would use geohash prefixes to narrow the scan.
    for item_result in db.scan_prefix(prefix.as_bytes()) {
        let (_key, value_ivec) = item_result?;
        if let Ok(entries) = serde_json::from_slice::<Vec<GeoEntry>>(&value_ivec) {
            for entry in entries {
                let entry_point: Point<f64> = entry.point.into();
                if bounding_box.contains(&entry_point) {
                    if !results_map.contains_key(&entry.key) {
                        match get_key(db, &entry.key) {
                            Ok(value) => { results_map.insert(entry.key.clone(), value); },
                            Err(DbError::NotFound) => warn!(key=entry.key, "Geo index points to non-existent key"),
                            Err(e) => return Err(e),
                        }
                    }
                }
            }
        }
    }
    Ok(results_map.into_values().collect())
}