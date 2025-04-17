use serde::{Serialize, Deserialize};
use serde_json::{Value, json, Map};
use sled::{Db, transaction::{TransactionError, UnabortableTransactionError, ConflictableTransactionError}};
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tracing::{error, debug, warn};
use geo::{Coord, Point, Rect, prelude::*};
use geohash::encode;
use geo::HaversineDistance;
use std::convert::Infallible;

// Constants
pub const GEO_INDEX_PREFIX: &str = "__geo__";
pub const GEOHASH_PRECISION: usize = 9;
pub const CAS_RETRY_LIMIT: u32 = 10;
pub const DEFAULT_DB_PATH: &str = "database_data_server";
pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:3000";
pub const FIELD_INDEX_PREFIX: &str = "__field_index__";

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
    #[error("Key not found")]
    NotFound,
    #[error("Missing request data: {0}")]
    MissingData(String),
    #[error("Sled transaction error: {0}")]
    Transaction(String),
    #[error("IO Error during operation: {0}")]
    Io(#[from] std::io::Error),
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

// Core CRUD Operations
pub fn set_key(db: &Db, key: &str, value: Value) -> DbResult<()> {
    let serialized_value = serde_json::to_vec(&value)?;
    let key_bytes = key.as_bytes();
    db.transaction(|tx_db| {
        if let Some(old_ivec) = tx_db.get(key_bytes)? {
            if let Ok(old_val) = serde_json::from_slice::<Value>(&old_ivec) {
                if let Value::Object(map) = &old_val {
                    for (field_name, field_value) in map {
                        let index_key = get_field_index_key(field_name, &field_value.to_string());
                        tx_db.remove(index_key.as_bytes())?;
                        if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(field_value.clone()) {
                            if let Err(e) = remove_geospatial_index(tx_db, key, field_name, &geo_point).map_err(ConflictableTransactionError::Abort) {
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }
        tx_db.insert(key_bytes, serialized_value.clone())?;
        if let Value::Object(map) = &value {
            for (field_name, field_value) in map {
                let index_key = get_field_index_key(field_name, &field_value.to_string());
                tx_db.insert(index_key.as_bytes(), key_bytes.to_vec())?;
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
    if let Some(ivec) = db.get(key.as_bytes())? {
        db.transaction(|tx_db| {
            if let Ok(val) = serde_json::from_slice::<Value>(&ivec) {
                if let Value::Object(map) = &val {
                    for (field_name, field_value) in map {
                        let index_key = get_field_index_key(field_name, &field_value.to_string());
                        tx_db.remove(index_key.as_bytes())?;
                        if let Ok(geo_point) = serde_json::from_value::<GeoPoint>(field_value.clone()) {
                            if let Err(e) = remove_geospatial_index(tx_db, key, field_name, &geo_point).map_err(|e| ConflictableTransactionError::Abort(e)) {
                                return Err(e);
                            }
                        }
                    }
                }
            }
            Ok(())
        })?;
        db.remove(key.as_bytes())?;
        db.flush_async().await?;
    }
    Ok(())
}

// Query AND Conditions
pub fn query_and(db: &Db, conditions: Vec<(&str, &str, &str)>) -> DbResult<Vec<Value>> {
    let mut key_sets = Vec::new();
    for (field, operator, value) in &conditions {
        match *operator {
            "===" => {
                let index_key = get_field_index_key(field, value);
                let keys = db.scan_prefix(index_key.as_bytes())
                    .filter_map(|res| res.ok())
                    .filter_map(|(_, v)| String::from_utf8(v.to_vec()).ok())
                    .collect::<HashSet<_>>();
                key_sets.push(keys);
            }
            "includes" => {
                let index_key = get_field_index_key(field, value);
                let keys = db.scan_prefix(index_key.as_bytes())
                    .filter_map(|res| res.ok())
                    .filter_map(|(_, v)| String::from_utf8(v.to_vec()).ok())
                    .collect::<HashSet<_>>();
                key_sets.push(keys);
            }
            _ => return Err(DbError::MissingData(format!("Unsupported operator: {}", operator))),
        }
    }
    let common_keys = key_sets.into_iter()
        .reduce(|a, b| a.intersection(&b).cloned().collect())
        .unwrap_or_default();
    let mut results: DbResult<Vec<Value>> = common_keys.into_iter()
        .filter_map(|k| get_key(db, &k).ok())
        .map(|v| Ok(v))
        .collect();
    Ok(results?)
}

// AST-Based Query Planner
#[derive(Debug, Deserialize)]
pub enum QueryNode {
    Eq(String, String),
    Includes(String, String),
    And(Box<QueryNode>, Box<QueryNode>),
    Or(Box<QueryNode>, Box<QueryNode>),
    Not(Box<QueryNode>),
}

#[derive(Debug)]
pub struct QueryPlan {
    indexes: Vec<String>,
    filters: Vec<DebuggableFilter>,
}

pub fn build_query_plan(db: &Db, query_node: QueryNode) -> QueryPlan {
    match query_node {
        QueryNode::Eq(field, value) => {
            let index_key = get_field_index_key(&field, &value);
            QueryPlan {
                indexes: vec![index_key],
                filters: vec![DebuggableFilter::new(Box::new(move |v| {
                    if let Some(map) = v.as_object() {
                        map.get(&field).map_or(false, |val| {
                            if let Value::String(s) = val {
                                s == &value
                            } else {
                                false
                            }
                        })
                    } else {
                        false
                    }
                }))],
            }
        }
        QueryNode::And(left, right) => {
            let left_plan = build_query_plan(db, *left);
            let right_plan = build_query_plan(db, *right);
            let mut combined = left_plan.indexes;
            combined.extend(right_plan.indexes);
            QueryPlan {
                indexes: combined,
                filters: {
                    let mut combined_filters = left_plan.filters;
                    combined_filters.extend(right_plan.filters);
                    combined_filters
                },
            }
        }
        _ => {
            QueryPlan {
                indexes: vec![],
                filters: vec![],
            }
        }
        // Handle other operators similarly...
    }
}

pub fn execute_query_plan(db: &Db, plan: QueryPlan) -> DbResult<Vec<Value>> {
    let mut candidates = HashSet::new();
    for index in plan.indexes {
        let keys = db.scan_prefix(index.as_bytes())
            .filter_map(|res| res.ok())
            .filter_map(|(_, v)| String::from_utf8(v.to_vec()).ok())
            .collect::<HashSet<_>>();
        candidates = if candidates.is_empty() {
            keys
        } else {
            candidates.intersection(&keys).cloned().collect()
        };
    }
    let mut results: DbResult<Vec<Value>> = candidates.into_iter()
        .filter_map(|k| get_key(db, &k).ok())
        .map(|v| Ok(v))
        .collect();
    Ok(results?)
}

pub fn export_data(db: &Db) -> DbResult<String> {
    let mut data = Vec::new();
    for result in db.iter() {
        let (key, value) = result?;
        if !key.starts_with(GEO_INDEX_PREFIX.as_bytes()) && !key.starts_with(FIELD_INDEX_PREFIX.as_bytes()) {
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