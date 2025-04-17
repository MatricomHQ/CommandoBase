use axum::{
    routing::{get, post},
    Router,
    response::{IntoResponse, Response, Json},
    http::{StatusCode, Request, header::{HeaderName, HeaderValue}}, // Corrected header import
    extract::{State, FromRequestParts},
    middleware::{self, Next},
    body::Body, // Import Body
};
use rust_db_logic::{
    self as logic,
    export_data,
    DbConfig as LogicDbConfig,
    BatchSetItem,
    TransactionOperation,
    QueryNode,
};
use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use sled::{Db, Config};
use std::sync::Arc;
use std::path::PathBuf;
use std::fs;
use std::env;
use tokio::net::TcpListener;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, error, warn, Level, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use clap::Parser;
use thiserror::Error;
use std::sync::Mutex;
use rand::{distributions::Alphanumeric, Rng};

const DEFAULT_BASE_PATH: &str = "database_data_server";
const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:8989";
const API_KEY_HEADER: &str = "X-API-Key";
const API_KEY_HEADER_LOWERCASE: &str = "x-api-key"; // Lowercase version

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, env = "DB_PATH", value_name = "DIR", default_value = DEFAULT_BASE_PATH)]
    base_path: PathBuf,
    #[arg(short, long, env = "DB_NAME", value_name = "NAME")]
    db_name: String,
    #[arg(short, long, env = "LISTEN_ADDR", value_name = "HOST:PORT", default_value = DEFAULT_LISTEN_ADDR)]
    listen_addr: String,
    #[arg(long, env = "DB_API_KEY")] // Reads from --api-key OR DB_API_KEY env var
    api_key: Option<String>,
}

#[derive(Clone, Debug)]
struct AppState {
    db: Arc<Db>,
    db_config: Arc<Mutex<LogicDbConfig>>,
    api_key: Arc<String>,
}

#[derive(Deserialize, Debug)]
struct KeyPayload {
    key: String,
}

#[derive(Deserialize, Debug)]
struct SetPayload {
    key: String,
    value: Value,
}

#[derive(Deserialize, Debug)]
struct GetPartialPayload {
    key: String,
    fields: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct QueryRadiusPayload {
    field: String,
    lat: f64,
    lon: f64,
    radius: f64,
}

#[derive(Deserialize, Debug)]
struct QueryBoxPayload {
    field: String,
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
}

#[derive(Deserialize, Debug)]
struct QueryAndPayload {
    conditions: Vec<(String, String, String)>,
}

#[derive(Deserialize, Debug)]
struct QueryAstPayload {
    ast: logic::QueryNode,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ImportItem {
    key: String,
    value: Value,
}

type ImportPayload = Vec<ImportItem>;
type BatchSetPayload = Vec<BatchSetItem>;
type TransactionPayload = Vec<TransactionOperation>;

#[derive(Deserialize, Debug)]
struct ClearPrefixPayload {
    prefix: String,
}

#[derive(Serialize)]
struct CountResponse {
    count: usize,
}

fn extract_eq_field(query_node: &QueryNode) -> Option<String> {
    match query_node {
        QueryNode::Eq(field, _, _) => Some(field.clone()),
        QueryNode::And(left, right) => extract_eq_field(left).or_else(|| extract_eq_field(right)),
        QueryNode::Or(left, right) => extract_eq_field(left).or_else(|| extract_eq_field(right)),
        QueryNode::Not(node) => extract_eq_field(node),
        _ => None,
    }
}

fn add_field_to_index(db_config: &mut LogicDbConfig, field_path: &str) {
    let mut current_path = String::new();
    for part in field_path.split('.') {
        if !current_path.is_empty() {
            current_path.push('.');
        }
        current_path.push_str(part);
        if db_config.hash_indexed_fields.insert(current_path.clone()) {
            info!("Dynamically indexing field: {}", current_path);
        }
    }
}

// Corrected middleware signature
async fn api_key_auth(
    State(state): State<AppState>,
    req: Request<Body>, // Use axum::body::Body
    next: Next, // Remove generic parameter
) -> Result<Response, AppError> {
    let headers = req.headers();
    // Use HeaderName::from_static for efficiency
    let api_key_header_name = HeaderName::from_static(API_KEY_HEADER_LOWERCASE);

    if let Some(provided_key) = headers.get(&api_key_header_name).and_then(|value| value.to_str().ok()) {
        if provided_key == state.api_key.as_str() {
            Ok(next.run(req).await) // Pass the original req
        } else {
            warn!("Invalid API Key provided");
            Err(AppError::Unauthorized)
        }
    } else {
        warn!("Missing API Key header: {}", API_KEY_HEADER);
        Err(AppError::Unauthorized)
    }
}


#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            "rust_db_server=info,tower_http=warn".into()
        }))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let api_key = match args.api_key.or_else(|| env::var("DB_API_KEY").ok()) {
        Some(key) => {
            if key.is_empty() {
                 error!("Provided API Key (via --api-key or DB_API_KEY) cannot be empty.");
                 std::process::exit(1);
            }
            info!("Using provided API Key.");
            key
        }
        None => {
            let generated_key: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(32)
                .map(char::from)
                .collect();
            warn!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            warn!("!!! WARNING: No API Key provided via --api-key or DB_API_KEY environment variable.");
            warn!("!!! Generating a random API Key for this session:");
            warn!("!!! {}", generated_key);
            warn!("!!! Use this key in the '{}' header for requests.", API_KEY_HEADER);
            warn!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            generated_key
        }
    };

    info!("Ensuring base directory exists at {:?}", args.base_path);
    if let Err(e) = fs::create_dir_all(&args.base_path) {
        error!("Failed to create base directory at {:?}: {}", args.base_path, e);
        std::process::exit(1);
    }

    let db_dir = args.base_path.join(&args.db_name);
    info!("Opening database {:?} at path: {:?} with compression enabled", args.db_name, db_dir);
    let db_result = Config::default()
        .path(&db_dir)
        .use_compression(true)
        .open();

    let db = match db_result {
        Ok(db) => Arc::new(db),
        Err(e) => {
            let logic_error = logic::DbError::from(e);
            let app_error = AppError::from(logic_error);
            error!("Failed to open database {:?}: {}", db_dir, app_error);
            std::process::exit(1);
        }
    };

    let db_config = Arc::new(Mutex::new(LogicDbConfig::default()));
    info!("Using default DbConfig: {:?}", db_config);

    let app_state = AppState {
        db,
        db_config,
        api_key: Arc::new(api_key),
    };

    let api_routes = Router::new()
        .route("/set", post(set_handler))
        .route("/get", post(get_handler))
        .route("/get_partial", post(get_partial_handler))
        .route("/delete", post(delete_handler))
        .route("/batch_set", post(batch_set_handler))
        .route("/transaction", post(transaction_handler))
        .route("/clear_prefix", post(clear_prefix_handler))
        .route("/drop_database", post(drop_database_handler))
        .route("/query/radius", post(query_radius_handler))
        .route("/query/box", post(query_box_handler))
        .route("/query/and", post(query_and_handler))
        .route("/query/ast", post(query_ast_handler))
        .route("/export", get(export_handler))
        .route("/import", post(import_handler))
        .route_layer(middleware::from_fn_with_state(app_state.clone(), api_key_auth));

    let app = Router::new()
        .route("/", get(health_check)) // Health check doesn't need auth
        .merge(api_routes)
        .with_state(app_state.clone())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(tower_http::trace::DefaultOnResponse::new().level(Level::INFO).latency_unit(tower_http::LatencyUnit::Micros)),
        )
        .layer(CorsLayer::permissive()); // Consider making CORS more restrictive

    info!("Attempting to bind listener to {}", args.listen_addr);
    let listener = match TcpListener::bind(&args.listen_addr).await {
        Ok(l) => {
            info!("Successfully bound listener to {}", args.listen_addr);
            l
        },
        Err(e) => {
            error!("Failed to bind listener to address {}: {}", args.listen_addr, e);
            std::process::exit(1);
        }
    };

    info!("Starting Axum server loop...");
    if let Err(e) = axum::serve(listener, app.into_make_service()).await {
        error!("Server error: {}", e);
        std::process::exit(1);
    }
}

#[instrument(skip(state), fields(handler="health_check"))]
async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    info!("Entering health_check handler");
    match state.db.size_on_disk() {
        Ok(size) => info!(db_size = size, "Health check OK"),
        Err(e) => error!("Health check failed to get DB size: {}", e),
    }
    (StatusCode::OK, "Server is running")
}

#[instrument(skip(state, payload), fields(handler="set_handler"))]
async fn set_handler(
    State(state): State<AppState>,
    Json(payload): Json<SetPayload>,
) -> Result<StatusCode, AppError> {
    let db_config_guard = state.db_config.lock().unwrap();
    logic::set_key(&state.db, &payload.key, payload.value, &db_config_guard)?;
    Ok(StatusCode::OK)
}

#[instrument(skip(state, payload), fields(handler="get_handler"))]
async fn get_handler(
    State(state): State<AppState>,
    Json(payload): Json<KeyPayload>,
) -> Result<Json<Value>, AppError> {
    let value = logic::get_key(&state.db, &payload.key)?;
    Ok(Json(value))
}

#[instrument(skip(state, payload), fields(handler="get_partial_handler"))]
async fn get_partial_handler(
    State(state): State<AppState>,
    Json(payload): Json<GetPartialPayload>,
) -> Result<Json<Value>, AppError> {
    let value = logic::get_partial_key(&state.db, &payload.key, &payload.fields)?;
    Ok(Json(value))
}

#[instrument(skip(state, payload), fields(handler="delete_handler"))]
async fn delete_handler(
    State(state): State<AppState>,
    Json(payload): Json<KeyPayload>,
) -> Result<StatusCode, AppError> {
    let config_clone = {
        let guard = state.db_config.lock().unwrap();
        let config_clone = guard.clone();
        drop(guard);
        config_clone
    };
    logic::delete_key(&state.db, &payload.key, &config_clone).await?;
    Ok(StatusCode::OK)
}

#[instrument(skip(state, payload), fields(handler="batch_set_handler"))]
async fn batch_set_handler(
    State(state): State<AppState>,
    Json(payload): Json<BatchSetPayload>,
) -> Result<StatusCode, AppError> {
    let db_config_guard = state.db_config.lock().unwrap();
    logic::batch_set(&state.db, &payload, &db_config_guard)?;
    Ok(StatusCode::OK)
}

#[instrument(skip(state, payload), fields(handler="transaction_handler"))]
async fn transaction_handler(
    State(state): State<AppState>,
    Json(payload): Json<TransactionPayload>,
) -> Result<StatusCode, AppError> {
    let db_config_guard = state.db_config.lock().unwrap();
    logic::execute_transaction(&state.db, &payload, &db_config_guard)?;
    Ok(StatusCode::OK)
}

#[instrument(skip(state, payload), fields(handler="clear_prefix_handler"))]
async fn clear_prefix_handler(
    State(state): State<AppState>,
    Json(payload): Json<ClearPrefixPayload>,
) -> Result<Json<CountResponse>, AppError> {
    let db_config_guard = state.db_config.lock().unwrap();
    let count = logic::clear_prefix(&state.db, &payload.prefix, &db_config_guard)?;
    Ok(Json(CountResponse { count }))
}

#[instrument(skip(state), fields(handler="drop_database_handler"))]
async fn drop_database_handler(
    State(state): State<AppState>,
) -> Result<Json<CountResponse>, AppError> {
    let db_config_guard = state.db_config.lock().unwrap();
    let count = logic::drop_database(&state.db, &db_config_guard)?;
    Ok(Json(CountResponse { count }))
}

#[instrument(skip(state, payload), fields(handler="query_radius_handler"))]
async fn query_radius_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryRadiusPayload>,
) -> Result<Json<Vec<Value>>, AppError> {
    let results = logic::query_within_radius_simplified(&state.db, &payload.field, payload.lat, payload.lon, payload.radius)?;
    Ok(Json(results))
}

#[instrument(skip(state, payload), fields(handler="query_box_handler"))]
async fn query_box_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryBoxPayload>,
) -> Result<Json<Vec<Value>>, AppError> {
    let results = logic::query_in_box(&state.db, &payload.field, payload.min_lat, payload.min_lon, payload.max_lat, payload.max_lon)?;
    Ok(Json(results))
}

#[instrument(skip(state, payload), fields(handler="query_and_handler"))]
async fn query_and_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryAndPayload>,
) -> Result<Json<Vec<Value>>, AppError> {
    let conditions: Vec<(&str, &str, &str)> = payload.conditions.iter()
        .map(|(field, op, value)| (field.as_str(), op.as_str(), value.as_str()))
        .collect();
    let results = logic::query_and(&state.db, conditions)?;
    Ok(Json(results))
}

#[instrument(skip(state, payload), fields(handler="query_ast_handler"))]
async fn query_ast_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryAstPayload>,
) -> Result<Json<Vec<Value>>, AppError> {
    let field_to_index = &payload.ast;
    let field_option = extract_eq_field(field_to_index);

    let config_clone = {
        let mut db_config_guard = state.db_config.lock().unwrap();
        if let Some(field) = field_option {
            add_field_to_index(&mut db_config_guard, &field);
        }
        let config_clone = db_config_guard.clone();
        drop(db_config_guard);
        config_clone
    };

    let results = logic::execute_ast_query(&state.db, payload.ast, payload.projection, payload.limit, payload.offset, &config_clone)?;
    Ok(Json(results))
}

#[instrument(skip(state), fields(handler="export_handler"))]
async fn export_handler(
    State(state): State<AppState>,
) -> Result<Json<String>, AppError> {
    let data_string = export_data(&state.db)?;
    Ok(Json(data_string))
}

#[instrument(skip(state, payload), fields(handler="import_handler"))]
async fn import_handler(
    State(state): State<AppState>,
    Json(payload): Json<ImportPayload>,
) -> Result<StatusCode, AppError> {
    let db_config_guard = state.db_config.lock().unwrap();
    logic::import_data(&state.db, &serde_json::to_string(&payload).unwrap(), &db_config_guard)?;
    Ok(StatusCode::CREATED)
}

#[derive(Error, Debug)]
enum AppError {
    #[error(transparent)]
    Logic(#[from] logic::DbError),
    #[error("JSON Error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Unauthorized: Missing or invalid API key")]
    Unauthorized,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match &self {
            AppError::Logic(logic_err) => match logic_err {
                logic::DbError::Sled(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Database internal error".to_string()),
                logic::DbError::Serde(_) => (StatusCode::BAD_REQUEST, "Invalid data format in logic".to_string()),
                logic::DbError::Geohash(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Geospatial processing error".to_string()),
                logic::DbError::ImportError(msg) => (StatusCode::BAD_REQUEST, format!("Import failed: {}", msg)),
                logic::DbError::CasRetryLimit(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Database concurrency error".to_string()),
                logic::DbError::Utf8Error(_) => (StatusCode::BAD_REQUEST, "Invalid UTF-8 data".to_string()),
                logic::DbError::HexError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal encoding error".to_string()),
                logic::DbError::TryFromSlice(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal byte conversion error".to_string()),
                logic::DbError::NotFound => (StatusCode::NOT_FOUND, "Key not found".to_string()),
                logic::DbError::MissingData(field) => (StatusCode::BAD_REQUEST, format!("Missing or invalid data: {}", field)),
                logic::DbError::Transaction(msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Transaction error: {}", msg)),
                logic::DbError::Io(io_err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("IO error: {}", io_err)),
                logic::DbError::InvalidComparisonValue(msg) => (StatusCode::BAD_REQUEST, format!("Invalid value for comparison: {}", msg)),
                logic::DbError::NotAnObject => (StatusCode::BAD_REQUEST, "Value is not an object, cannot retrieve partial fields".to_string()),
                logic::DbError::FieldNotFound(field) => (StatusCode::BAD_REQUEST, format!("Field not found in object: {}", field)),
                logic::DbError::NotAGeoPoint(field) => (StatusCode::BAD_REQUEST, format!("Field is not a valid GeoPoint: {}", field)),
                logic::DbError::InvalidGeoSortedKey(key) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Invalid geo sorted index key format: {}", key)),
                logic::DbError::AstQueryError(msg) => (StatusCode::BAD_REQUEST, format!("AST Query Error: {}", msg)),
                logic::DbError::InvalidPath(path) => (StatusCode::BAD_REQUEST, format!("Invalid path specified: {}", path)),
                logic::DbError::TransactionOperationFailed(msg) => (StatusCode::CONFLICT, format!("Transaction failed: {}", msg)),
                logic::DbError::InvalidFieldIndexKey(key) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Invalid field index key format: {}", key)),
            },
            AppError::Json(json_err) => (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", json_err)),
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized: Missing or invalid API key".to_string()),
        };
        error!("Error processing request: {}", self);
        (status, Json(json!({ "error": error_message }))).into_response()
    }
}