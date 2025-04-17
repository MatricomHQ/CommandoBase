use axum::{
    routing::{get, post},
    Router,
    response::{IntoResponse, Response, Json},
    http::StatusCode,
    extract::State,
};
use rust_db_logic::{
    self as logic,
    export_data,
    import_data,
    DbConfig as LogicDbConfig,
    BatchSetItem, // Import new types
    TransactionOperation,
};
use serde::{Serialize, Deserialize};
use serde_json::{Value, json}; // Import json macro
use sled::{Db, Config};
use std::sync::Arc;
use std::path::PathBuf;
use std::fs;
use tokio::net::TcpListener;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, error, Level, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use clap::Parser;
use thiserror::Error;

const DEFAULT_BASE_PATH: &str = "database_data_server";
const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:8989";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, env = "DB_PATH", value_name = "DIR", default_value = DEFAULT_BASE_PATH)]
    base_path: PathBuf,
    #[arg(short, long, env = "DB_NAME", value_name = "NAME")]
    db_name: String,
    #[arg(short, long, env = "LISTEN_ADDR", value_name = "HOST:PORT", default_value = DEFAULT_LISTEN_ADDR)]
    listen_addr: String,

}

#[derive(Clone, Debug)]
struct AppState {
    db: Arc<Db>,
    db_config: Arc<LogicDbConfig>,
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
    limit: Option<usize>, // Added
    offset: Option<usize>, // Added
}

#[derive(Serialize, Deserialize, Debug)]
struct ImportItem {
    key: String,
    value: Value,
}

type ImportPayload = Vec<ImportItem>;

// New Payloads
type BatchSetPayload = Vec<BatchSetItem>;
type TransactionPayload = Vec<TransactionOperation>;

#[derive(Deserialize, Debug)]
struct ClearPrefixPayload {
    prefix: String,
}

#[derive(Serialize)] // For response
struct CountResponse {
    count: usize,
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


    let db_config = Arc::new(LogicDbConfig::default());
    info!("Using default DbConfig: {:?}", db_config);


    let app_state = AppState { db, db_config };

    let app = Router::new()
        .route("/", get(health_check))
        .route("/set", post(set_handler))
        .route("/get", post(get_handler))
        .route("/get_partial", post(get_partial_handler))
        .route("/delete", post(delete_handler))
        .route("/batch_set", post(batch_set_handler)) // New
        .route("/transaction", post(transaction_handler)) // New
        .route("/clear_prefix", post(clear_prefix_handler)) // New
        .route("/drop_database", post(drop_database_handler)) // New
        .route("/query/radius", post(query_radius_handler))
        .route("/query/box", post(query_box_handler))
        .route("/query/and", post(query_and_handler))
        .route("/query/ast", post(query_ast_handler))
        .route("/export", get(export_handler))
        .route("/import", post(import_handler))
        .with_state(app_state.clone())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(tower_http::trace::DefaultOnResponse::new().level(Level::INFO).latency_unit(tower_http::LatencyUnit::Micros)),
        )
        .layer(CorsLayer::permissive());

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

    logic::set_key(&state.db, &payload.key, payload.value, &state.db_config)?;
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

    logic::delete_key(&state.db, &payload.key, &state.db_config).await?;
    Ok(StatusCode::OK)
}

// New Handlers
#[instrument(skip(state, payload), fields(handler="batch_set_handler"))]
async fn batch_set_handler(
    State(state): State<AppState>,
    Json(payload): Json<BatchSetPayload>,
) -> Result<StatusCode, AppError> {
    logic::batch_set(&state.db, &payload, &state.db_config)?; // Pass slice
    Ok(StatusCode::OK)
}

#[instrument(skip(state, payload), fields(handler="transaction_handler"))]
async fn transaction_handler(
    State(state): State<AppState>,
    Json(payload): Json<TransactionPayload>,
) -> Result<StatusCode, AppError> {
    logic::execute_transaction(&state.db, &payload, &state.db_config)?; // Pass slice
    Ok(StatusCode::OK)
}

#[instrument(skip(state, payload), fields(handler="clear_prefix_handler"))]
async fn clear_prefix_handler(
    State(state): State<AppState>,
    Json(payload): Json<ClearPrefixPayload>,
) -> Result<Json<CountResponse>, AppError> {
    let count = logic::clear_prefix(&state.db, &payload.prefix, &state.db_config)?;
    Ok(Json(CountResponse { count }))
}

#[instrument(skip(state), fields(handler="drop_database_handler"))]
async fn drop_database_handler(
    State(state): State<AppState>,
) -> Result<Json<CountResponse>, AppError> {
    let count = logic::drop_database(&state.db, &state.db_config)?;
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
    // Pass limit and offset
    let results = logic::execute_ast_query(&state.db, payload.ast, payload.projection, payload.limit, payload.offset)?;
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
    let data_string = serde_json::to_string(&payload)?;

    import_data(&state.db, &data_string, &state.db_config)?;
    Ok(StatusCode::CREATED)
}

#[derive(Error, Debug)]
enum AppError {
    #[error(transparent)]
    Logic(#[from] logic::DbError),
    #[error("JSON Error: {0}")]
    Json(#[from] serde_json::Error),
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
                logic::DbError::TransactionOperationFailed(msg) => (StatusCode::CONFLICT, format!("Transaction failed: {}", msg)), // Added
            },
            AppError::Json(json_err) => (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", json_err)),
        };
        error!("Error processing request: {}", self);
        (status, Json(json!({ "error": error_message }))).into_response()
    }
}