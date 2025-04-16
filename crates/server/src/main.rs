use axum::{
    routing::{get, post},
    Router,
    response::{IntoResponse, Response, Json},
    http::StatusCode,
    extract::State,
    serve,
};
use rust_db_logic::{self as logic, DbError};
use serde::{Serialize, Deserialize};
use serde_json::Value;
use sled::Db;
use std::sync::Arc;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, error, debug, trace, Level, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use clap::Parser;
use thiserror::Error;


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
                 logic::DbError::NotFound => (StatusCode::NOT_FOUND, "Key not found".to_string()),
                 logic::DbError::MissingData(field) => (StatusCode::BAD_REQUEST, format!("Missing field: {}", field)),
                 logic::DbError::Transaction(msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Transaction error: {}", msg)),
                 logic::DbError::Io(io_err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("IO error: {}", io_err)),
            },
            AppError::Json(json_err) => (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", json_err)),
        };
         error!("Error processing request: {}", self);
        (status, Json(serde_json::json!({ "error": error_message }))).into_response()
    }
}


const DEFAULT_DB_PATH: &str = "database_data_server";
const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:8989";


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, env = "DB_PATH", value_name = "DIR", default_value = DEFAULT_DB_PATH)]
    db_path: PathBuf,
    #[arg(short, long, env = "LISTEN_ADDR", value_name = "HOST:PORT", default_value = DEFAULT_LISTEN_ADDR)]
    listen_addr: String,
}


#[derive(Clone, Debug)]
struct AppState {
    db: Arc<Db>,
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

#[derive(Serialize, Deserialize, Debug)]
struct ImportItem {
     key: String,
     value: Value,
}
type ImportPayload = Vec<ImportItem>;



#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            "rust_db_server=info,tower_http=warn".into()
        }))
        .with(tracing_subscriber::fmt::layer())
        .init();

    trace!("Tracing initialized.");

    info!("Opening database at path: {:?}", args.db_path);
    let db_result = sled::open(&args.db_path);
    debug!("Sled open result: {:?}", db_result.is_ok());

    let db = match db_result {
        Ok(db) => Arc::new(db),
        Err(e) => {
            // Explicitly convert sled::Error to logic::DbError before wrapping in AppError
            let logic_error = logic::DbError::from(e);
            let app_error = AppError::from(logic_error);
            error!("Failed to open database at {:?}: {}", args.db_path, app_error);
            std::process::exit(1);
        }
    };
    info!("Database opened successfully.");

    let app_state = AppState { db };
    debug!("App state created: {:?}", app_state);

    let app = Router::new()
        .route("/", get(health_check))
        .route("/set", post(set_handler))
        .route("/get", post(get_handler))
        .route("/delete", post(delete_handler))
        .route("/query/radius", post(query_radius_handler))
        .route("/query/box", post(query_box_handler))
        .route("/export", get(export_handler))
        .route("/import", post(import_handler))
        .with_state(app_state.clone())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(tower_http::trace::DefaultOnResponse::new().level(Level::INFO).latency_unit(tower_http::LatencyUnit::Micros)),
        )
        .layer(CorsLayer::permissive());

    debug!("Router configured with state: {:?}", app_state);

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
    if let Err(e) = serve(listener, app.into_make_service()).await {
        error!("Server error: {}", e);
        std::process::exit(1);
    }

    trace!("Axum server loop finished.");
}


#[instrument(skip(state), fields(handler="health_check"))]
async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    info!("Entering health_check handler");
    trace!("Checking DB size on disk...");
     match state.db.size_on_disk() {
         Ok(size) => info!(db_size = size, "Health check OK"),
         Err(e) => error!("Health check failed to get DB size: {}", e),
     }
    trace!("Exiting health_check handler");
    (StatusCode::OK, "Server is running")
}

#[instrument(skip(state, payload), fields(handler="set_handler"))]
async fn set_handler(
    State(state): State<AppState>,
    Json(payload): Json<SetPayload>,
) -> Result<StatusCode, AppError> {
    info!("Entering set_handler");
    info!(key = %payload.key, "Handling set request");
    trace!("Calling logic::set_key");
    logic::set_key(&state.db, &payload.key, payload.value)?;
    trace!("Exiting set_handler");
    Ok(StatusCode::OK)
}

#[instrument(skip(state, payload), fields(handler="get_handler"))]
async fn get_handler(
    State(state): State<AppState>,
    Json(payload): Json<KeyPayload>,
) -> Result<Json<Value>, AppError> {
    info!("Entering get_handler");
    info!(key = %payload.key, "Handling get request");
    trace!("Calling logic::get_key");
    let value = logic::get_key(&state.db, &payload.key)?;
    trace!("Exiting get_handler");
    Ok(Json(value))
}

#[instrument(skip(state, payload), fields(handler="delete_handler"))]
async fn delete_handler(
    State(state): State<AppState>,
    Json(payload): Json<KeyPayload>,
) -> Result<StatusCode, AppError> {
    info!("Entering delete_handler");
    info!(key = %payload.key, "Handling delete request");
    trace!("Calling logic::delete_key");
    logic::delete_key(&state.db, &payload.key).await?; // Re-add .await
    trace!("Exiting delete_handler");
    Ok(StatusCode::OK)
}

#[instrument(skip(state, payload), fields(handler="query_radius_handler"))]
async fn query_radius_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryRadiusPayload>,
) -> Result<Json<Vec<Value>>, AppError> {
    info!("Entering query_radius_handler");
    info!(field = %payload.field, lat = payload.lat, lon = payload.lon, radius = payload.radius, "Handling query radius request");
    trace!("Calling logic::query_within_radius_simplified");
    let results = logic::query_within_radius_simplified(&state.db, &payload.field, payload.lat, payload.lon, payload.radius)?;
    trace!("Exiting query_radius_handler");
    Ok(Json(results))
}

#[instrument(skip(state, payload), fields(handler="query_box_handler"))]
async fn query_box_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryBoxPayload>,
) -> Result<Json<Vec<Value>>, AppError> {
    info!("Entering query_box_handler");
    info!(field = %payload.field, min_lat = payload.min_lat, min_lon = payload.min_lon, max_lat = payload.max_lat, max_lon = payload.max_lon, "Handling query box request");
    trace!("Calling logic::query_in_box");
    let results = logic::query_in_box(&state.db, &payload.field, payload.min_lat, payload.min_lon, payload.max_lat, payload.max_lon)?;
    trace!("Exiting query_box_handler");
    Ok(Json(results))
}

#[instrument(skip(state), fields(handler="export_handler"))]
async fn export_handler(
    State(state): State<AppState>,
) -> Result<Json<String>, AppError> {
    info!("Entering export_handler");
    trace!("Calling logic::export_data");
    let data_string = logic::export_data(&state.db)?;
    trace!("Exiting export_handler");
    Ok(Json(data_string))
}

#[instrument(skip(state, payload), fields(handler="import_handler"))]
async fn import_handler(
    State(state): State<AppState>,
    Json(payload): Json<ImportPayload>,
) -> Result<StatusCode, AppError> {
    info!("Entering import_handler");
    info!(count = payload.len(), "Handling import request");
    trace!("Serializing import payload");
    // The payload is already deserialized into ImportPayload (Vec<ImportItem>) by Axum
    // We need to serialize it back to a string for logic::import_data
    let data_string = serde_json::to_string(&payload)?;
    trace!("Calling logic::import_data");
    logic::import_data(&state.db, &data_string)?;
    trace!("Exiting import_handler");
    Ok(StatusCode::CREATED)
}