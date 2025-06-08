use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Semaphore, TryAcquireError};

#[derive(Parser, Debug)]
#[command(author, version, about = "High-performance Rust mock server with configurable buffer & queue")]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    bind: String,

    /// Maximum concurrent worker processing capacity
    #[arg(short, long, default_value_t = 100)]
    concurrency_limit: usize,

    /// Maximum incoming queue/buffer capacity before shedding with 429/503
    #[arg(short, long, default_value_t = 500)]
    queue_capacity: usize,

    /// Simulated downstream service processing delay in milliseconds
    #[arg(short, long, default_value_t = 50)]
    processing_delay_ms: u64,
}

#[derive(Clone)]
struct AppState {
    active_semaphore: Arc<Semaphore>,
    queue_semaphore: Arc<Semaphore>,
    total_received: Arc<AtomicU64>,
    total_processed: Arc<AtomicU64>,
    total_shed: Arc<AtomicU64>,
    current_queued: Arc<AtomicUsize>,
    current_active: Arc<AtomicUsize>,
    default_delay_ms: u64,
}

#[derive(Deserialize)]
struct WorkQuery {
    delay_ms: Option<u64>,
}

#[derive(Serialize)]
struct MetricsResponse {
    received: u64,
    processed: u64,
    shed: u64,
    current_queued: usize,
    current_active: usize,
}

#[derive(Serialize)]
struct WorkResponse {
    status: &'static str,
    active: usize,
    queued: usize,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let state = AppState {
        active_semaphore: Arc::new(Semaphore::new(args.concurrency_limit)),
        queue_semaphore: Arc::new(Semaphore::new(args.queue_capacity)),
        total_received: Arc::new(AtomicU64::new(0)),
        total_processed: Arc::new(AtomicU64::new(0)),
        total_shed: Arc::new(AtomicU64::new(0)),
        current_queued: Arc::new(AtomicUsize::new(0)),
        current_active: Arc::new(AtomicUsize::new(0)),
        default_delay_ms: args.processing_delay_ms,
    };

    println!("===============================================================");
    println!("🚀 Rust Mock Overflow HTTP Server (Tokio + Axum)");
    println!("Listening on:           {}", args.bind);
    println!("Active Worker Limit:    {}", args.concurrency_limit);
    println!("Queue/Buffer Capacity:  {}", args.queue_capacity);
    println!("Simulated Delay:        {} ms", args.processing_delay_ms);
    println!("===============================================================");

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/work", get(work_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let addr: SocketAddr = args.bind.parse().expect("Invalid bind address");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(MetricsResponse {
        received: state.total_received.load(Ordering::Relaxed),
        processed: state.total_processed.load(Ordering::Relaxed),
        shed: state.total_shed.load(Ordering::Relaxed),
        current_queued: state.current_queued.load(Ordering::Relaxed),
        current_active: state.current_active.load(Ordering::Relaxed),
    })
}

async fn work_handler(
    State(state): State<AppState>,
    Query(query): Query<WorkQuery>,
) -> Result<impl IntoResponse, (StatusCode, &'static str)> {
    state.total_received.fetch_add(1, Ordering::Relaxed);

    // 1. Try to admit into the admission queue / buffer
    let queue_permit = match state.queue_semaphore.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => {
            state.total_shed.fetch_add(1, Ordering::Relaxed);
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                "{\"error\": \"Buffer Full: Request Shed\"}",
            ));
        }
        Err(TryAcquireError::Closed) => {
            return Err((StatusCode::SERVICE_UNAVAILABLE, "Queue Closed"));
        }
    };

    state.current_queued.fetch_add(1, Ordering::Relaxed);

    // 2. Wait in queue until an active execution slot is available
    let active_permit = state
        .active_semaphore
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Semaphore Error"))?;

    // Now actively executing: release queue permit
    drop(queue_permit);
    state.current_queued.fetch_sub(1, Ordering::Relaxed);
    state.current_active.fetch_add(1, Ordering::Relaxed);

    // 3. Simulate downstream I/O latency
    let delay = query.delay_ms.unwrap_or(state.default_delay_ms);
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }

    let active = state.current_active.fetch_sub(1, Ordering::Relaxed) - 1;
    let queued = state.current_queued.load(Ordering::Relaxed);
    state.total_processed.fetch_add(1, Ordering::Relaxed);
    drop(active_permit);

    Ok(Json(WorkResponse {
        status: "success",
        active,
        queued,
    }))
}
