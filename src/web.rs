use crate::stats::{StatsHandle, StatsStatus};
use axum::extract::State;
use axum::{routing, Json, Router};
use log::{info, warn};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

type ServerStateType = Arc<Mutex<ServerState>>;

async fn stats(State(server_state): State<ServerStateType>) -> Json<StatsStatus> {
    Json(server_state.lock().unwrap().stats_handle.get_status())
}

async fn delete_service(State(server_state): State<ServerStateType>) {
    server_state.lock().unwrap().cancel_token.cancel()
}

struct ServerState {
    cancel_token: CancellationToken,
    stats_handle: StatsHandle,
}

pub async fn server(cancel_token: CancellationToken, port: u16, stats_handle: StatsHandle) {
    let state = Arc::new(Mutex::new(ServerState {
        cancel_token: cancel_token.clone(),
        stats_handle,
    }));
    let app = Router::new()
        .route("/status", routing::get(stats))
        .route("/service", routing::delete(delete_service))
        .with_state(state);

    info!("Starting web service on port {}", port);

    axum::Server::bind(&format!("0.0.0.0:{}", port).parse().unwrap())
        .serve(app.into_make_service())
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            warn!("Gracefully shut down web server");
        })
        .await
        .expect("Failure in HTTP server");
}
