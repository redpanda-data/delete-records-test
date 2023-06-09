use crate::config::DeleteRecordPosition;
use crate::stats::{StatsHandle, StatsStatus};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::{routing, Json, Router};
use log::{error, info, warn};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

type ServerStateType = Arc<Mutex<ServerState>>;

async fn stats(State(server_state): State<ServerStateType>) -> Json<StatsStatus> {
    Json(server_state.lock().await.stats_handle.get_status())
}

async fn delete_service(State(server_state): State<ServerStateType>) {
    server_state.lock().await.cancel_token.cancel()
}

#[derive(Deserialize)]
struct RecordPosition {
    position: DeleteRecordPosition,
}

async fn delete_record(
    State(server_state): State<ServerStateType>,
    Query(params): Query<RecordPosition>,
) -> Result<(), StatusCode> {
    let server_state = server_state.lock().await;
    match server_state
        .trigger_record_delete
        .send(params.position)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("Error during sending of trigger: {}", e);
            Err(StatusCode::from_u16(500).unwrap())
        }
    }
}

struct ServerState {
    cancel_token: CancellationToken,
    stats_handle: StatsHandle,
    trigger_record_delete: Sender<DeleteRecordPosition>,
}

pub async fn server(
    cancel_token: CancellationToken,
    port: u16,
    stats_handle: StatsHandle,
    trigger_record_delete: Sender<DeleteRecordPosition>,
) {
    let state = Arc::new(Mutex::new(ServerState {
        cancel_token: cancel_token.clone(),
        stats_handle,
        trigger_record_delete,
    }));
    let app = Router::new()
        .route("/status", routing::get(stats))
        .route("/service", routing::delete(delete_service))
        .route("/record", routing::delete(delete_record))
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
