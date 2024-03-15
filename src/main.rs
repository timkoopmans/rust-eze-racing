use axum::{routing::get, Router};
use scylla::{Session, SessionBuilder};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use crate::sdb::{drivers_by_last_updated, max_speed_for_driver, max_speed_for_driver_in_timeframe};

mod drivers;
mod sdb;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rust_eze_racing=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // TODO: Do I need to use a connection pool for ScyllaDB?
    let session: Arc<Session> = Arc::new(
        SessionBuilder::new()
            .known_nodes(vec!["0.0.0.0:9041", "0.0.0.0:9042", "0.0.0.0:9043"])
            .build()
            .await
            .unwrap(),
    );

    tokio::spawn(sdb::writer(session.clone()));

    // build our application with some routes
    let app = Router::new()
        .route("/", get(drivers_by_last_updated))
        .route("/driver/:driver_name/max_speed", get(max_speed_for_driver))
        .route(
            "/driver/:driver_name/max_speed/:since_time",
            get(max_speed_for_driver_in_timeframe),
        )
        .with_state(session);

    // run it
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8000")
        .await
        .unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}