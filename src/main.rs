use std::sync::Arc;
use axum::{Router, routing::get};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use scylla::{Session, SessionBuilder};
use tokio_postgres::NoTls;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::db::{
    drivers_by_last_updated, max_speed_for_driver, max_speed_for_driver_in_timeframe, writer,
};

mod db;
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

    // set up connection pool
    let manager = PostgresConnectionManager::new_from_stringlike(
        "host=localhost user=postgres password=rusteze port=5432 dbname=rusteze",
        NoTls,
    )
        .unwrap();
    let pool = Pool::builder().build(manager).await.unwrap();

    // spawn the background task
    tokio::spawn(writer(pool.clone()));

    // TODO: Do I need to use a connection pool for ScyllaDB?
    let session: Arc<Session> = Arc::new(SessionBuilder::new()
        .known_nodes(vec!["0.0.0.0:9041", "0.0.0.0:9042", "0.0.0.0:9043"])
        .build()
        .await.unwrap());

    tokio::spawn(sdb::writer(session));

    // build our application with some routes
    let app = Router::new()
        .route("/", get(drivers_by_last_updated))
        .route("/driver/:driver_name/max_speed", get(max_speed_for_driver))
        .route(
            "/driver/:driver_name/max_speed/:since_time",
            get(max_speed_for_driver_in_timeframe),
        )
        .with_state(pool);

    // run it
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8000")
        .await
        .unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}