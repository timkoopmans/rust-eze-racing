mod db;
mod drivers;

use axum::{
    routing::get,
    Router,
};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use crate::db::{writer, reader};

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
    let manager =
        PostgresConnectionManager::new_from_stringlike("host=localhost user=postgres password=rusteze port=54321 dbname=rusteze", NoTls)
            .unwrap();
    let pool = Pool::builder().build(manager).await.unwrap();

    // spawn the background task
    tokio::spawn(writer(pool.clone()));

    // build our application with some routes
    let app = Router::new()
        .route(
            "/",
            get(reader),
        )
        .with_state(pool);

    // run it
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8000")
        .await
        .unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}