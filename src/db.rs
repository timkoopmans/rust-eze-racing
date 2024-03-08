use axum::{
    async_trait,
    extract::{FromRef, FromRequestParts, State},
    http::{request::Parts, StatusCode},
};
use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use std::time::Instant;
use tokio::time::{sleep, Duration};
use crate::drivers::pick_random_name;

type ConnectionPool = Pool<PostgresConnectionManager<NoTls>>;

pub struct DatabaseConnection(PooledConnection<'static, PostgresConnectionManager<NoTls>>);

#[async_trait]
impl<S> FromRequestParts<S> for DatabaseConnection
    where
        ConnectionPool: FromRef<S>,
        S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(_parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let pool = ConnectionPool::from_ref(state);

        let conn = pool.get_owned().await.map_err(internal_error)?;

        Ok(Self(conn))
    }
}

pub async fn reader(
    State(pool): State<ConnectionPool>,
) -> Result<String, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let start_time = Instant::now(); // Capture the start time

    let rows = conn
        .query("SELECT \
        driver_name, \
        top_speed \
        FROM racing_cars \
        ORDER BY last_updated DESC LIMIT 5", &[])
        .await
        .map_err(internal_error)?;

    let query_time = start_time.elapsed(); // Calculate the query time

    let drivers: Vec<String> = rows.iter().map(|row| {
        let driver_name: String = row.try_get("driver_name").unwrap();
        let top_speed: i32 = row.try_get("top_speed").unwrap();
        format!("{}: {}", driver_name, top_speed)
    }).collect();

    Ok(format!("{}\n---\nQuery Time: {:?}", drivers.join("\n"), query_time))
}

pub async fn writer(pool: ConnectionPool) {
    let conn = pool.get().await.unwrap();
    loop {
        let result = conn.execute(
            "INSERT INTO racing_cars (
            driver_name,
            top_speed,
            acceleration,
            handling,
            last_updated)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)",
            &[
                &format!("Driver {}", pick_random_name()),
                &(rand::random::<i32>().abs() % 251 + 50), // top_speed range: 50 - 300
                &(rand::random::<i32>().abs() % 10 + 1), // acceleration range: 1 - 10
                &(rand::random::<i32>().abs() % 5 + 1), // handling range: 1 - 5
            ],
        )
            .await;

        match result {
            Ok(_) => tracing::debug!("writing data"),
            Err(e) => tracing::error!("Error writing data: {}", e),
        }

        sleep(Duration::from_micros(1)).await;
    }
}

fn internal_error<E>(err: E) -> (StatusCode, String)
    where
        E: std::error::Error,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}