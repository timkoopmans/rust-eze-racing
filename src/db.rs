use axum::{
    async_trait,
    extract::{FromRef, FromRequestParts, State},
    http::{request::Parts, StatusCode},
};
use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use std::time::Instant;
use axum::extract::Path;
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

pub async fn drivers_by_last_updated(
    State(pool): State<ConnectionPool>,
) -> Result<String, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let start_time = Instant::now();

    let rows = conn
        .query("SELECT \
        driver_name, \
        top_speed \
        FROM racing_cars \
        ORDER BY last_updated DESC LIMIT 5", &[])
        .await
        .map_err(internal_error)?;

    let query_time = start_time.elapsed();

    let drivers: Vec<String> = rows.iter().map(|row| {
        let driver_name: String = row.try_get("driver_name").unwrap();
        let top_speed: i32 = row.try_get("top_speed").unwrap();
        format!("{}: {}", driver_name, top_speed)
    }).collect();

    Ok(format!("{}\n---\nQuery Time: {:?}", drivers.join("\n"), query_time))
}

pub async fn max_speed_for_driver(
    State(pool): State<ConnectionPool>,
    Path(driver_name): Path<String>,
) -> Result<String, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    if driver_name.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "driver_name cannot be empty".to_string()));
    }

    let start_time = Instant::now();

    let row = conn
        .query_one("SELECT max(top_speed) FROM racing_cars WHERE driver_name = $1", &[&driver_name])
        .await
        .map_err(internal_error)?;

    let query_time = start_time.elapsed();

    if row.is_empty() {
        return Ok("No data available".to_string());
    }

    let max_speed: Option<i32> = row.try_get("max").ok();

    match max_speed {
        Some(speed) => Ok(format!("{}\n---\nQuery Time: {:?}", speed.to_string(), query_time)),
        None => Ok("No data available".to_string()),
    }
}

pub async fn max_speed_for_driver_in_timeframe(
    State(pool): State<ConnectionPool>,
    Path((driver_name, since_time)): Path<(String, String)>,
) -> Result<String, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let since_time = humantime::parse_duration(&since_time)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid since_time".to_string()))?;

    let since_time = std::time::SystemTime::now()
        .checked_sub(since_time)
        .ok_or((StatusCode::BAD_REQUEST, "Invalid since_time".to_string()))?;

    let start_time = Instant::now();

    let row = conn
        .query_one("SELECT max(top_speed) FROM racing_cars WHERE driver_name = $1 AND last_updated > $2", &[&driver_name, &since_time])
        .await
        .map_err(internal_error)?;

    let query_time = start_time.elapsed();

    let max_speed: i32 = row.try_get(0).unwrap();

    Ok(format!("{}\n---\nQuery Time: {:?}", max_speed.to_string(), query_time))
}


pub async fn writer(pool: ConnectionPool) {
    let conn = pool.get().await.unwrap();
    let mut counter = 0u64;
    let mut start_time = Instant::now();
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
                &format!("{}", pick_random_name()),
                &(rand::random::<i32>().abs() % 251 + 50), // top_speed range: 50 - 300
                &(rand::random::<i32>().abs() % 10 + 1), // acceleration range: 1 - 10
                &(rand::random::<i32>().abs() % 5 + 1), // handling range: 1 - 5
            ],
        )
            .await;

        match result {
            Ok(_) => {
                tracing::debug!("writing data");
                counter += 1;
            }
            Err(e) => tracing::error!("Error writing data: {}", e),
        }

        if start_time.elapsed() >= Duration::from_secs(1) {
            let rate = counter as f64 / start_time.elapsed().as_secs_f64();
            tracing::info!("Rows written per second: {}", rate);
            counter = 0;
            start_time = Instant::now();
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