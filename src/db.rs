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
use chrono::{DateTime, Utc};

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

pub async fn max_speed_for_driver(
    State(pool): State<ConnectionPool>,
    driver_name: String,
) -> Result<String, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let rows = conn
        .query("SELECT max(top_speed) FROM racing_cars WHERE driver_name = $1", &[&driver_name])
        .await
        .map_err(internal_error)?;

    if rows.is_empty() {
        return Ok("No data available".to_string());
    }
    println!("First row: {:?}", rows[0]);

    println!("Executed query, number of rows returned: {}", rows.len());


    let max_speed: i32 = rows[0].try_get("max").unwrap_or(0);
    println!("Max speed: {:?}", max_speed);

    Ok(max_speed.to_string())

    // match max_speed {
    //     Some(speed) => Ok(speed.to_string()),
    //     None => Ok("No data available".to_string()),
    // }
}

pub async fn max_speed_for_driver_in_timeframe(
    State(pool): State<ConnectionPool>,
    Path((driver_name, start_time)): Path<(String, String)>,
) -> Result<String, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    // Parse the start_time string into a DateTime<Utc>
    let start_time = DateTime::parse_from_rfc3339(&start_time)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid start_time".to_string()))?
        .with_timezone(&Utc);
    let start_time: std::time::SystemTime = start_time.into();

    let row = conn
        .query_one("SELECT max(top_speed) FROM racing_cars WHERE driver_name = $1 AND last_updated > $2", &[&driver_name, &start_time])
        .await
        .map_err(internal_error)?;



    let max_speed: i32 = row.try_get(0).unwrap();

    Ok(max_speed.to_string())
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
                &format!("{}", pick_random_name()),
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