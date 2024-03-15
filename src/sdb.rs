use crate::drivers::pick_random_name;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use scylla::frame::value::CqlTimestamp;
use scylla::Session;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

pub async fn drivers_by_last_updated(State(session): State<Arc<Session>>) -> impl IntoResponse {
    let start_time = Instant::now();

    // TODO: Error message: ORDER BY is only supported when the partition key is restricted by an EQ or an IN.
    let result = session
        .query(
            "SELECT
        driver_name,
        top_speed
        FROM demo.racing_car_metrics
        ORDER BY last_updated DESC LIMIT 5",
            &[],
        )
        .await;

    let query_time = start_time.elapsed();

    match result {
        Ok(result) => {
            // TODO: use a struct instead of tuples
            let mut rows = result.rows_typed::<(String, i32)>().unwrap();

            let mut drivers = Vec::new();
            while let Some((driver_name, top_speed)) = rows.next().transpose().unwrap() {
                drivers.push(format!("{}: {}", driver_name, top_speed));
            }

            (
                StatusCode::OK,
                format!("{}\n---\nQuery Time: {:?}", drivers.join("\n"), query_time),
            )
                .into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn max_speed_for_driver(
    State(session): State<Arc<Session>>,
    Path(driver_name): Path<String>,
) -> impl IntoResponse {
    if driver_name.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "driver_name cannot be empty".to_string(),
        )
            .into_response());
    }

    let start_time = Instant::now();

    // TODO: Database returned an error: The query is syntactically correct but invalid ... (ALLOW FILTERING)
    let result = session
        .query(
            "SELECT max(top_speed) FROM demo.racing_car_metrics WHERE driver_name = ?",
            (driver_name,),
        )
        .await;

    let query_time = start_time.elapsed();

    match result {
        Ok(result) => {
            let mut rows = result.rows_typed::<(i32,)>().unwrap();
            let row = rows.next();

            let max_speed = match row {
                Some(Ok((top_speed,))) => top_speed,
                Some(Err(e)) => {
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())
                }
                None => return Err((StatusCode::OK, "No data found".to_string()).into_response()),
            };

            Ok((
                StatusCode::OK,
                format!("{}\n---\nQuery Time: {:?}", max_speed, query_time),
            )
                .into_response())
        }
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()),
    }
}

pub async fn max_speed_for_driver_in_timeframe(
    State(session): State<Arc<Session>>,
    Path((driver_name, since_time)): Path<(String, String)>,
) -> impl IntoResponse {
    let since_time = humantime::parse_duration(&since_time)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid since_time".to_string()))
        .unwrap();

    let since_time = chrono::Duration::from_std(since_time)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid since_time".to_string()))
        .unwrap();

    let last_updated = chrono::Utc::now()
        .checked_sub_signed(since_time)
        .ok_or((StatusCode::BAD_REQUEST, "Invalid since_time".to_string()))
        .unwrap();

    let last_updated_millis = last_updated.timestamp_millis();

    let start_time = Instant::now();

    // TODO: Database returned an error: The query is syntactically correct but invalid ... (ALLOW FILTERING)
    let result = session
        .query(
            "SELECT max(top_speed) FROM demo.racing_car_metrics WHERE driver_name = ? AND last_updated > ?",
            (driver_name, CqlTimestamp(last_updated_millis)),
        )
        .await;

    let query_time = start_time.elapsed();

    match result {
        Ok(result) => {
            let mut rows = result.rows_typed::<(i32,)>().unwrap();
            let row = rows.next();

            let max_speed = match row {
                Some(Ok((top_speed,))) => top_speed,
                Some(Err(e)) => {
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())
                }
                None => return Err((StatusCode::OK, "No data found".to_string()).into_response()),
            };

            Ok((
                StatusCode::OK,
                format!("{}\n---\nQuery Time: {:?}", max_speed, query_time),
            )
                .into_response())
        }
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()),
    }
}

pub async fn writer(session: Arc<Session>) {
    let mut counter = 0u64;
    let mut start_time = Instant::now();

    loop {
        // TODO: Should I be using prepared statements here?
        // TODO: Should I maybe batch statements?
        // TODO: How can I avoid prefixing the keyspace in all my queries?
        let result = session
            .query(
                "INSERT INTO demo.racing_car_metrics (
                    id,
                    driver_name,
                    top_speed,
                    acceleration,
                    handling,
                    last_updated)
                    VALUES (?, ?, ?, ?, ?, ?)",
                (
                    Uuid::new_v4(),
                    &pick_random_name().to_string(),
                    &(rand::random::<i32>().abs() % 251 + 50), // top_speed range: 50 - 300
                    &(rand::random::<i32>().abs() % 10 + 1),   // acceleration range: 1 - 10
                    &(rand::random::<i32>().abs() % 5 + 1),    // handling range: 1 - 5
                    &CqlTimestamp(Utc::now().timestamp_millis()),
                ),
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