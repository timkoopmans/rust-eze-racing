use std::sync::Arc;
use std::time::{Duration, Instant};
use scylla::{Session};
use tokio::time::sleep;
use uuid::Uuid;
use crate::drivers::pick_random_name;
use chrono::Utc;
use scylla::frame::value::CqlTimestamp;

pub async fn writer(session: Arc<Session>) {
    let mut counter = 0u64;
    let mut start_time = Instant::now();

    loop {
        // TODO: Should I be using prepared statements here?
        // TODO: Should I maybe batch statements?
        let result = session.query(
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
        ).await;

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