use std::sync::Arc;
use std::time::{Duration, Instant};
use scylla::{Session};
use tokio::time::sleep;
use uuid::Uuid;
use crate::drivers::pick_random_name;
use chrono::Utc;
use scylla::frame::value::CqlTimestamp;
use anyhow::Result;
use scylla::prepared_statement::PreparedStatement;
use scylla::batch::Batch;

pub async fn writer(session: Arc<Session>) -> Result<()> {
    let mut counter = 0u64;
    let mut start_time = Instant::now();

    // Create a batch statement
    let mut batch: Batch = Default::default();

    let prepared: PreparedStatement = session
        .prepare("INSERT INTO demo.racing_car_metrics (
                    id,
                    driver_name,
                    top_speed,
                    acceleration,
                    handling,
                    last_updated)
                    VALUES (?, ?, ?, ?, ?, ?)")
        .await?;
    batch.append_statement(prepared);

    let mut batch_values = Vec::new();

    loop {
        batch_values.push((
            Uuid::new_v4(),
            pick_random_name().to_string(),
            rand::random::<i32>().abs() % 251 + 50, // top_speed range: 50 - 300
            rand::random::<i32>().abs() % 10 + 1,   // acceleration range: 1 - 10
            rand::random::<i32>().abs() % 5 + 1,    // handling range: 1 - 5
            CqlTimestamp(Utc::now().timestamp_millis()),
        ));

        if counter % 1000 == 0 {
            let result = session.batch(&batch, &batch_values).await;
            batch_values.clear();

            match result {
                Ok(_) => {
                    tracing::debug!("writing data");
                    counter += 1000;
                }
                Err(e) => tracing::error!("Error writing data: {}", e),
            }
        }
        
        if start_time.elapsed() >= Duration::from_secs(1) {
            let rate = counter as f64 / start_time.elapsed().as_secs_f64();
            tracing::info!("Rows written per second: {}", rate);
            counter = 0;
            start_time = Instant::now();
        }

        // sleep(Duration::from_micros(1)).await;
    }
}