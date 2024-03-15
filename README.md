![img.png](img.png)

Last 5 drivers and their top speed
http://localhost:8000/

    SELECT
    driver_name,
    top_speed
    FROM racing_car_metrics
    ORDER BY last_updated DESC LIMIT 5;

Max speed for a driver
http://localhost:8000/driver/Stacy/max_speed

    SELECT max(top_speed) 
    FROM racing_car_metrics 
    WHERE driver_name = 'Stacy';

Max speed for drivers within a timeframe
http://localhost:8000/driver/Stacy/max_speed/5s

    SELECT max(top_speed) 
    FROM racing_car_metrics 
    WHERE driver_name = 'Stacy' 
      AND last_updated > (NOW() - INTERVAL '5 seconds');