CREATE EXTENSION IF NOT EXISTS "pgcrypto";

DROP TABLE IF EXISTS racing_car_metrics;
CREATE TABLE IF NOT EXISTS racing_car_metrics
(
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_name  VARCHAR(255),
    top_speed    INT,
    acceleration INT,
    handling     INT,
    last_updated TIMESTAMP
);

SELECT
    driver_name,
    top_speed
FROM racing_car_metrics
ORDER BY last_updated DESC LIMIT 5;

CREATE INDEX idx_racing_cars_last_updated ON racing_car_metrics (last_updated DESC);
DROP INDEX idx_racing_cars_last_updated;

SELECT max(top_speed) FROM racing_car_metrics WHERE driver_name = 'Stacy';

CREATE INDEX idx_driver_name ON racing_car_metrics (driver_name);
DROP INDEX idx_driver_name;

SELECT max(top_speed)
FROM racing_car_metrics
WHERE driver_name = 'Stacy'
  AND last_updated > (NOW() - INTERVAL '5 seconds');

CREATE INDEX idx_driver_name_last_updated ON racing_car_metrics (driver_name, last_updated);
DROP INDEX idx_driver_name_last_updated;

DO
$$
    DECLARE
        i INT;
    BEGIN
        FOR i IN 1..10000000
            LOOP
                INSERT INTO racing_car_metrics (driver_name, top_speed, acceleration, handling, last_updated)
                VALUES ('Driver ' || i,
                        (random() * (300 - 50) + 50)::INT, -- top_speed range: 50 - 300
                        round((random() * (10 - 1) + 1)::numeric, 2), -- acceleration range: 1 - 10
                        round((random() * (5 - 1) + 1)::numeric, 2), -- handling range: 1 - 5
                        CURRENT_TIMESTAMP);
            END LOOP;
    END
$$;



CREATE INDEX idx_driver_name ON racing_car_metrics (driver_name);
DROP INDEX idx_driver_name;