CREATE TABLE racing_cars
(
    id           SERIAL PRIMARY KEY,
    driver_name  VARCHAR(255),
    top_speed    INT,
    acceleration INT,
    handling     INT,
last_updated TIMESTAMP
);

TRUNCATE TABLE racing_cars;

DO
$$
    DECLARE
i INT;
BEGIN
FOR i IN 1..10000000
            LOOP
                INSERT INTO racing_cars (driver_name, top_speed, acceleration, handling, last_updated)
                VALUES ('Driver ' || i,
                        (random() * (300 - 50) + 50)::INT, -- top_speed range: 50 - 300
                        round((random() * (10 - 1) + 1)::numeric, 2), -- acceleration range: 1 - 10
                        round((random() * (5 - 1) + 1)::numeric, 2), -- handling range: 1 - 5
                        CURRENT_TIMESTAMP);
END LOOP;
END
$$;

CREATE INDEX idx_racing_cars_last_updated ON racing_cars (last_updated DESC);
DROP INDEX idx_racing_cars_last_updated;