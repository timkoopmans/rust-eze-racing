CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION =
    { 'class' : 'NetworkTopologyStrategy',
        'replication_factor' : 3 };
DROP KEYSPACE demo;
USE demo;

DROP TABLE demo.racing_car_metrics;
CREATE TABLE demo.racing_car_metrics
(
    id           UUID PRIMARY KEY,
    driver_name  text,
    top_speed    int,
    acceleration int,
    handling     int,
    last_updated timestamp
);