INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;

CREATE OR REPLACE SECRET minio (
    TYPE S3,
    KEY_ID 'admin',
    SECRET 'password',
    ENDPOINT 'minio:9000',
    URL_STYLE 'path',
    USE_SSL false
);


ATTACH 'warehouse' AS lake (
    TYPE ICEBERG,
    ENDPOINT 'http://iceberg-rest:8181',
    AUTHORIZATION_TYPE 'none'
);

-- This schema contains curated views, separate from `lake.mcap`.
CREATE SCHEMA IF NOT EXISTS curated;

CREATE OR REPLACE MACRO curated.format_duration(duration)
AS CASE
    WHEN duration > INTERVAL (1) MINUTE THEN DATEPART('minute', duration) || 'm' || DATEPART('second', duration) || 's'
    ELSE DATEPART('second', duration) || 's'
END;

-- Error counts per flight
CREATE OR REPLACE VIEW curated.error_counts AS
SELECT
    mcap_id,
    count(CASE WHEN level = 'ERROR' THEN 1 END) as error_count
FROM lake.mcap.logged_messages
GROUP BY 1;

-- Very high-level flight overview:
-- id, log_date, duration, armed_duration
CREATE OR REPLACE VIEW curated.flights AS
WITH flight_time_stats AS (
    SELECT
        mcap_id,
        min(log_date) as flight_date,
        min(log_time) as start_time,
        format_duration(max(log_time) - min(log_time)) as duration,
        format_duration(
            max(log_time) FILTER (WHERE arming_state = 2)
            - min(log_time) FILTER (WHERE arming_state = 2)
        ) as armed_duration
    FROM lake.mcap.vehicle_status
    GROUP BY mcap_id
)

SELECT
    mcap_id,
    flight_date,
    start_time,
    duration,
    armed_duration,
    COALESCE(error_counts.error_count, 0) as error_count
FROM
    flight_time_stats
    LEFT JOIN curated.error_counts
        using(mcap_id);

CREATE OR REPLACE VIEW curated.battery AS
SELECT
    mcap_id,
    multi_id,
    cell_count,
    min(voltage_v) as min_voltage,
    max(voltage_v) as max_voltage,
    min(current_a) as min_current,
    max(current_a) as max_current,
    min(remaining) as min_remaining,
    max(remaining) as max_remaining,
    min(temperature) as min_temperature,
    max(temperature) as max_temperature,
FROM lake.mcap.battery_status
group by 1, 2, 3;

SET search_path='lake.mcap,memory.curated';

.print 'Attached curated views as `curated`.  Attached raw Iceberg catalog as `lake.mcap`'
.print 'Try `SHOW ALL TABLES;`, `SELECT * FROM curated.flights;`, or `SELECT * FROM mcap.logged_messages LIMIT 10;`'
