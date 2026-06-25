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

USE lake.mcap;

.print 'Attached Iceberg catalog as "lake".  You are in the `lake.mcap` schema.  Try: SHOW ALL TABLES;  or  SELECT * FROM sensor_baro LIMIT 10;'
