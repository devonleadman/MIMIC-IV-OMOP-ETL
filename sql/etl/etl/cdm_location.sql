-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_care_site table
-- 
-- Dependencies: run after st_core.sql
-- on Demo: 
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize "create or replace"
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_location
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS cdm_location;
CREATE TABLE cdm_location
(
    location_id           BIGINT     NOT NULL,
    address_1             TEXT                ,
    address_2             TEXT                ,
    city                  TEXT                ,
    state                 TEXT                ,
    zip                   TEXT                ,
    county                TEXT                ,
    location_source_value TEXT                ,
    unit_id               TEXT,
    load_table_id         TEXT,
    load_row_id           BIGINT,
    trace_id              TEXT
);

-- -------------------------------------------------------------------
-- Insert data into cdm_location
-- -------------------------------------------------------------------

INSERT INTO cdm_location
SELECT
    1                           AS location_id,
    NULL                        AS address_1,
    NULL                        AS address_2,
    NULL                        AS city,
    'MA'                        AS state,
    NULL                        AS zip,
    NULL                        AS county,
    'Beth Israel Hospital'      AS location_source_value,
    'location.null'             AS unit_id,
    'null'                      AS load_table_id,
    0                           AS load_row_id,
    NULL                        AS trace_id;
