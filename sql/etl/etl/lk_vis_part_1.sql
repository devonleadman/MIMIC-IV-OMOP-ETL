-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate first part of lookups for cdm_visit_occurrence and cdm_visit_detail
-- to use it for lk_meas_* tables and then vise versa
-- 
-- Dependencies: run after 
--      st_core.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- There were logic for post mortem donors in MIMIC III based on patients' diagnosis
-- It is not implemented here
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_admissions_clean
--
-- All to visit_occurrence, to create visits with hadm_id
-- Then there will be added visits without hadm_id
-- ER admissions to visit_detail too
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Create lk_admissions_clean
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_admissions_clean;
CREATE TABLE lk_admissions_clean AS
SELECT
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    CASE 
        WHEN src.edregtime < src.admittime THEN src.edregtime
        ELSE src.admittime
    END                                             AS start_datetime, -- the earliest of
    src.dischtime                                   AS end_datetime,
    src.admission_type                              AS admission_type, -- current location
    src.admission_location                          AS admission_location, -- to hospital
    src.discharge_location                          AS discharge_location, -- from hospital
    CASE 
        WHEN src.edregtime IS NULL THEN FALSE
        ELSE TRUE
    END                                             AS is_er_admission, -- create visit_detail if TRUE
    'admissions'                                    AS unit_id,
    src.load_table_id                               AS load_table_id,
    src.load_row_id                                 AS load_row_id,
    src.trace_id                                    AS trace_id
FROM
    src_admissions src;

-- -------------------------------------------------------------------
-- Create lk_transfers_clean
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_transfers_clean;
CREATE TABLE lk_transfers_clean AS
SELECT
    src.subject_id                                  AS subject_id,
    COALESCE(src.hadm_id, vis.hadm_id)              AS hadm_id,
    CAST(src.intime AS DATE)                        AS date_id,
    src.transfer_id                                 AS transfer_id,
    src.intime                                      AS start_datetime,
    src.outtime                                     AS end_datetime,
    src.careunit                                    AS current_location, -- find prev and next for adm and disch location
    'transfers'                                     AS unit_id,
    src.load_table_id                               AS load_table_id,
    src.load_row_id                                 AS load_row_id,
    src.trace_id                                    AS trace_id
FROM 
    src_transfers src
LEFT JOIN
    lk_admissions_clean vis
        ON vis.subject_id = src.subject_id
        AND src.intime::TIMESTAMP BETWEEN vis.start_datetime AND vis.end_datetime
        AND src.hadm_id IS NULL
WHERE 
    src.eventtype != 'discharge'; -- these are not useful

-- -------------------------------------------------------------------
-- Create lk_services_duplicated
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_services_duplicated;
CREATE TABLE lk_services_duplicated AS
SELECT
    src.trace_id::TEXT, COUNT(*) AS row_count
FROM 
    src_services src
GROUP BY
    src.trace_id::TEXT
HAVING COUNT(*) > 1;

-- -------------------------------------------------------------------
-- Create lk_services_clean
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_services_clean;
CREATE TABLE lk_services_clean AS
SELECT
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    src.transfertime                                AS start_datetime,
    LEAD(src.transfertime) OVER (
        PARTITION BY src.subject_id, src.hadm_id 
        ORDER BY src.transfertime
    )                                               AS end_datetime,
    src.curr_service                                AS curr_service,
    src.prev_service                                AS prev_service,
    LAG(src.curr_service) OVER (
        PARTITION BY src.subject_id, src.hadm_id 
        ORDER BY src.transfertime
    )                                               AS lag_service,
    'services'                                      AS unit_id,
    src.load_table_id                               AS load_table_id,
    src.load_row_id                                 AS load_row_id,
    src.trace_id                                    AS trace_id
FROM 
    src_services src
LEFT JOIN
    lk_services_duplicated sd
        ON src.trace_id::TEXT = sd.trace_id
WHERE
    sd.trace_id IS NULL; -- remove duplicates with the exact same time of transferring

