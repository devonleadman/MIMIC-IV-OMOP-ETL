-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate staging tables for cdm dimension tables
-- 
-- Dependencies: run first after DDL
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_procedureevents
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_procedureevents
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_procedureevents;
CREATE TABLE src_procedureevents AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    stay_id                             AS stay_id,
    itemid                              AS itemid,
    starttime                           AS starttime,
    value                               AS value,
    0                                   AS cancelreason, -- Placeholder for removed field
    -- Static value
    'procedureevents'                   AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id, -- Generate a UUID-like value
    json_build_object(
        'subject_id', subject_id,
        'hadm_id', hadm_id,
        'starttime', starttime
    )                                  AS trace_id
FROM
    procedureevents;

-- -------------------------------------------------------------------
-- src_d_items
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_d_items;
CREATE TABLE src_d_items AS
SELECT
    itemid                              AS itemid,
    label                               AS label,
    linksto                             AS linksto,
    -- Static value
    'd_items'                           AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id, -- Generate a UUID-like value
    json_build_object(
        'itemid', itemid,
        'linksto', linksto
    )                                  AS trace_id
FROM
    d_items;

-- -------------------------------------------------------------------
-- src_datetimeevents
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_datetimeevents;
CREATE TABLE src_datetimeevents AS
SELECT
    subject_id  AS subject_id,
    hadm_id     AS hadm_id,
    stay_id     AS stay_id,
    itemid      AS itemid,
    charttime   AS charttime,
    value       AS value,
    -- Static value
    'datetimeevents'                    AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id, -- Generate a UUID-like value
    json_build_object(
        'subject_id', subject_id,
        'hadm_id', hadm_id,
        'stay_id', stay_id,
        'charttime', charttime
    )                                  AS trace_id
FROM
    datetimeevents;

-- -------------------------------------------------------------------
-- src_chartevents
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_chartevents;
CREATE TABLE src_chartevents AS
SELECT
    subject_id  AS subject_id,
    hadm_id     AS hadm_id,
    stay_id     AS stay_id,
    itemid      AS itemid,
    charttime   AS charttime,
    value       AS value,
    valuenum    AS valuenum,
    valueuom    AS valueuom,
    -- Static value
    'chartevents'                       AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id, -- Generate a UUID-like value
    json_build_object(
        'subject_id', subject_id,
        'hadm_id', hadm_id,
        'stay_id', stay_id,
        'charttime', charttime
    )                                  AS trace_id
FROM
    chartevents;

