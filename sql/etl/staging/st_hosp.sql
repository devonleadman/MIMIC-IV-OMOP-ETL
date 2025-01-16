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
-- for Condition_occurrence
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_diagnoses_icd
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_diagnoses_icd
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_diagnoses_icd;
CREATE TABLE src_diagnoses_icd AS
SELECT
    subject_id      AS subject_id,
    hadm_id         AS hadm_id,
    seq_num         AS seq_num,
    icd_code        AS icd_code,
    icd_version     AS icd_version,
    -- Static value
    'diagnoses_icd'                     AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id, -- Generate a UUID-like value
    json_build_object(
        'hadm_id', hadm_id,
        'seq_num', seq_num
    )                                  AS trace_id
FROM
    diagnoses_icd;

-- -------------------------------------------------------------------
-- src_services
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_services;
CREATE TABLE src_services AS
SELECT
    subject_id                          AS subject_id,
    hadm_id                             AS hadm_id,
    transfertime                        AS transfertime,
    prev_service                        AS prev_service,
    curr_service                        AS curr_service,
    -- Static value
    'services'                          AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'subject_id', subject_id,
        'hadm_id', hadm_id,
        'transfertime', transfertime
    )                                  AS trace_id
FROM
    services;

-- -------------------------------------------------------------------
-- src_labevents
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_labevents;
CREATE TABLE src_labevents AS
SELECT
    labevent_id                         AS labevent_id,
    subject_id                          AS subject_id,
    charttime                           AS charttime,
    hadm_id                             AS hadm_id,
    itemid                              AS itemid,
    valueuom                            AS valueuom,
    value                               AS value,
    flag                                AS flag,
    ref_range_lower                     AS ref_range_lower,
    ref_range_upper                     AS ref_range_upper,
    -- Static value
    'labevents'                         AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'labevent_id', labevent_id
    )                                  AS trace_id
FROM
    labevents;

-- -------------------------------------------------------------------
-- src_d_labitems
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_d_labitems;
CREATE TABLE src_d_labitems AS
SELECT
    itemid                              AS itemid,
    label                               AS label,
    fluid                               AS fluid,
    category                            AS category,
    NULL                                AS loinc_code, -- Placeholder for missing field
    -- Static value
    'd_labitems'                        AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'itemid', itemid
    )                                  AS trace_id
FROM
    d_labitems;

-- -------------------------------------------------------------------
-- src_procedures_icd
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_procedures_icd;
CREATE TABLE src_procedures_icd AS
SELECT
    subject_id                          AS subject_id,
    hadm_id                             AS hadm_id,
    icd_code                            AS icd_code,
    icd_version                         AS icd_version,
    -- Static value
    'procedures_icd'                    AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'subject_id', subject_id,
        'hadm_id', hadm_id,
        'icd_code', icd_code,
        'icd_version', icd_version
    )                                  AS trace_id
FROM
    procedures_icd;

-- -------------------------------------------------------------------
-- src_hcpcsevents
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_hcpcsevents;
CREATE TABLE src_hcpcsevents AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    hcpcs_cd                            AS hcpcs_cd,
    seq_num                             AS seq_num,
    short_description                   AS short_description,
    -- Static value
    'hcpcsevents'                       AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'subject_id', subject_id,
        'hadm_id', hadm_id,
        'hcpcs_cd', hcpcs_cd,
        'seq_num', seq_num
    )                                  AS trace_id
FROM
    hcpcsevents;

-- -------------------------------------------------------------------
-- src_drgcodes
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_drgcodes;
CREATE TABLE src_drgcodes AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    drg_code                            AS drg_code,
    description                         AS description,
    -- Static value
    'drgcodes'                          AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'subject_id', subject_id,
        'hadm_id', hadm_id,
        'drg_code', COALESCE(drg_code, NULL)
    )                                  AS trace_id
FROM
    drgcodes;

-- -------------------------------------------------------------------
-- src_prescriptions
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_prescriptions;
CREATE TABLE src_prescriptions AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    pharmacy_id                         AS pharmacy_id,
    starttime                           AS starttime,
    stoptime                            AS stoptime,
    drug_type                           AS drug_type,
    drug                                AS drug,
    gsn                                 AS gsn,
    ndc                                 AS ndc,
    prod_strength                       AS prod_strength,
    form_rx                             AS form_rx,
    dose_val_rx                         AS dose_val_rx,
    dose_unit_rx                        AS dose_unit_rx,
    form_val_disp                       AS form_val_disp,
    form_unit_disp                      AS form_unit_disp,
    doses_per_24_hrs                    AS doses_per_24_hrs,
    route                               AS route,
    -- Static value
    'prescriptions'                     AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'subject_id', subject_id,
        'hadm_id', hadm_id,
        'pharmacy_id', pharmacy_id,
        'starttime', starttime
    )                                  AS trace_id
FROM
    prescriptions;

-- -------------------------------------------------------------------
-- src_microbiologyevents
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_microbiologyevents;
CREATE TABLE src_microbiologyevents AS
SELECT
    microevent_id                       AS microevent_id,
    subject_id                          AS subject_id,
    hadm_id                             AS hadm_id,
    chartdate                           AS chartdate,
    charttime                           AS charttime,
    spec_itemid                         AS spec_itemid,
    spec_type_desc                      AS spec_type_desc,
    test_itemid                         AS test_itemid,
    test_name                           AS test_name,
    org_itemid                          AS org_itemid,
    org_name                            AS org_name,
    ab_itemid                           AS ab_itemid,
    ab_name                             AS ab_name,
    dilution_comparison                 AS dilution_comparison,
    dilution_value                      AS dilution_value,
    interpretation                      AS interpretation,
    -- Static value
    'microbiologyevents'                AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'subject_id', subject_id,
        'hadm_id', hadm_id,
        'microevent_id', microevent_id
    )                                  AS trace_id
FROM
    microbiologyevents;

-- -------------------------------------------------------------------
-- src_pharmacy
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_pharmacy;
CREATE TABLE src_pharmacy AS
SELECT
    pharmacy_id                         AS pharmacy_id,
    medication                          AS medication,
    -- Static value
    'pharmacy'                          AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'pharmacy_id', pharmacy_id
    )                                  AS trace_id
FROM
    pharmacy;


