-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookup tables for cdm_measurement table
-- Rule 1
-- Labs from labevents
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      lk_vis_part_1.sql,
--      lk_meas_unit.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- src_labevents: 
--      look closer to fields priority and specimen_id
--      Add 'Maps to value'
-- src_labevents.value: 
--      investigate if there are formatted values with thousand separators,
--      and if we need to use more complicated parsing.
--      see an_labevents_full
--      see a possibility to use 'Maps to value'
-- custom mapping:
--      gcpt_lab_label_to_concept -> mimiciv_meas_lab_loinc
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Rule 1
-- LABS from labevents
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_meas_d_labevents_clean
-- source label for custom mapping: label|fluid|category
-- source code to join vocabulary tables: coalesce(LOINC, itemid)
-- source code represented in cdm tables: itemid
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_meas_d_labitems_clean
-- Clean and transform lab items for mapping
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_meas_d_labitems_clean;
CREATE TABLE lk_meas_d_labitems_clean AS
SELECT
    dlab.itemid                                                 AS itemid, -- Item ID for source value
    COALESCE(dlab.loinc_code, 
        CAST(dlab.itemid AS TEXT))                              AS source_code, -- Source code to join with vocab
    dlab.loinc_code                                             AS loinc_code, -- Crosswalk table
    CONCAT(dlab.label, '|', dlab.fluid, '|', dlab.category)     AS source_label, -- Crosswalk table
    CASE 
        WHEN dlab.loinc_code IS NOT NULL THEN 'LOINC'
        ELSE 'mimiciv_meas_lab_loinc'
    END                                                         AS source_vocabulary_id
FROM
    src_d_labitems dlab;

-- -------------------------------------------------------------------
-- lk_meas_labevents_clean
-- Clean and extract necessary fields from lab events
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_meas_labevents_clean;
CREATE TABLE lk_meas_labevents_clean AS
SELECT
    md5(gen_random_uuid()::TEXT)       AS measurement_id, -- Generate unique measurement ID
    src.subject_id                          AS subject_id,
    src.charttime                           AS start_datetime, -- Measurement timestamp
    src.hadm_id                             AS hadm_id,
    src.itemid                              AS itemid,
    src.value                               AS value, -- Source value
    REGEXP_REPLACE(src.value, '^(\<=|\>=|\>|\<|=|).*', '\1')   AS value_operator, -- Extract operator
    REGEXP_REPLACE(src.value, '[^0-9.-]', '', 'g')             AS value_number, -- Extract numeric value
    NULLIF(TRIM(src.valueuom), '')          AS valueuom, -- Unit of measurement
    src.ref_range_lower                     AS ref_range_lower,
    src.ref_range_upper                     AS ref_range_upper,
    'labevents'                             AS unit_id,
    -- Source metadata
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    src_labevents src
INNER JOIN
    src_d_labitems dlab
        ON src.itemid = dlab.itemid;

-- -------------------------------------------------------------------
-- lk_meas_d_labitems_concept
-- Map lab items to vocabulary concepts
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_meas_d_labitems_concept;
CREATE TABLE lk_meas_d_labitems_concept AS
SELECT
    dlab.itemid                 AS itemid,
    dlab.source_code            AS source_code,
    dlab.loinc_code             AS loinc_code,
    dlab.source_label           AS source_label,
    dlab.source_vocabulary_id   AS source_vocabulary_id,
    -- Source concept
    vc.domain_id                AS source_domain_id,
    vc.concept_id               AS source_concept_id,
    vc.concept_name             AS source_concept_name,
    -- Target concept
    vc2.vocabulary_id           AS target_vocabulary_id,
    vc2.domain_id               AS target_domain_id,
    vc2.concept_id              AS target_concept_id,
    vc2.concept_name            AS target_concept_name,
    vc2.standard_concept        AS target_standard_concept
FROM
    lk_meas_d_labitems_clean dlab
LEFT JOIN
    voc_concept vc
        ON vc.concept_code = dlab.source_code
        AND vc.vocabulary_id = dlab.source_vocabulary_id
LEFT JOIN
    voc_concept_relationship vcr
        ON vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL;

-- -------------------------------------------------------------------
-- lk_meas_labevents_hadm_id
-- Map lab events to hospital admission IDs
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_meas_labevents_hadm_id;
CREATE TABLE lk_meas_labevents_hadm_id AS
SELECT
    src.trace_id                        AS event_trace_id, 
    adm.hadm_id                         AS hadm_id,
    ROW_NUMBER() OVER (
        PARTITION BY src.trace_id::TEXT
        ORDER BY adm.start_datetime
    )                                   AS row_num -- Select the earliest hadm_id
FROM  
    lk_meas_labevents_clean src
INNER JOIN 
    lk_admissions_clean adm
        ON adm.subject_id = src.subject_id
        AND src.start_datetime::TIMESTAMP BETWEEN adm.start_datetime AND adm.end_datetime
WHERE
    src.hadm_id IS NULL;

-- -------------------------------------------------------------------
-- lk_meas_labevents_mapped
-- Map lab events to measurements
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_meas_labevents_mapped;
CREATE TABLE lk_meas_labevents_mapped AS
SELECT
    src.measurement_id                      AS measurement_id,
    src.subject_id                          AS subject_id,
    COALESCE(src.hadm_id, hadm.hadm_id)     AS hadm_id,
    CAST(src.start_datetime AS DATE)        AS date_id,
    src.start_datetime                      AS start_datetime,
    src.itemid                              AS itemid,
    CAST(src.itemid AS TEXT)                AS source_code, -- Convert itemid to text
    labc.source_vocabulary_id               AS source_vocabulary_id,
    labc.source_concept_id                  AS source_concept_id,
    COALESCE(labc.target_domain_id, 'Measurement')  AS target_domain_id,
    labc.target_concept_id                  AS target_concept_id,
    src.valueuom                            AS unit_source_value,
    COALESCE(uc.target_concept_id, 0)       AS unit_concept_id,
    src.value_operator                      AS operator_source_value,
    opc.target_concept_id                   AS operator_concept_id,
    src.value                               AS value_source_value,
    src.value_number                        AS value_as_number,
    NULL                                    AS value_as_concept_id,
    src.ref_range_lower                     AS range_low,
    src.ref_range_upper                     AS range_high,
    -- Metadata
    CONCAT('meas.', src.unit_id)            AS unit_id,
    src.load_table_id                       AS load_table_id,
    src.load_row_id                         AS load_row_id,
    src.trace_id                            AS trace_id
FROM  
    lk_meas_labevents_clean src
INNER JOIN 
    lk_meas_d_labitems_concept labc
        ON labc.itemid = src.itemid
LEFT JOIN 
    lk_meas_operator_concept opc
        ON opc.source_code = src.value_operator
LEFT JOIN 
    lk_meas_unit_concept uc
        ON uc.source_code = src.valueuom
LEFT JOIN 
    lk_meas_labevents_hadm_id hadm
        ON hadm.event_trace_id::TEXT = src.trace_id::TEXT -- Cast to TEXT for comparison
        AND hadm.row_num = 1;