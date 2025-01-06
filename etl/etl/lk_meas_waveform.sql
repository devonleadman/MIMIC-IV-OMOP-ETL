-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate lookups for cdm_measurement table
-- Rule 10 waveforms
-- Dependencies: run after 
--      st_waveform_poc2.sql,
--      lk_meas_unit_concept
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- A draft to apply Wave Forms

-- (Manlik) Regardless of what format we end up with - is to take the meta data and map them into:

-- Procedure
-- [SNOMED.4141651] Continuous ECG monitoring
-- date/time is offset from start of monitor (1/1/1990 if no actual time is given)
-- procedure_source_value = "<wfdb reference ID - ex 3700002_0011>"

-- Device_exposure
-- [SNOMED.45758393] Patient monitoring system module, electrocardiographic
-- device_exposure_start_datetime is the date/time is offset from start of monitor (1/1/1990 if no actual time is given)
-- device_exposure_end_datetime is the start + total duration of the monitor data
-- device_source_value = "<wfdb reference ID - ex 3700002_0011>" 

-- Measurement - heart rate (1 to N), original WFDB reported or generated by our ETL
-- [SNOMED.4239408] Heart rate - units [SNOMED.4118124] bpm
-- date/time is offset from start of monitor (1/1/1990 if no actual time is given)
-- measurement_source_value = "<wfdb reference ID>.<algorithm ID> - ex "3700002_0011.WFDB" or "3700002_0011.CCSIMxv1"

-- Measurement - P-QRS-T derived measurements - aVF R-wave example
-- [LOINC.3022916] R wave amplitude in lead AVF
-- date/time is offset from start of monitor (1/1/1990 if no actual time is given) + segment offset
-- measurement_source_value = "<wfdb reference ID>.<algorithm ID> - ex 3700002_0011.CCSIMxv1"

-- I have ECG measurement map down to lead level

-- The same approach would apply to BP and Respiratory values. 

-- If we derive observations like AFib and Tachycardia - we can further map these 
-- to the condition_occurrence table as [4064452] ECG: atrial fibrillation using the same reference time and source
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Adding a sample of Waveforms to OMOP
-- - visit_detail is the core point "to link them all"
-- - visit_detail refers to storage location
-- - visit_detail stores time of start and end of the measurement
-- - identify the visit: CAST(waveform_source.hadm_id AS STRING) = vis.visit_source_value
-- -------------------------------------------------------------------

-- from meas.chart.4: note gcpt_chart_label_to_concept

-- -------------------------------------------------------------------
-- Use custom mapping:
--      gcpt_meas_waveforms.csv -> mimiciv_meas_wf
--      gcpt_meas_unit.csv      -> mimiciv_meas_wf_unit
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_waveform_clean
-- put together poc_2 and poc_3
-- -------------------------------------------------------------------

-- poc_2

DROP TABLE IF EXISTS `@etl_project`.@etl_dataset.lk_wf_clean;

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_waveform_clean AS
SELECT
    wh.subject_id                           AS subject_id,
    CONCAT(
        src.reference_id, '.', src.segment_name,
        '.', src.source_code
    )                                       AS reference_id, -- add segment name and source code to make the field unique
    IF(
        EXTRACT(YEAR FROM src.mx_datetime) < pat.anchor_year,
        DATETIME(pat.anchor_year, EXTRACT(MONTH FROM src.mx_datetime), EXTRACT(DAY FROM src.mx_datetime),
            EXTRACT(HOUR FROM src.mx_datetime), EXTRACT(MINUTE FROM src.mx_datetime), EXTRACT(SECOND FROM src.mx_datetime)),
        src.mx_datetime
    )                                       AS start_datetime, -- shift date to anchor_year if it is earlier
    src.value_as_number                     AS value_as_number,
    src.source_code                         AS source_code, 
    src.unit_source_value                   AS unit_source_value,
    -- 
    'waveforms.poc_2'                       AS unit_id,
    src.load_table_id                       AS load_table_id,
    src.load_row_id                         AS load_row_id,
    src.trace_id                            AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_waveform_mx src -- wm
INNER JOIN
    `@etl_project`.@etl_dataset.src_waveform_header wh
        ON wh.reference_id = src.reference_id
INNER JOIN
    `@etl_project`.@etl_dataset.src_patients pat
        ON wh.subject_id = pat.subject_id
;

-- poc_3

INSERT INTO `@etl_project`.@etl_dataset.lk_waveform_clean
SELECT
    wh.subject_id                           AS subject_id,
    CONCAT(
        wh.reference_id, '.', 
        COALESCE(src.Visit_Detail___Source, 'Unknown'), '.', 
        CAST(COALESCE(src.Visit_Detail___Start_from_minutes, -1) AS STRING), '.',
        CAST(COALESCE(src.Visit_Detail___Report_minutes, -1) AS STRING), '.', 
        CAST(COALESCE(src.Visit_Detail___Sumarize_minutes, -1) AS STRING), '.', 
        COALESCE(src.Visit_Detail___Method, 'UNKNOWN'), '.', 
        src.source_code
    )                                       AS reference_id, -- make the field unique for Visit_detail_source_value
    src.mx_datetime                         AS start_datetime,
    src.value_as_number                     AS value_as_number,
    src.source_code                         AS source_code, 
    src.unit_source_value                   AS unit_source_value,
    -- 
    'waveforms.poc_3'                       AS unit_id,
    src.load_table_id                       AS load_table_id,
    src.load_row_id                         AS load_row_id,
    src.trace_id                            AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_waveform_mx_3 src -- wm
INNER JOIN
    `@etl_project`.@etl_dataset.src_waveform_header_3 wh
        ON wh.case_id = src.case_id
;

-- -------------------------------------------------------------------
-- lk_wf_hadm_id
-- pick additional hadm_id by event start_datetime
-- row_num is added to select the earliest if more than one hadm_ids are found
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_wf_hadm_id AS
SELECT
    src.trace_id                        AS event_trace_id,
    adm.hadm_id                         AS hadm_id,
    ROW_NUMBER() OVER (
        PARTITION BY src.trace_id
        ORDER BY adm.start_datetime
    )                                   AS row_num
FROM
    `@etl_project`.@etl_dataset.lk_waveform_clean src
INNER JOIN 
    `@etl_project`.@etl_dataset.lk_admissions_clean adm
        ON adm.subject_id = src.subject_id
        AND src.start_datetime BETWEEN adm.start_datetime AND adm.end_datetime
;

-- -------------------------------------------------------------------
-- lk_meas_waveform_mapped
-- Rule 10 (waveform)
-- reference_id = visit_detail_source_value
-- -------------------------------------------------------------------


CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_waveform_mapped AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())       AS measurement_id,
    src.subject_id                          AS subject_id,
    hadm.hadm_id                            AS hadm_id,     -- get hadm_id by datetime period
    src.reference_id                        AS reference_id, -- make field unique for visit_detail_source_value
    COALESCE(vc2.concept_id, 0)             AS target_concept_id,
    COALESCE(vc2.domain_id, 'Measurement')  AS target_domain_id,
    src.start_datetime                      AS start_datetime,
    src.value_as_number                     AS value_as_number,
    IF(src.unit_source_value IS NOT NULL, 
        COALESCE(uc.target_concept_id, 0), NULL)    AS unit_concept_id,
    src.source_code                         AS source_code, 
    COALESCE(vc1.concept_id, 0)             AS source_concept_id,
    src.unit_source_value                   AS unit_source_value,
    -- 
    src.unit_id                             AS unit_id,
    src.load_table_id                       AS load_table_id,
    src.load_row_id                         AS load_row_id,
    src.trace_id                            AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_waveform_clean src
-- mapping of the main source code
-- mapping for measurement unit
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_meas_unit_concept uc
        ON uc.source_code = src.unit_source_value
        -- supposing that the standard mapping is supplemented with custom concepts for waveform specific units
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc1
        ON vc1.concept_code = src.source_code
        AND vc1.vocabulary_id = 'mimiciv_meas_wf'
            -- supposing that the standard mapping is supplemented with custom concepts for waveform specific values
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vr
        ON vc1.concept_id = vr.concept_id_1
        AND vr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
LEFT JOIN 
    `@etl_project`.@etl_dataset.lk_wf_hadm_id hadm
        ON hadm.event_trace_id = src.trace_id
        AND hadm.row_num = 1
;

