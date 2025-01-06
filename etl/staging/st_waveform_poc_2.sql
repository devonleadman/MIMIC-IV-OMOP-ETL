-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- dependency, run after:
--      st_core.sql
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

-- If we derive observations like AFib and Tachycardia - we can further map these to the condition_occurrence table as [4064452] ECG: atrial fibrillation using the same reference time and source
--
-- parsed codes targeted to clinical findings, target cdm table - cdm_condition_occurrence
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- open points:
--      parse XML to create src_* or raw_* tables
--
-- POC source table:
/*
 bq --location=US load --replace --source_format=CSV  --allow_quoted_newlines=True --skip_leading_rows=1 --autodetect waveform_source_poc.raw_case055_ecg_lines3 z_more/raw_case055_ecg_lines3.csv
*/
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- staging tables
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_waveform_header
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.src_waveform_header
(       
    reference_id            STRING,
    raw_files_path          STRING,
    case_id                 INT64,
    subject_id              INT64,
    start_datetime          DATETIME,
    end_datetime            DATETIME,
    --
    load_table_id           STRING,
    load_row_id             INT64,
    trace_id                STRING
);

-- parsed codes to be targeted to table cdm_measurement

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.src_waveform_mx
(
    case_id                 INT64,  -- FK to the header
    segment_name            STRING, -- two digits of case_id, 5 digits of internal sequence number
    subject_id              INT64,  -- patient's id
    reference_id            STRING, -- file name without extension
    mx_datetime             DATETIME, -- time of measurement
    source_code             STRING,   -- type of measurement
    value_as_number         FLOAT64,
    unit_source_value       STRING, -- measurement unit "BPM", "MS", "UV" (microvolt) etc.
                                    -- map these labels and populate unit_concept_id
    --
    load_table_id           STRING,
    load_row_id             INT64,
    trace_id                STRING
);


-- parse xml from Manlik? -> src_waveform
-- src_waveform -> visit_detail (visit_detail_source_value = <reference ID>)

-- finding the visit 
-- create visit_detail
-- create measurement -> link visit_detail using visit_detail_source_value = meas_source_value 
-- (start with Manlik's proposal)


-- -------------------------------------------------------------------
-- insert sample data
-- -------------------------------------------------------------------


INSERT INTO `@etl_project`.@etl_dataset.src_waveform_header
SELECT
    subj.short_reference_id             AS reference_id,
    subj.long_reference_id              AS raw_files_path,
    subj.case_id                        AS case_id,
    subj.subject_id                     AS subject_id,
    CAST(src.start_datetime AS DATETIME)    AS start_datetime,
    CAST(src.end_datetime AS DATETIME)      AS end_datetime,
    --
    'wf_header'                         AS load_table_id,
    0                                   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subj.subject_id AS subject_id,
        subj.short_reference_id AS reference_id
    ))                                  AS trace_id
FROM
    `@wf_project`.@wf_dataset.wf_header subj
INNER JOIN
    (
        SELECT 
            case_id, 
            MIN(date_time) AS start_datetime,
            MAX(date_time) AS end_datetime 
        FROM `@wf_project`.@wf_dataset.wf_details
        GROUP BY case_id
    ) src
        ON src.case_id = subj.case_id
;


INSERT INTO `@etl_project`.@etl_dataset.src_waveform_mx
SELECT
    src.case_id                         AS case_id, -- FK to the header
    CAST(src.segment_name AS STRING)    AS segment_name,
    subj.subject_id                     AS subject_id,
    subj.short_reference_id             AS reference_id,
    CAST(src.date_time AS DATETIME)     AS mx_datetime,
    src.src_name                        AS source_code,
    CAST(src.value AS FLOAT64)          AS value_as_number,
    unit_concept_name                   AS unit_source_value,
    --
    'wf_details' load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
            src.case_id AS case_id,
            CAST(src.date_time AS STRING) AS date_time,
            src.src_name AS src_name
        )) AS trace_id -- 
FROM
    `@wf_project`.@wf_dataset.wf_details src
INNER JOIN
    `@wf_project`.@wf_dataset.wf_header subj
        ON src.case_id = subj.case_id
;
