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
-- 
-- 3 chunks from a trending data CSV file, and from a summarized CSV file
--      
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- open points:
--      parse XML to create src_* or raw_* tables
--
-- POC source tables:
/*
    created from trending data and summarized data in source csv files
    case_id = subject_id, case_id is string
*/
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- staging tables
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_waveform_header
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Create src_waveform_header_3 table
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_waveform_header_3;
CREATE TABLE src_waveform_header_3
(       
    reference_id            TEXT,
    raw_files_path          TEXT,
    case_id                 TEXT,
    subject_id              BIGINT,
    start_datetime          TIMESTAMP,
    end_datetime            TIMESTAMP,
    load_table_id           TEXT,
    load_row_id             BIGINT,
    trace_id                TEXT
);

-- -------------------------------------------------------------------
-- Create src_waveform_mx_3 table
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS src_waveform_mx_3;
CREATE TABLE src_waveform_mx_3
(
    case_id                 TEXT,  -- FK to the header
    segment_name            TEXT,  -- two digits of case_id, 5 digits of internal sequence number
    mx_datetime             TIMESTAMP, -- time of measurement
    source_code             TEXT,      -- type of measurement
    value_as_number         DOUBLE PRECISION,
    unit_source_value       TEXT,      -- measurement unit
    Visit_Detail___Source               TEXT,
    Visit_Detail___Start_from_minutes   BIGINT,
    Visit_Detail___Report_minutes       BIGINT,
    Visit_Detail___Sumarize_minutes     BIGINT,
    Visit_Detail___Method               TEXT,
    load_table_id           TEXT,
    load_row_id             BIGINT,
    trace_id                TEXT
);

-- -------------------------------------------------------------------
-- Insert sample data into src_waveform_header_3
-- -------------------------------------------------------------------

INSERT INTO src_waveform_header_3
SELECT
    subj.short_reference_id             AS reference_id,
    subj.long_reference_id              AS raw_files_path,
    subj.case_id                        AS case_id,
    CAST(REPLACE(subj.case_id, 'p', '') AS BIGINT) AS subject_id,
    subj.start_datetime::TIMESTAMP      AS start_datetime,
    subj.end_datetime::TIMESTAMP        AS end_datetime,
    'poc_3_header'                      AS load_table_id,
    0                                   AS load_row_id,
    json_build_object(
        'case_id', subj.case_id,
        'reference_id', subj.short_reference_id
    )::TEXT                             AS trace_id
FROM
    poc_3_header subj;

-- -------------------------------------------------------------------
-- Chunk 1: Insert sample data into src_waveform_mx_3
-- -------------------------------------------------------------------

INSERT INTO src_waveform_mx_3
SELECT
    src.case_id                         AS case_id, -- FK to the header
    src.segment_name                    AS segment_name,
    src.date_time::TIMESTAMP            AS mx_datetime,
    src.src_name                        AS source_code,
    src.value::DOUBLE PRECISION         AS value_as_number,
    src.unit_concept_name               AS unit_source_value,
    'csv'                               AS Visit_Detail___Source,
    NULL                                AS Visit_Detail___Start_from_minutes,
    NULL                                AS Visit_Detail___Report_minutes,
    NULL                                AS Visit_Detail___Sumarize_minutes,
    'NONE'                              AS Visit_Detail___Method,
    'poc_3_chunk_1'                     AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'case_id', src.case_id,
        'date_time', src.date_time::TEXT,
        'src_name', src.src_name
    )::TEXT                             AS trace_id
FROM
    poc_3_chunk_1 src
INNER JOIN
    src_patients pat
        ON CAST(REPLACE(src.case_id, 'p', '') AS BIGINT) = pat.subject_id;

-- -------------------------------------------------------------------
-- Chunk 2: Insert summarized data for Full set and Demo into src_waveform_mx_3
-- -------------------------------------------------------------------

INSERT INTO src_waveform_mx_3
SELECT
    src.case_id                         AS case_id, -- FK to the header
    src.segment_name                    AS segment_name,
    src.date_time::TIMESTAMP            AS mx_datetime,
    src.src_name                        AS source_code,
    src.value::DOUBLE PRECISION         AS value_as_number,
    src.unit_concept_name               AS unit_source_value,
    src.Visit_Detail___Source           AS Visit_Detail___Source,
    src.Visit_Detail___Start_from_minutes AS Visit_Detail___Start_from_minutes,
    src.Visit_Detail___Report_minutes   AS Visit_Detail___Report_minutes,
    src.Visit_Detail___Sumarize_minutes AS Visit_Detail___Sumarize_minutes,
    src.Visit_Detail___Method           AS Visit_Detail___Method,
    'poc_3_chunk_2'                     AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'case_id', src.case_id,
        'date_time', src.date_time::TEXT,
        'src_name', src.src_name
    )::TEXT                             AS trace_id
FROM
    poc_3_chunk_2 src;

-- -------------------------------------------------------------------
-- Chunk 3: Insert tiny mass data for Demo into src_waveform_mx_3
-- -------------------------------------------------------------------

INSERT INTO src_waveform_mx_3
SELECT
    src.case_id                         AS case_id, -- FK to the header
    src.segment_name                    AS segment_name,
    src.date_time::TIMESTAMP            AS mx_datetime,
    src.src_name                        AS source_code,
    src.value::DOUBLE PRECISION         AS value_as_number,
    src.unit_concept_name               AS unit_source_value,
    src.Visit_Detail___Source           AS Visit_Detail___Source,
    src.Visit_Detail___Start_from_minutes AS Visit_Detail___Start_from_minutes,
    src.Visit_Detail___Report_minutes   AS Visit_Detail___Report_minutes,
    src.Visit_Detail___Sumarize_minutes AS Visit_Detail___Sumarize_minutes,
    src.Visit_Detail___Method           AS Visit_Detail___Method,
    'poc_3_chunk_3'                     AS load_table_id,
    md5(random()::text || clock_timestamp()::text) AS load_row_id,
    json_build_object(
        'case_id', src.case_id,
        'date_time', src.date_time::TEXT,
        'src_name', src.src_name
    )::TEXT                             AS trace_id
FROM
    poc_3_chunk_3 src;



