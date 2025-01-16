-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_person table
-- 
-- Dependencies: run after st_core.sql
-- on Demo: 12.4 sec
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize "create or replace"
-- cdm_person;
--
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- loaded custom mapping: 
--      gcpt_ethnicity_to_concept -> mimiciv_per_ethnicity
--
-- Why don't we want to use subject_id as person_id and hadm_id as visit_occurrence_id?
--      ask analysts
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- tmp_subject_ethnicity
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Create tmp_subject_ethnicity
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS tmp_subject_ethnicity;
CREATE TABLE tmp_subject_ethnicity AS
SELECT DISTINCT
    src.subject_id                      AS subject_id,
    FIRST_VALUE(src.ethnicity) OVER (
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC)     AS ethnicity_first
FROM
    src_admissions src;

-- -------------------------------------------------------------------
-- Create lk_pat_ethnicity_concept
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_pat_ethnicity_concept;
CREATE TABLE lk_pat_ethnicity_concept AS
SELECT DISTINCT
    src.ethnicity_first     AS source_code,
    vc.concept_id           AS source_concept_id,
    vc.vocabulary_id        AS source_vocabulary_id,
    vc1.concept_id          AS target_concept_id,
    vc1.vocabulary_id       AS target_vocabulary_id -- look here to distinguish Race and Ethnicity
FROM
    tmp_subject_ethnicity src
LEFT JOIN
    voc_concept vc
        ON UPPER(vc.concept_code) = UPPER(src.ethnicity_first) -- do the custom mapping
        AND vc.domain_id IN ('Race', 'Ethnicity')
LEFT JOIN
    voc_concept_relationship cr1
        ON cr1.concept_id_1 = vc.concept_id
        AND cr1.relationship_id = 'Maps to'
LEFT JOIN
    voc_concept vc1
        ON cr1.concept_id_2 = vc1.concept_id
        AND vc1.invalid_reason IS NULL
        AND vc1.standard_concept = 'S';

-- -------------------------------------------------------------------
-- Create cdm_person
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS cdm_person;
CREATE TABLE cdm_person
(
    person_id                   UUID     NOT NULL,
    gender_concept_id           BIGINT     NOT NULL,
    year_of_birth               BIGINT     NOT NULL,
    month_of_birth              BIGINT             ,
    day_of_birth                BIGINT             ,
    birth_datetime              TIMESTAMP          ,
    race_concept_id             BIGINT     NOT NULL,
    ethnicity_concept_id        BIGINT     NOT NULL,
    location_id                 BIGINT             ,
    provider_id                 BIGINT             ,
    care_site_id                BIGINT             ,
    person_source_value         TEXT               ,
    gender_source_value         TEXT               ,
    gender_source_concept_id    BIGINT             ,
    race_source_value           TEXT               ,
    race_source_concept_id      BIGINT             ,
    ethnicity_source_value      TEXT               ,
    ethnicity_source_concept_id BIGINT             ,
    unit_id                     TEXT,
    load_table_id               TEXT,
    load_row_id                 BIGINT,
    trace_id                    TEXT
);

-- -------------------------------------------------------------------
-- Insert data into cdm_person
-- -------------------------------------------------------------------

INSERT INTO cdm_person
SELECT
    md5(random()::text || clock_timestamp()::text)::uuid AS person_id, -- Generate UUID-like value
    CASE 
        WHEN p.gender = 'F' THEN 8532 -- FEMALE
        WHEN p.gender = 'M' THEN 8507 -- MALE
        ELSE 0 
    END                             AS gender_concept_id,
    p.anchor_year                   AS year_of_birth,
    NULL                            AS month_of_birth,
    NULL                            AS day_of_birth,
    NULL::TIMESTAMP                 AS birth_datetime,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
                THEN map_eth.target_concept_id
            ELSE NULL
        END, 0)                               AS race_concept_id,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id = 'Ethnicity'
                THEN map_eth.target_concept_id
            ELSE NULL
        END, 0)                     AS ethnicity_concept_id,
    NULL                            AS location_id,
    NULL                            AS provider_id,
    NULL                            AS care_site_id,
    CAST(p.subject_id AS TEXT)      AS person_source_value,
    p.gender                        AS gender_source_value,
    0                               AS gender_source_concept_id,
    CASE
        WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
            THEN eth.ethnicity_first
        ELSE NULL
    END                             AS race_source_value,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
                THEN map_eth.source_concept_id
            ELSE NULL
        END, 0)                        AS race_source_concept_id,
    CASE
        WHEN map_eth.target_vocabulary_id = 'Ethnicity'
            THEN eth.ethnicity_first
        ELSE NULL
    END                             AS ethnicity_source_value,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id = 'Ethnicity'
                THEN map_eth.source_concept_id
            ELSE NULL
        END, 0)                     AS ethnicity_source_concept_id,
    'person.patients'               AS unit_id,
    p.load_table_id::TEXT         AS load_table_id,
    p.load_row_id::TEXT             AS load_row_id,
    p.trace_id                      AS trace_id
FROM 
    src_patients p
LEFT JOIN 
    tmp_subject_ethnicity eth 
        ON p.subject_id = eth.subject_id
LEFT JOIN
    lk_pat_ethnicity_concept map_eth
        ON eth.ethnicity_first = map_eth.source_code;

-- -------------------------------------------------------------------
-- Cleanup
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS tmp_subject_ethnicity;
