-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_condition_occurrence table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      lk_cond_diagnoses.sql,
--      lk_meas_chartevents.sql,
--      cdm_person.sql,
--      cdm_visit_occurrence.sql
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------

-- 4,520 rows on demo

-- -------------------------------------------------------------------
-- cdm_condition_occurrence
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE cdm_condition_occurrence
(
    condition_occurrence_id       BIGINT     NOT NULL,
    person_id                     BIGINT     NOT NULL,
    condition_concept_id          BIGINT     NOT NULL,
    condition_start_date          DATE       NOT NULL,
    condition_start_datetime      TIMESTAMP           ,
    condition_end_date            DATE                ,
    condition_end_datetime        TIMESTAMP           ,
    condition_type_concept_id     BIGINT     NOT NULL,
    stop_reason                   TEXT                ,
    provider_id                   BIGINT              ,
    visit_occurrence_id           BIGINT              ,
    visit_detail_id               BIGINT              ,
    condition_source_value        TEXT                ,
    condition_source_concept_id   BIGINT              ,
    condition_status_source_value TEXT                ,
    condition_status_concept_id   BIGINT              ,
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   BIGINT,
    trace_id                      TEXT
);

-- -------------------------------------------------------------------
-- Rule 1: Diagnoses
-- -------------------------------------------------------------------

INSERT INTO cdm_condition_occurrence
SELECT
    FLOOR(RANDOM() * 1e18)::BIGINT             AS condition_occurrence_id,
    per.person_id                             AS person_id,
    COALESCE(src.target_concept_id, 0)        AS condition_concept_id,
    src.start_datetime::DATE                  AS condition_start_date,
    src.start_datetime                        AS condition_start_datetime,
    src.end_datetime::DATE                    AS condition_end_date,
    src.end_datetime                          AS condition_end_datetime,
    src.type_concept_id                       AS condition_type_concept_id,
    NULL                                      AS stop_reason,
    NULL                                      AS provider_id,
    vis.visit_occurrence_id                   AS visit_occurrence_id,
    NULL                                      AS visit_detail_id,
    src.source_code                           AS condition_source_value,
    COALESCE(src.source_concept_id, 0)        AS condition_source_concept_id,
    NULL                                      AS condition_status_source_value,
    NULL                                      AS condition_status_concept_id,
    'condition.' || src.unit_id               AS unit_id,
    src.load_table_id                         AS load_table_id,
    src.load_row_id                           AS load_row_id,
    src.trace_id                              AS trace_id
FROM
    lk_diagnoses_icd_mapped src
INNER JOIN
    cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    cdm_visit_occurrence vis
        ON vis.visit_source_value = CONCAT(src.subject_id::TEXT, '|', src.hadm_id::TEXT)
WHERE
    src.target_domain_id = 'Condition';

-- -------------------------------------------------------------------
-- Rule 2: Chartevents.value
-- -------------------------------------------------------------------

INSERT INTO cdm_condition_occurrence
SELECT
    FLOOR(RANDOM() * 1e18)::BIGINT             AS condition_occurrence_id,
    per.person_id                             AS person_id,
    COALESCE(src.target_concept_id, 0)        AS condition_concept_id,
    src.start_datetime::DATE                  AS condition_start_date,
    src.start_datetime                        AS condition_start_datetime,
    src.start_datetime::DATE                  AS condition_end_date,
    src.start_datetime                        AS condition_end_datetime,
    32817                                     AS condition_type_concept_id, -- EHR Type Concept
    NULL                                      AS stop_reason,
    NULL                                      AS provider_id,
    vis.visit_occurrence_id                   AS visit_occurrence_id,
    NULL                                      AS visit_detail_id,
    src.source_code                           AS condition_source_value,
    COALESCE(src.source_concept_id, 0)        AS condition_source_concept_id,
    NULL                                      AS condition_status_source_value,
    NULL                                      AS condition_status_concept_id,
    'condition.' || src.unit_id               AS unit_id,
    src.load_table_id                         AS load_table_id,
    src.load_row_id                           AS load_row_id,
    src.trace_id                              AS trace_id
FROM
    lk_chartevents_condition_mapped src
INNER JOIN
    cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    cdm_visit_occurrence vis
        ON vis.visit_source_value = CONCAT(src.subject_id::TEXT, '|', src.hadm_id::TEXT)
WHERE
    src.target_domain_id = 'Condition';

-- -------------------------------------------------------------------
-- Rule 3: Chartevents
-- -------------------------------------------------------------------

INSERT INTO cdm_condition_occurrence
SELECT
    FLOOR(RANDOM() * 1e18)::BIGINT             AS condition_occurrence_id,
    per.person_id                             AS person_id,
    COALESCE(src.target_concept_id, 0)        AS condition_concept_id,
    src.start_datetime::DATE                  AS condition_start_date,
    src.start_datetime                        AS condition_start_datetime,
    src.start_datetime::DATE                  AS condition_end_date,
    src.start_datetime                        AS condition_end_datetime,
    src.type_concept_id                       AS condition_type_concept_id,
    NULL                                      AS stop_reason,
    NULL                                      AS provider_id,
    vis.visit_occurrence_id                   AS visit_occurrence_id,
    NULL                                      AS visit_detail_id,
    src.source_code                           AS condition_source_value,
    COALESCE(src.source_concept_id, 0)        AS condition_source_concept_id,
    NULL                                      AS condition_status_source_value,
    NULL                                      AS condition_status_concept_id,
    'condition.' || src.unit_id               AS unit_id,
    src.load_table_id                         AS load_table_id,
    src.load_row_id                           AS load_row_id,
    src.trace_id                              AS trace_id
FROM
    lk_chartevents_mapped src
INNER JOIN
    cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    cdm_visit_occurrence vis
        ON vis.visit_source_value = CONCAT(src.subject_id::TEXT, '|', src.hadm_id::TEXT)
WHERE
    src.target_domain_id = 'Condition';



