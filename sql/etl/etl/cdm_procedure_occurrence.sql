-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_procedure_occurrence table
-- 
-- Dependencies: run after 
--      cdm_person.sql,
--      cdm_visit_occurrence,
--      lk_procedure_occurrence
--      lk_meas_specimen
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- cdm_procedure_occurrence
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE cdm_procedure_occurrence
(
    procedure_occurrence_id     BIGINT     NOT NULL,
    person_id                   BIGINT     NOT NULL,
    procedure_concept_id        BIGINT     NOT NULL,
    procedure_date              DATE       NOT NULL,
    procedure_datetime          TIMESTAMP           ,
    procedure_type_concept_id   BIGINT     NOT NULL,
    modifier_concept_id         BIGINT              ,
    quantity                    BIGINT              ,
    provider_id                 BIGINT              ,
    visit_occurrence_id         BIGINT              ,
    visit_detail_id             BIGINT              ,
    procedure_source_value      TEXT               ,
    procedure_source_concept_id BIGINT              ,
    modifier_source_value       TEXT               ,
    unit_id                     TEXT,
    load_table_id               TEXT,
    load_row_id                 BIGINT,
    trace_id                    TEXT
);

-- -------------------------------------------------------------------
-- Rule 1-4: Procedure Mapped
-- -------------------------------------------------------------------

INSERT INTO cdm_procedure_occurrence
SELECT
    FLOOR(RANDOM() * 1e18)::BIGINT                     AS procedure_occurrence_id,
    per.person_id                                      AS person_id,
    src.target_concept_id                              AS procedure_concept_id,
    src.start_datetime::DATE                           AS procedure_date,
    src.start_datetime                                 AS procedure_datetime,
    src.type_concept_id                                AS procedure_type_concept_id,
    0                                                 AS modifier_concept_id,
    src.quantity::BIGINT                               AS quantity,
    NULL                                              AS provider_id,
    vis.visit_occurrence_id                            AS visit_occurrence_id,
    NULL                                              AS visit_detail_id,
    src.source_code                                    AS procedure_source_value,
    src.source_concept_id                              AS procedure_source_concept_id,
    NULL                                              AS modifier_source_value,
    'procedure.' || src.unit_id                       AS unit_id,
    src.load_table_id                                  AS load_table_id,
    src.load_row_id                                    AS load_row_id,
    src.trace_id                                       AS trace_id
FROM
    lk_procedure_mapped src
INNER JOIN
    cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    cdm_visit_occurrence vis
        ON vis.visit_source_value = 
            CONCAT(src.subject_id::TEXT, '|', src.hadm_id::TEXT)
WHERE
    src.target_domain_id = 'Procedure';

-- -------------------------------------------------------------------
-- Rule 5: Observation Mapped (DRG codes)
-- -------------------------------------------------------------------

INSERT INTO cdm_procedure_occurrence
SELECT
    FLOOR(RANDOM() * 1e18)::BIGINT                     AS procedure_occurrence_id,
    per.person_id                                      AS person_id,
    src.target_concept_id                              AS procedure_concept_id,
    src.start_datetime::DATE                           AS procedure_date,
    src.start_datetime                                 AS procedure_datetime,
    src.type_concept_id                                AS procedure_type_concept_id,
    0                                                 AS modifier_concept_id,
    NULL                                              AS quantity,
    NULL                                              AS provider_id,
    vis.visit_occurrence_id                            AS visit_occurrence_id,
    NULL                                              AS visit_detail_id,
    src.source_code                                    AS procedure_source_value,
    src.source_concept_id                              AS procedure_source_concept_id,
    NULL                                              AS modifier_source_value,
    'procedure.' || src.unit_id                       AS unit_id,
    src.load_table_id                                  AS load_table_id,
    src.load_row_id                                    AS load_row_id,
    src.trace_id                                       AS trace_id
FROM
    lk_observation_mapped src
INNER JOIN
    cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    cdm_visit_occurrence vis
        ON vis.visit_source_value = 
            CONCAT(src.subject_id::TEXT, '|', src.hadm_id::TEXT)
WHERE
    src.target_domain_id = 'Procedure';

-- -------------------------------------------------------------------
-- Rule 6: Specimen Mapped
-- -------------------------------------------------------------------

INSERT INTO cdm_procedure_occurrence
SELECT
    FLOOR(RANDOM() * 1e18)::BIGINT                     AS procedure_occurrence_id,
    per.person_id                                      AS person_id,
    src.target_concept_id                              AS procedure_concept_id,
    src.start_datetime::DATE                           AS procedure_date,
    src.start_datetime                                 AS procedure_datetime,
    src.type_concept_id                                AS procedure_type_concept_id,
    0                                                 AS modifier_concept_id,
    NULL                                              AS quantity,
    NULL                                              AS provider_id,
    vis.visit_occurrence_id                            AS visit_occurrence_id,
    NULL                                              AS visit_detail_id,
    src.source_code                                    AS procedure_source_value,
    src.source_concept_id                              AS procedure_source_concept_id,
    NULL                                              AS modifier_source_value,
    'procedure.' || src.unit_id                       AS unit_id,
    src.load_table_id                                  AS load_table_id,
    src.load_row_id                                    AS load_row_id,
    src.trace_id                                       AS trace_id
FROM
    lk_specimen_mapped src
INNER JOIN
    cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    cdm_visit_occurrence vis
        ON vis.visit_source_value = 
            CONCAT(src.subject_id::TEXT, '|', COALESCE(src.hadm_id::TEXT, src.date_id::TEXT))
WHERE
    src.target_domain_id = 'Procedure';

-- -------------------------------------------------------------------
-- Rule 7: Chartevents Mapped
-- -------------------------------------------------------------------

INSERT INTO cdm_procedure_occurrence
SELECT
    FLOOR(RANDOM() * 1e18)::BIGINT                     AS procedure_occurrence_id,
    per.person_id                                      AS person_id,
    src.target_concept_id                              AS procedure_concept_id,
    src.start_datetime::DATE                           AS procedure_date,
    src.start_datetime                                 AS procedure_datetime,
    src.type_concept_id                                AS procedure_type_concept_id,
    0                                                 AS modifier_concept_id,
    NULL                                              AS quantity,
    NULL                                              AS provider_id,
    vis.visit_occurrence_id                            AS visit_occurrence_id,
    NULL                                              AS visit_detail_id,
    src.source_code                                    AS procedure_source_value,
    src.source_concept_id                              AS procedure_source_concept_id,
    NULL                                              AS modifier_source_value,
    'procedure.' || src.unit_id                       AS unit_id,
    src.load_table_id                                  AS load_table_id,
    src.load_row_id                                    AS load_row_id,
    src.trace_id                                       AS trace_id
FROM
    lk_chartevents_mapped src
INNER JOIN
    cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    cdm_visit_occurrence vis
        ON vis.visit_source_value = 
            CONCAT(src.subject_id::TEXT, '|', src.hadm_id::TEXT)
WHERE
    src.target_domain_id = 'Procedure';


