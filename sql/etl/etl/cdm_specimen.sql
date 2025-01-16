-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_specimen table
-- 
-- Dependencies: run after 
--      cdm_person.sql,
--      lk_meas_specimen.sql
--      
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Rule 1 specimen from microbiology
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_specimen
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE cdm_specimen
(
    specimen_id                 BIGINT     NOT NULL,
    person_id                   BIGINT     NOT NULL,
    specimen_concept_id         BIGINT     NOT NULL,
    specimen_type_concept_id    BIGINT     NOT NULL,
    specimen_date               DATE       NOT NULL,
    specimen_datetime           TIMESTAMP           ,
    quantity                    DOUBLE PRECISION     ,
    unit_concept_id             BIGINT              ,
    anatomic_site_concept_id    BIGINT              ,
    disease_status_concept_id   BIGINT              ,
    specimen_source_id          TEXT                ,
    specimen_source_value       TEXT                ,
    unit_source_value           TEXT                ,
    anatomic_site_source_value  TEXT                ,
    disease_status_source_value TEXT                ,
    unit_id                     TEXT,
    load_table_id               TEXT,
    load_row_id                 BIGINT,
    trace_id                    TEXT
);

INSERT INTO cdm_specimen
SELECT
    src.specimen_id                             AS specimen_id,
    per.person_id                               AS person_id,
    COALESCE(src.target_concept_id, 0)          AS specimen_concept_id,
    32856                                       AS specimen_type_concept_id, -- OMOP4976929 Lab
    src.start_datetime::DATE                    AS specimen_date,
    src.start_datetime                          AS specimen_datetime,
    NULL::DOUBLE PRECISION                      AS quantity,
    NULL::BIGINT                                AS unit_concept_id,
    0                                           AS anatomic_site_concept_id,
    0                                           AS disease_status_concept_id,
    src.trace_id                                AS specimen_source_id,
    src.source_code                             AS specimen_source_value,
    NULL::TEXT                                  AS unit_source_value,
    NULL::TEXT                                  AS anatomic_site_source_value,
    NULL::TEXT                                  AS disease_status_source_value,
    CONCAT('specimen.', src.unit_id)            AS unit_id,
    src.load_table_id                           AS load_table_id,
    src.load_row_id                             AS load_row_id,
    src.trace_id                                AS trace_id
FROM
    lk_specimen_mapped src
INNER JOIN
    cdm_person per
        ON src.subject_id::TEXT = per.person_source_value
WHERE
    src.target_domain_id = 'Specimen';

