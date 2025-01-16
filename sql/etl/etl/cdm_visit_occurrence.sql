-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_visit_occurrence table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      cdm_person.sql,
--      cdm_care_site
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize "create or replace"
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- Using cdm_care_site:
--      care_site_name = 'BIDMC' -- Beth Israel hospital for all
--      (populate with departments)
--
-- Field diagnosis is not found in admissions table.
--      diagnosis is used to set admission/discharge concepts for organ donors
--      use hosp.diagnosis_icd + hosp.d_icd_diagnoses/voc_concept?
--
-- Review logic for organ donors. Concepts used in MIMIC III:
--      4216643 -- DEAD/EXPIRED
--      4022058 -- ORGAN DONOR
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_visit_occurrence
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE cdm_visit_occurrence (
    visit_occurrence_id           BIGINT NOT NULL,
    person_id                     BIGINT NOT NULL,
    visit_concept_id              BIGINT NOT NULL,
    visit_start_date              DATE NOT NULL,
    visit_start_datetime          TIMESTAMP,
    visit_end_date                DATE NOT NULL,
    visit_end_datetime            TIMESTAMP,
    visit_type_concept_id         BIGINT NOT NULL,
    provider_id                   BIGINT,
    care_site_id                  BIGINT,
    visit_source_value            TEXT,
    visit_source_concept_id       BIGINT,
    admitting_source_concept_id   BIGINT,
    admitting_source_value        TEXT,
    discharge_to_concept_id       BIGINT,
    discharge_to_source_value     TEXT,
    preceding_visit_occurrence_id BIGINT,
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   BIGINT,
    trace_id                      TEXT
);

INSERT INTO cdm_visit_occurrence (
    visit_occurrence_id,
    person_id,
    visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    visit_source_concept_id,
    admitting_source_concept_id,
    admitting_source_value,
    discharge_to_concept_id,
    discharge_to_source_value,
    preceding_visit_occurrence_id,
    unit_id,
    load_table_id,
    load_row_id,
    trace_id
)
SELECT
    src.visit_occurrence_id,
    per.person_id,
    COALESCE(lat.target_concept_id, 0) AS visit_concept_id,
    src.start_datetime::DATE AS visit_start_date,
    src.start_datetime AS visit_start_datetime,
    src.end_datetime::DATE AS visit_end_date,
    src.end_datetime AS visit_end_datetime,
    32817 AS visit_type_concept_id, -- EHR Type Concept Standard
    NULL::BIGINT AS provider_id,
    cs.care_site_id,
    src.source_value,
    COALESCE(lat.source_concept_id, 0) AS visit_source_concept_id,
    CASE
        WHEN src.admission_location IS NOT NULL THEN COALESCE(la.target_concept_id, 0)
        ELSE NULL
    END AS admitting_source_concept_id,
    src.admission_location AS admitting_source_value,
    CASE
        WHEN src.discharge_location IS NOT NULL THEN COALESCE(ld.target_concept_id, 0)
        ELSE NULL
    END AS discharge_to_concept_id,
    src.discharge_location AS discharge_to_source_value,
    LAG(src.visit_occurrence_id) OVER (
        PARTITION BY subject_id, hadm_id
        ORDER BY start_datetime
    ) AS preceding_visit_occurrence_id,
    CONCAT('visit.', src.unit_id) AS unit_id,
    src.load_table_id,
    src.load_row_id,
    src.trace_id
FROM 
    lk_visit_clean src
INNER JOIN
    cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
LEFT JOIN 
    lk_visit_concept lat
        ON lat.source_code = src.admission_type
LEFT JOIN 
    lk_visit_concept la 
        ON la.source_code = src.admission_location
LEFT JOIN 
    lk_visit_concept ld
        ON ld.source_code = src.discharge_location
LEFT JOIN 
    cdm_care_site cs
        ON cs.care_site_name = 'BIDMC'; -- Beth Israel hospital for all

