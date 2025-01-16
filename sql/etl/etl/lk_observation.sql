-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookups for cdm_observation table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- loaded custom mapping:
--      gcpt_insurance_to_concept -> mimiciv_obs_insurance
--      gcpt_marital_status_to_concept -> mimiciv_obs_marital
--      gcpt_drgcode_to_concept -> mimiciv_obs_drgcodes
--          source_code = gcpt.description
-- Cost containment drgcode should be in cost table apparently.... 
--      http://forums.ohdsi.org/t/most-appropriate-omop-table-to-house-drg-information/1591/9,
-- observation.proc.* (Achilless Heel report)
--      value_as_string IS NULL AND value_as_number IS NULL AND COALESCE(value_as_concept_id, 0) = 0
--      review custom mapping. if ok, use value_as_concept_id = 4188539 'Yes'?
-- -------------------------------------------------------------------

-- on demo: 1585 rows
-- -------------------------------------------------------------------
-- lk_observation_clean from admissions
-- rules 1-3
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_datetimeevents_concept;
DROP TABLE IF EXISTS lk_proc_event_clean;
DROP TABLE IF EXISTS lk_datetimeevents_clean;

-- -------------------------------------------------------------------
-- lk_hcpcsevents_clean
-- Rule 1, HCPCS mapping
-- -------------------------------------------------------------------

CREATE TABLE lk_hcpcsevents_clean AS
SELECT
    src.subject_id      AS subject_id,
    src.hadm_id         AS hadm_id,
    adm.dischtime       AS start_datetime,
    src.seq_num         AS seq_num, -- procedure_type as in condition_occurrence
    src.hcpcs_cd        AS hcpcs_cd,
    src.short_description AS short_description,
    -- Metadata
    src.load_table_id   AS load_table_id,
    src.load_row_id     AS load_row_id,
    src.trace_id        AS trace_id
FROM
    src_hcpcsevents src
INNER JOIN
    src_admissions adm
        ON src.hadm_id = adm.hadm_id;

-- -------------------------------------------------------------------
-- lk_procedures_icd_clean
-- Rule 2, ICD mapping
-- -------------------------------------------------------------------

CREATE TABLE lk_procedures_icd_clean AS
SELECT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    adm.dischtime                               AS start_datetime,
    src.icd_code                                AS icd_code,
    src.icd_version                             AS icd_version,
    CASE
        WHEN src.icd_version = 9 THEN 'ICD9Proc'
        WHEN src.icd_version = 10 THEN 'ICD10PCS'
        ELSE 'Unknown'
    END                                         AS source_vocabulary_id,
    REPLACE(src.icd_code, '.', '')              AS source_code, -- Normalize for joining
    -- Metadata
    src.load_table_id                           AS load_table_id,
    src.load_row_id                             AS load_row_id,
    src.trace_id                                AS trace_id
FROM
    src_procedures_icd src
INNER JOIN
    src_admissions adm
        ON src.hadm_id = adm.hadm_id;

-- -------------------------------------------------------------------
-- lk_proc_d_items_clean from procedureevents
-- Rule 3, d_items custom mapping
-- -------------------------------------------------------------------

CREATE TABLE lk_proc_d_items_clean AS
SELECT
    src.subject_id                      AS subject_id,
    src.hadm_id                         AS hadm_id,
    src.starttime                       AS start_datetime,
    src.value                           AS quantity, 
    src.itemid                          AS itemid,
    -- Metadata
    'procedureevents'                   AS unit_id,
    src.load_table_id                   AS load_table_id,
    src.load_row_id                     AS load_row_id,
    src.trace_id                        AS trace_id
FROM
    src_procedureevents src
WHERE
    src.cancelreason = 0; -- Exclude cancelled events

-- -------------------------------------------------------------------
-- lk_proc_d_items_clean from datetimeevents
-- Rule 4, d_items custom mapping
-- -------------------------------------------------------------------

INSERT INTO lk_proc_d_items_clean
SELECT
    src.subject_id                      AS subject_id,
    src.hadm_id                         AS hadm_id,
    src.value                           AS start_datetime,
    1                                   AS quantity,
    src.itemid                          AS itemid,
    -- Metadata
    'datetimeevents'                    AS unit_id,
    src.load_table_id                   AS load_table_id,
    src.load_row_id                     AS load_row_id,
    src.trace_id                        AS trace_id
FROM
    src_datetimeevents src
INNER JOIN
    src_patients pat
        ON pat.subject_id = src.subject_id
WHERE
    EXTRACT(YEAR FROM src.value) >= (pat.anchor_year - pat.anchor_age - 1);

-- -------------------------------------------------------------------
-- lk_hcpcs_concept
-- HCPCS Rule 1 mapping
-- -------------------------------------------------------------------

CREATE TABLE lk_hcpcs_concept AS
SELECT
    vc.concept_code         AS source_code,
    vc.vocabulary_id        AS source_vocabulary_id,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    voc_concept vc
LEFT JOIN
    voc_concept_relationship vcr
        ON vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
WHERE
    vc.vocabulary_id IN ('HCPCS', 'CPT4');

-- -------------------------------------------------------------------
-- lk_icd_proc_concept
-- ICD Rule 2 mapping
-- -------------------------------------------------------------------

CREATE TABLE lk_icd_proc_concept AS
SELECT
    REPLACE(vc.concept_code, '.', '')   AS source_code,
    vc.vocabulary_id                    AS source_vocabulary_id,
    vc.domain_id                        AS source_domain_id,
    vc.concept_id                       AS source_concept_id,
    vc2.domain_id                       AS target_domain_id,
    vc2.concept_id                      AS target_concept_id
FROM
    voc_concept vc
LEFT JOIN
    voc_concept_relationship vcr
        ON vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
WHERE
    vc.vocabulary_id IN ('ICD9Proc', 'ICD10PCS');

-- -------------------------------------------------------------------
-- lk_itemid_concept
-- Rule 3 and 4 mapping
-- -------------------------------------------------------------------

CREATE TABLE lk_itemid_concept AS
SELECT
    d_items.itemid                      AS itemid,
    CAST(d_items.itemid AS TEXT)        AS source_code,
    d_items.label                       AS source_label,
    vc.vocabulary_id                    AS source_vocabulary_id,
    vc.domain_id                        AS source_domain_id,
    vc.concept_id                       AS source_concept_id,
    vc2.domain_id                       AS target_domain_id,
    vc2.concept_id                      AS target_concept_id
FROM
    src_d_items d_items
LEFT JOIN
    voc_concept vc
        ON vc.concept_code = CAST(d_items.itemid AS TEXT)
        AND vc.vocabulary_id IN ('mimiciv_proc_itemid', 'mimiciv_proc_datetimeevents')
LEFT JOIN
    voc_concept_relationship vcr
        ON vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
WHERE
    d_items.linksto IN ('procedureevents', 'datetimeevents');

