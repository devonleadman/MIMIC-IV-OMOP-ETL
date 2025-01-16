-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookups for cdm_procedure_occurrence table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- remove dots from ICD codes
-- loaded custom mapping:
--      gcpt_procedure_to_concept -> mimiciv_proc_itemid
--      gcpt_datetimeevents_to_concept -> mimiciv_proc_datetimeevents
-- to review "custom mapping" from d_icd_procedures: domain_id = 'd_icd_procedures' AND vocabulary_id = 'MIMIC Local Codes'
-- to review relationship_id IN ('CPT4 - SNOMED eq','Maps to')
-- datetimeevents: to summarize count of duplicated rows or to use charttime instead of value?
-- Rule 1
--      add more custom mapping: gcpt_cpt4_to_concept --> mimiciv_proc_xxx (?)
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
    src.load_row_id


