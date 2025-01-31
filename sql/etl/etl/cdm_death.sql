-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_death table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      cdm_person.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_death_adm_mapped
-- Rule 1, admissionss
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Create lk_death_adm_mapped
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_death_adm_mapped;
CREATE TABLE lk_death_adm_mapped AS
SELECT DISTINCT
    src.subject_id, 
    FIRST_VALUE(src.deathtime) OVER (
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC
    )                                   AS deathtime, 
    FIRST_VALUE(src.dischtime) OVER (
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC
    )                                   AS dischtime,
    32817                               AS type_concept_id, -- OMOP4976890 EHR
    'admissions'                        AS unit_id,
    src.load_table_id                   AS load_table_id,
    FIRST_VALUE(src.load_row_id) OVER (
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC
    )                                   AS load_row_id,
    FIRST_VALUE(src.trace_id::TEXT) OVER (
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC
    )                                   AS trace_id
FROM 
    src_admissions src
WHERE 
    src.deathtime IS NOT NULL;

-- -------------------------------------------------------------------
-- Create cdm_death
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS cdm_death;
CREATE TABLE cdm_death
(
    person_id               UUID     NOT NULL,
    death_date              DATE       NOT NULL,
    death_datetime          TIMESTAMP           ,
    death_type_concept_id   BIGINT     NOT NULL,
    cause_concept_id        BIGINT              ,
    cause_source_value      TEXT               ,
    cause_source_concept_id BIGINT              ,
    unit_id                 TEXT,
    load_table_id           TEXT,
    load_row_id             TEXT,
    trace_id                TEXT
);

-- -------------------------------------------------------------------
-- Insert data into cdm_death
-- -------------------------------------------------------------------

INSERT INTO cdm_death
SELECT
    per.person_id                             AS person_id,
    CASE
        WHEN src.deathtime <= src.dischtime THEN src.deathtime::DATE
        ELSE src.dischtime::DATE
    END                                       AS death_date,
    CASE
        WHEN src.deathtime <= src.dischtime THEN src.deathtime
        ELSE src.dischtime
    END                                       AS death_datetime,
    src.type_concept_id                       AS death_type_concept_id,
    0                                         AS cause_concept_id,
    NULL                                      AS cause_source_value,
    0                                         AS cause_source_concept_id,
    CONCAT('death.', src.unit_id)             AS unit_id,
    src.load_table_id                         AS load_table_id,
    src.load_row_id                           AS load_row_id,
    src.trace_id                              AS trace_id
FROM
    lk_death_adm_mapped src
INNER JOIN
    cdm_person per
        ON src.subject_id::TEXT = per.person_source_value;
