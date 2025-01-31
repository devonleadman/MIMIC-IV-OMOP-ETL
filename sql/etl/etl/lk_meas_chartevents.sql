-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookup tables for cdm_measurement table
-- Rule 2
-- Labs from chartevents
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      st_icu.sql,
--      lk_meas_units
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- to add more measurements from chartevents (see table d_items and analysis for custom mapping)
-- 
-- custom mapping:
--      brand new vocabulary -> mimiciv_meas_chartevents_value -- mapped partially
--          src_chartevents.value = measurement.value_source_value -> value_as_concept_id
-- 
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Rule 2
-- chartevents
-- chartevents keeps all possible records about events during an ICU stay
-- it repeats labs, but in case of discrepancies labs is the final truth
-- both HR and RR obviously do not repeat labs
-- Saturation probably repeats labs. Compare to Lab with source_value = "Oxygen Saturation | 50817"
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_chartevents_clean
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_chartevents_clean;
CREATE TABLE lk_chartevents_clean AS
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    src.stay_id                     AS stay_id,
    src.itemid                      AS itemid,
    CAST(src.itemid AS TEXT)        AS source_code,
    di.label                        AS source_label,
    src.charttime                   AS start_datetime,
    TRIM(src.value)                 AS value,
    CASE 
        WHEN TRIM(src.value) ~ '^[-]?\\d+[.]?\\d*[ ]*[a-z]+$'  -- Use `~` for regex match as BOOLEAN
        THEN CAST(REGEXP_REPLACE(src.value, '[^\\d.-]', '', 'g') AS DOUBLE PRECISION)
        ELSE src.valuenum
    END                             AS valuenum,
    CASE
        WHEN TRIM(src.value) ~ '^[-]?\\d+[.]?\\d*[ ]*[a-z]+$'  -- Use `~` for regex match as BOOLEAN
        THEN REGEXP_REPLACE(src.value, '[^a-z]', '', 'g')
        ELSE src.valueuom
    END                             AS valueuom,
    -- 
    'chartevents'           AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    src_chartevents src
INNER JOIN
    src_d_items di
        ON src.itemid = di.itemid
WHERE
    di.label NOT LIKE '%Temperature'
    OR (di.label LIKE '%Temperature' 
        AND (CASE 
                 WHEN src.valueuom LIKE '%F%' THEN (src.valuenum - 32) * 5 / 9
                 ELSE src.valuenum
             END BETWEEN 25 AND 44)
    );

-- -------------------------------------------------------------------
-- tmp_chartevents_code_dist
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS tmp_chartevents_code_dist;
CREATE TABLE tmp_chartevents_code_dist AS
-- source codes to be mapped
SELECT
    itemid                      AS itemid,
    source_code                 AS source_code,
    source_label                AS source_label,
    'mimiciv_meas_chart'        AS source_vocabulary_id,
    COUNT(*)                    AS row_count
FROM
    lk_chartevents_clean
GROUP BY
    itemid, source_code, source_label
UNION ALL
-- values to be mapped
SELECT
    NULL                        AS itemid,
    value                       AS source_code,
    value                       AS source_label,
    'mimiciv_meas_chartevents_value' AS source_vocabulary_id,
    COUNT(*)                    AS row_count
FROM
    lk_chartevents_clean
GROUP BY
    value; -- Include value in GROUP BY

-- -------------------------------------------------------------------
-- lk_chartevents_concept
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_chartevents_concept;
CREATE TABLE lk_chartevents_concept AS
SELECT
    src.itemid                  AS itemid,
    src.source_code             AS source_code,
    src.source_label            AS source_label,
    src.source_vocabulary_id    AS source_vocabulary_id,
    -- source concept
    vc.domain_id                AS source_domain_id,
    vc.concept_id               AS source_concept_id,
    -- target concept
    vc2.domain_id               AS target_domain_id,
    vc2.concept_id              AS target_concept_id,
    src.row_count               AS row_count
FROM
    tmp_chartevents_code_dist src
LEFT JOIN
    voc_concept vc
        ON vc.concept_code = src.source_code
        AND vc.vocabulary_id = src.source_vocabulary_id
LEFT JOIN
    voc_concept_relationship vcr
        ON vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL;

DROP TABLE IF EXISTS tmp_chartevents_code_dist;

-- -------------------------------------------------------------------
-- lk_chartevents_mapped
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_chartevents_mapped;
CREATE TABLE lk_chartevents_mapped AS
SELECT
    md5(gen_random_uuid()::TEXT)  AS measurement_id, -- Use gen_random_uuid()
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.stay_id                                 AS stay_id,
    src.start_datetime                          AS start_datetime,
    32817                                       AS type_concept_id,  -- OMOP4976890 EHR
    src.itemid                                  AS itemid,
    src.source_code                             AS source_code,
    src.source_label                            AS source_label,
    c_main.source_vocabulary_id                 AS source_vocabulary_id,
    c_main.source_domain_id                     AS source_domain_id,
    c_main.source_concept_id                    AS source_concept_id,
    c_main.target_domain_id                     AS target_domain_id,
    c_main.target_concept_id                    AS target_concept_id,
    src.value                                   AS value_source_value,
    CASE
        WHEN src.valuenum IS NULL THEN COALESCE(c_value.target_concept_id, 0)
        ELSE NULL
    END                                         AS value_as_concept_id,
    src.valuenum                                AS value_as_number,
    src.valueuom                                AS unit_source_value,
    CASE 
        WHEN src.valueuom IS NOT NULL THEN COALESCE(uc.target_concept_id, 0)
        ELSE NULL
    END                                         AS unit_concept_id,
    -- 
    CONCAT('meas.', src.unit_id)                AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    lk_chartevents_clean src
LEFT JOIN
    lk_chartevents_concept c_main
        ON c_main.source_code = src.source_code 
        AND c_main.source_vocabulary_id = 'mimiciv_meas_chart'
LEFT JOIN
    lk_chartevents_concept c_value
        ON c_value.source_code = src.value
        AND c_value.source_vocabulary_id = 'mimiciv_meas_chartevents_value'
        AND c_value.target_domain_id = 'Meas Value'
LEFT JOIN 
    lk_meas_unit_concept uc
        ON uc.source_code = src.valueuom;

-- -------------------------------------------------------------------
-- lk_chartevents_condition_mapped
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_chartevents_condition_mapped;
CREATE TABLE lk_chartevents_condition_mapped AS
SELECT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.stay_id                                 AS stay_id,
    src.start_datetime                          AS start_datetime,
    src.value                                   AS source_code,
    c_main.source_vocabulary_id                 AS source_vocabulary_id,
    c_main.source_concept_id                    AS source_concept_id,
    c_main.target_domain_id                     AS target_domain_id,
    c_main.target_concept_id                    AS target_concept_id,
    -- 
    CONCAT('cond.', src.unit_id)                AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    lk_chartevents_clean src
INNER JOIN
    lk_chartevents_concept c_main
        ON c_main.source_code = src.value
        AND c_main.source_vocabulary_id = 'mimiciv_meas_chartevents_value'
        AND c_main.target_domain_id = 'Condition';


