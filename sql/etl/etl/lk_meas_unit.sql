-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookup tables for cdm_measurement table
--      lk_meas_operator_concept
--      lk_meas_unit_concept
-- 
-- Dependencies: run after 
--      none
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- custom mapping:
--      gcpt_lab_unit_to_concept -> mimiciv_meas_unit
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- lk_meas_operator_concept
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Create lk_meas_operator_concept
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_meas_operator_concept;
CREATE TABLE lk_meas_operator_concept AS
SELECT
    vc.concept_name     AS source_code, -- operator_name
    vc.concept_id       AS target_concept_id -- operator_concept_id
FROM
    voc_concept vc
WHERE
    vc.domain_id = 'Meas Value Operator';

-- -------------------------------------------------------------------
-- Create tmp_meas_unit
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS tmp_meas_unit;
CREATE TABLE tmp_meas_unit AS
SELECT
    vc.concept_code                         AS concept_code,
    vc.vocabulary_id                        AS vocabulary_id,
    vc.domain_id                            AS domain_id,
    vc.concept_id                           AS concept_id,
    ROW_NUMBER() OVER (
        PARTITION BY vc.concept_code
        ORDER BY UPPER(vc.vocabulary_id)
    )                                       AS row_num -- for de-duplication
FROM
    voc_concept vc
WHERE
    vc.vocabulary_id IN ('UCUM', 'mimiciv_meas_unit', 'mimiciv_meas_wf_unit')
    AND vc.domain_id = 'Unit';

-- -------------------------------------------------------------------
-- Create lk_meas_unit_concept
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_meas_unit_concept;
CREATE TABLE lk_meas_unit_concept AS
SELECT
    vc.concept_code         AS source_code,
    vc.vocabulary_id        AS source_vocabulary_id,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    tmp_meas_unit vc
LEFT JOIN
    voc_concept_relationship vcr
        ON vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.invalid_reason IS NULL
WHERE
    vc.row_num = 1;

-- -------------------------------------------------------------------
-- Drop Temporary Table
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS tmp_meas_unit;
