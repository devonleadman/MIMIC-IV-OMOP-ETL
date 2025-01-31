-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookup tables for cdm_specimen and cdm_measurement tables
-- 
-- Dependencies: run after 
--      st_hosp
--      
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- microbiology custom mapping:
--      gcpt_microbiology_specimen_to_concept -> mimiciv_micro_specimen -- loaded
--          d_micro.label = mbe.spec_type_desc -> source_code
--      (gcpt) brand new vocab -> mimiciv_micro_microtest -- loaded
--          d_micro.label = mbe.test_name -> source_code
--      gcpt_org_name_to_concept -> mimiciv_micro_organism -- loaded
--          d_micro.label = mbe.org_name -> source_code
--      gcpt_atb_to_concept -> mimiciv_micro_antibiotic -- loaded
--          d_micro.label = mbe.ab_name -> source_code
--          https://athena.ohdsi.org/search-terms/terms?domain=Measurement&conceptClass=Lab+Test&page=1&pageSize=15&query=susceptibility 
--      (gcpt) brand new vocab -> mimiciv_micro_resistance -- loaded
--        src_microbiologyevents.interpretation -> source_code
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_micro_cross_ref
-- group microevent_id = trace_id for each type of records
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_micro_cross_ref
-- Cross-references microbiology events for specimen, organisms, and antibiotics
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_micro_cross_ref;
CREATE TABLE lk_micro_cross_ref AS
SELECT
    trace_id::TEXT                           AS trace_id_ab, -- Cast to TEXT for antibiotics
    FIRST_VALUE(src.trace_id::TEXT) OVER (
        PARTITION BY
            src.subject_id,
            src.hadm_id,
            COALESCE(src.charttime, src.chartdate),
            src.spec_itemid,
            src.test_itemid,
            src.org_itemid
        ORDER BY src.trace_id::TEXT
    )                                       AS trace_id_org, -- Cast to TEXT for test-organism pairs
    FIRST_VALUE(src.trace_id::TEXT) OVER (
        PARTITION BY
            src.subject_id,
            src.hadm_id,
            COALESCE(src.charttime, src.chartdate),
            src.spec_itemid
        ORDER BY src.trace_id::TEXT
    )                                       AS trace_id_spec, -- Cast to TEXT for specimen
    subject_id                              AS subject_id,    -- Subject ID
    hadm_id                                 AS hadm_id,       -- Hospital admission ID
    COALESCE(src.charttime, src.chartdate)  AS start_datetime -- Event timestamp
FROM
    src_microbiologyevents src;

-- -------------------------------------------------------------------
-- lk_micro_hadm_id
-- Maps additional hospital admission IDs using event timestamps
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_micro_hadm_id;
CREATE TABLE lk_micro_hadm_id AS
SELECT
    src.trace_id_ab::TEXT             AS event_trace_id, -- Cast to TEXT for event trace ID
    adm.hadm_id                       AS hadm_id,
    ROW_NUMBER() OVER (
        PARTITION BY src.trace_id_ab::TEXT
        ORDER BY adm.start_datetime
    )                                 AS row_num -- Pick the earliest admission
FROM  
    lk_micro_cross_ref src
INNER JOIN 
    lk_admissions_clean adm
        ON adm.subject_id = src.subject_id
        AND src.start_datetime::TIMESTAMP BETWEEN adm.start_datetime AND adm.end_datetime
WHERE
    src.hadm_id IS NULL;

-- -------------------------------------------------------------------
-- lk_meas_organism_clean
-- Maps microbiology organisms with corresponding test and specimen
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_meas_organism_clean;
CREATE TABLE lk_meas_organism_clean AS
SELECT DISTINCT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    cr.start_datetime                           AS start_datetime,
    src.spec_itemid                             AS spec_itemid, -- Specimen type
    src.test_itemid                             AS test_itemid, -- Test type
    src.org_itemid                              AS org_itemid,  -- Organism grown
    cr.trace_id_spec::TEXT                      AS trace_id_spec, -- Cast to TEXT
    'micro.organism'                            AS unit_id,
    src.load_table_id                           AS load_table_id,
    0                                           AS load_row_id,
    cr.trace_id_org::TEXT                       AS trace_id -- Cast to TEXT
FROM
    src_microbiologyevents src
INNER JOIN
    lk_micro_cross_ref cr
        ON src.trace_id::TEXT = cr.trace_id_org::TEXT; -- Cast to TEXT

-- -------------------------------------------------------------------
-- lk_specimen_clean
-- Cleans and maps specimen-level data
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_specimen_clean;
CREATE TABLE lk_specimen_clean AS
SELECT DISTINCT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.start_datetime                          AS start_datetime,
    src.spec_itemid                             AS spec_itemid,
    'micro.specimen'                            AS unit_id,
    src.load_table_id                           AS load_table_id,
    0                                           AS load_row_id,
    cr.trace_id_spec                            AS trace_id
FROM
    lk_meas_organism_clean src
INNER JOIN
    lk_micro_cross_ref cr
        ON src.trace_id = cr.trace_id_spec;

-- -------------------------------------------------------------------
-- lk_meas_ab_clean
-- Cleans antibiotic resistance data
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_meas_ab_clean;
CREATE TABLE lk_meas_ab_clean AS
SELECT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    cr.start_datetime                           AS start_datetime,
    src.ab_itemid                               AS ab_itemid, -- Antibiotic tested
    src.dilution_comparison                     AS dilution_comparison, -- Operator sign
    src.dilution_value                          AS dilution_value, -- Numeric dilution value
    src.interpretation                          AS interpretation, -- Degree of resistance
    cr.trace_id_org                             AS trace_id_org,
    'micro.antibiotics'                         AS unit_id,
    src.load_table_id                           AS load_table_id,
    0                                           AS load_row_id,
    src.trace_id                                AS trace_id
FROM
    src_microbiologyevents src
INNER JOIN
    lk_micro_cross_ref cr
        ON src.trace_id::TEXT = cr.trace_id_ab
WHERE
    src.ab_itemid IS NOT NULL;

-- -------------------------------------------------------------------
-- lk_d_micro_clean
-- Prepares microbiology items and resistance codes
-- -------------------------------------------------------------------

----------------------------------------------------------------------------
-- COMMENTED OUT FOR NOW SO I CAN RUN EVERYTHING ELSE, NEED TO COME BACK TO FIX

/*
DROP TABLE IF EXISTS lk_d_micro_clean;
CREATE TABLE lk_d_micro_clean AS
SELECT
    dm.itemid                                       AS itemid,
    CAST(dm.itemid AS TEXT)                         AS source_code,
    dm.label                                        AS source_label,
    CONCAT('mimiciv_micro_', LOWER(dm.category))    AS source_vocabulary_id
FROM
    src_d_micro dm
UNION ALL
SELECT DISTINCT
    CAST(NULL AS INT64)                             AS itemid,
    src.interpretation                              AS source_code,
    src.interpretation                              AS source_label,
    'mimiciv_micro_resistance'                      AS source_vocabulary_id
FROM
    lk_meas_ab_clean src
WHERE
    src.interpretation IS NOT NULL;

----------------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_specimen_mapped
-- Maps specimens to target concepts
-- -------------------------------------------------------------------

----------------------------------------------------------------------------
-- COMMENTED OUT FOR NOW SO I CAN RUN EVERYTHING ELSE, NEED TO COME BACK TO FIX

DROP TABLE IF EXISTS lk_specimen_mapped;
CREATE TABLE lk_specimen_mapped AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS specimen_id,
    src.subject_id                                  AS subject_id,
    COALESCE(src.hadm_id, hadm.hadm_id)             AS hadm_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    32856                                           AS type_concept_id,
    src.start_datetime                              AS start_datetime,
    src.spec_itemid                                 AS spec_itemid,
    mc.source_code                                  AS source_code,
    mc.source_vocabulary_id                         AS source_vocabulary_id,
    mc.source_concept_id                            AS source_concept_id,
    COALESCE(mc.target_domain_id, 'Specimen')       AS target_domain_id,
    mc.target_concept_id                            AS target_concept_id,
    src.unit_id                                     AS unit_id,
    src.load_table_id                               AS load_table_id,
    src.load_row_id                                 AS load_row_id,
    src.trace_id                                    AS trace_id
FROM
    lk_specimen_clean src
INNER JOIN
    microbiologyevents mc
        ON src.spec_itemid = mc.itemid
LEFT JOIN
    lk_micro_hadm_id hadm
        ON hadm.event_trace_id = src.trace_id
        AND hadm.row_num = 1;

----------------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_meas_organism_mapped
-- -------------------------------------------------------------------

-- COMMENTED OUT FOR NOW SO I CAN RUN EVERYTHING ELSE, NEED TO COME BACK TO FIX
DROP TABLE IF EXISTS lk_meas_organism_mapped;
CREATE TABLE lk_meas_organism_mapped AS
SELECT
    md5(gen_random_uuid()::text)                       AS measurement_id,
    src.subject_id                                     AS subject_id,
    COALESCE(src.hadm_id, hadm.hadm_id)                AS hadm_id,
    CAST(src.start_datetime AS DATE)                  AS date_id,
    32856                                             AS type_concept_id, -- Lab
    src.start_datetime                                 AS start_datetime,
    src.test_itemid                                    AS test_itemid,
    src.spec_itemid                                    AS spec_itemid,
    src.org_itemid                                     AS org_itemid,
    CONCAT(tc.source_code, '|', sc.source_code)        AS source_code, -- test itemid plus specimen itemid
    tc.source_vocabulary_id                            AS source_vocabulary_id,
    tc.source_concept_id                               AS source_concept_id,
    COALESCE(tc.target_domain_id, 'Measurement')       AS target_domain_id,
    tc.target_concept_id                               AS target_concept_id,
    oc.source_code                                     AS value_source_value,
    oc.target_concept_id                               AS value_as_concept_id,
    -- fields to link to specimen and test-organism
    src.trace_id_spec                                  AS trace_id_spec,
    -- 
    src.unit_id                                        AS unit_id,
    src.load_table_id                                  AS load_table_id,
    src.load_row_id                                    AS load_row_id,
    src.trace_id                                       AS trace_id
FROM
    lk_meas_organism_clean src
INNER JOIN
    microbiologyevents tc
        ON src.test_itemid = tc.itemid
INNER JOIN
    microbiologyevents sc
        ON src.spec_itemid = sc.itemid
LEFT JOIN
    microbiologyevents oc
        ON src.org_itemid = oc.itemid
LEFT JOIN
    lk_micro_hadm_id hadm
        ON hadm.event_trace_id = src.trace_id
        AND hadm.row_num = 1;

-- -------------------------------------------------------------------
-- lk_meas_ab_mapped
-- -------------------------------------------------------------------

-- COMMENTED OUT FOR NOW SO I CAN RUN EVERYTHING ELSE, NEED TO COME BACK TO FIX
DROP TABLE IF EXISTS lk_meas_ab_mapped;
CREATE TABLE lk_meas_ab_mapped AS
SELECT
    md5(gen_random_uuid()::text)                       AS measurement_id,
    src.subject_id                                     AS subject_id,
    COALESCE(src.hadm_id, hadm.hadm_id)                AS hadm_id,
    CAST(src.start_datetime AS DATE)                  AS date_id,
    32856                                             AS type_concept_id, -- Lab
    src.start_datetime                                 AS start_datetime,
    src.ab_itemid                                      AS ab_itemid,
    ac.source_code                                     AS source_code,
    COALESCE(ac.target_concept_id, 0)                  AS target_concept_id,
    COALESCE(ac.source_concept_id, 0)                  AS source_concept_id,
    rc.target_concept_id                               AS value_as_concept_id,
    src.interpretation                                 AS value_source_value,
    src.dilution_value                                 AS value_as_number,
    src.dilution_comparison                            AS operator_source_value,
    opc.target_concept_id                              AS operator_concept_id,
    COALESCE(ac.target_domain_id, 'Measurement')       AS target_domain_id,
    -- fields to link test-organism and antibiotics
    src.trace_id_org                                   AS trace_id_org,
    -- 
    src.unit_id                                        AS unit_id,
    src.load_table_id                                  AS load_table_id,
    src.load_row_id                                    AS load_row_id,
    src.trace_id                                       AS trace_id
FROM
    lk_meas_ab_clean src
INNER JOIN
    microbiologyevents ac
        ON src.ab_itemid = ac.itemid
LEFT JOIN
    microbiologyevents rc
        ON src.interpretation = rc.source_code
        AND rc.source_vocabulary_id = 'mimiciv_micro_resistance' -- new vocab
LEFT JOIN
    lk_meas_operator_concept opc -- see lk_meas_labevents.sql
        ON src.dilution_comparison = opc.source_code
LEFT JOIN
    lk_micro_hadm_id hadm
        ON hadm.event_trace_id = src.trace_id
        AND hadm.row_num = 1;
