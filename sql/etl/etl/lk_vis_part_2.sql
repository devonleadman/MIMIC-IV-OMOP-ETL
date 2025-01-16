-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate lookups for cdm_visit_occurrence and cdm_visit_detail
-- 
-- Dependencies: run after 
--      st_core.sql
--      lk_vis_part_1.sql
--      lk_meas_labevents.sql
--      lk_meas_specimen.sql
--      lk_meas_waveform.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_visit_no_hadm_all
--
-- collect rows without hadm_id from all tables affected by this case:
--      lk_meas_labevents_mapped
--      lk_meas_organism_mapped
--      lk_meas_ab_mapped
--      lk_meas_waveform_mapped
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_visit_detail_clean
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_visit_detail_clean;

CREATE TABLE lk_visit_detail_clean AS
SELECT
    md5(gen_random_uuid()::text)                    AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    src.date_id                                     AS date_id,
    src.start_datetime                              AS start_datetime,
    src.end_datetime                                AS end_datetime,
    CONCAT(
        CAST(src.subject_id AS TEXT), '|',
        COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)), '|',
        CAST(src.transfer_id AS TEXT)
    )                                               AS source_value,
    src.current_location                            AS current_location,
    src.unit_id                                     AS unit_id,
    src.load_table_id                               AS load_table_id,
    src.load_row_id                                 AS load_row_id,
    src.trace_id                                    AS trace_id
FROM 
    lk_transfers_clean src
WHERE
    src.hadm_id IS NOT NULL;

-- Rule 2: ER admissions
INSERT INTO lk_visit_detail_clean
SELECT
    md5(gen_random_uuid()::text)                    AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    CAST(src.start_datetime AS DATE)               AS date_id,
    src.start_datetime                              AS start_datetime,
    CAST(NULL AS TIMESTAMP)                         AS end_datetime,
    CONCAT(
        CAST(src.subject_id AS TEXT), '|',
        CAST(src.hadm_id AS TEXT)
    )                                               AS source_value,
    src.admission_type                              AS current_location,
    src.unit_id                                     AS unit_id,
    src.load_table_id                               AS load_table_id,
    src.load_row_id                                 AS load_row_id,
    src.trace_id                                    AS trace_id
FROM 
    lk_admissions_clean src
WHERE
    src.is_er_admission;

-- Rule 3: Services
INSERT INTO lk_visit_detail_clean
SELECT
    md5(gen_random_uuid()::text)                    AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    CAST(src.start_datetime AS DATE)               AS date_id,
    src.start_datetime                              AS start_datetime,
    src.end_datetime                                AS end_datetime,
    CONCAT(
        CAST(src.subject_id AS TEXT), '|',
        CAST(src.hadm_id AS TEXT), '|',
        CAST(src.start_datetime AS TEXT)
    )                                               AS source_value,
    src.curr_service                                AS current_location,
    src.unit_id                                     AS unit_id,
    src.load_table_id                               AS load_table_id,
    src.load_row_id                                 AS load_row_id,
    src.trace_id                                    AS trace_id
FROM 
    lk_services_clean src
WHERE
    src.prev_service = src.lag_service;

-- Rule 4: Waveforms
INSERT INTO lk_visit_detail_clean
SELECT
    md5(gen_random_uuid()::text)                    AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    src.date_id                                     AS date_id,
    src.start_datetime                              AS start_datetime,
    src.end_datetime                                AS end_datetime,
    src.reference_id                                AS source_value,
    src.current_location                            AS current_location,
    src.unit_id                                     AS unit_id,
    src.load_table_id                               AS load_table_id,
    src.load_row_id                                 AS load_row_id,
    src.trace_id                                    AS trace_id
FROM 
    lk_visit_detail_waveform_dist src;

-- -------------------------------------------------------------------
-- lk_visit_detail_prev_next
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_visit_detail_prev_next;

CREATE TABLE lk_visit_detail_prev_next AS
SELECT 
    src.visit_detail_id                             AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    src.date_id                                     AS date_id,
    src.start_datetime                              AS start_datetime,
    COALESCE(
        src.end_datetime,
        LEAD(src.start_datetime) OVER (
            PARTITION BY src.subject_id, src.hadm_id, src.date_id
            ORDER BY src.start_datetime ASC
        ),
        vis.end_datetime
    )                                               AS end_datetime,
    src.source_value                                AS source_value,
    src.current_location                            AS current_location,
    LAG(src.visit_detail_id) OVER (
        PARTITION BY src.subject_id, src.hadm_id, src.date_id, src.unit_id
        ORDER BY src.start_datetime ASC
    )                                               AS preceding_visit_detail_id,
    COALESCE(
        LAG(src.current_location) OVER (
            PARTITION BY src.subject_id, src.hadm_id, src.date_id, src.unit_id
            ORDER BY src.start_datetime ASC
        ),
        vis.admission_location
    )                                               AS admission_location,
    COALESCE(
        LEAD(src.current_location) OVER (
            PARTITION BY src.subject_id, src.hadm_id, src.date_id, src.unit_id
            ORDER BY src.start_datetime ASC
        ),
        vis.discharge_location
    )                                               AS discharge_location,
    src.unit_id                                     AS unit_id,
    src.load_table_id                               AS load_table_id,
    src.load_row_id                                 AS load_row_id,
    src.trace_id                                    AS trace_id
FROM 
    lk_visit_detail_clean src
LEFT JOIN 
    lk_visit_clean vis
        ON src.subject_id = vis.subject_id
        AND (
            src.hadm_id = vis.hadm_id
            OR (src.hadm_id IS NULL AND src.date_id = vis.date_id)
        );

-- -------------------------------------------------------------------
-- lk_visit_concept
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS lk_visit_concept;

CREATE TABLE lk_visit_concept AS
SELECT 
    vc.concept_code     AS source_code,
    vc.concept_id       AS source_concept_id,
    vc2.concept_id      AS target_concept_id,
    vc.vocabulary_id    AS source_vocabulary_id
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
    vc.vocabulary_id IN (
        'mimiciv_vis_admission_location',
        'mimiciv_vis_discharge_location',
        'mimiciv_vis_service',
        'mimiciv_vis_admission_type',
        'mimiciv_cs_place_of_service'
    );
