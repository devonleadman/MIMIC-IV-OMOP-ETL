-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_visit_detail table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      st_waveform.sql,
--      lk_vis_adm_transfers.sql,
--      cdm_person.sql,
--      cdm_visit_occurrence.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- src.callout - is there any derived table in MIMIC IV?
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_visit_detail
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE cdm_visit_detail (
    visit_detail_id                BIGINT NOT NULL,
    person_id                      BIGINT NOT NULL,
    visit_detail_concept_id        BIGINT NOT NULL,
    visit_detail_start_date        DATE NOT NULL,
    visit_detail_start_datetime    TIMESTAMP,
    visit_detail_end_date          DATE NOT NULL,
    visit_detail_end_datetime      TIMESTAMP,
    visit_detail_type_concept_id   BIGINT NOT NULL,
    provider_id                    BIGINT,
    care_site_id                   BIGINT,
    admitting_source_concept_id    BIGINT,
    discharge_to_concept_id        BIGINT,
    preceding_visit_detail_id      BIGINT,
    visit_detail_source_value      TEXT,
    visit_detail_source_concept_id BIGINT,
    admitting_source_value         TEXT,
    discharge_to_source_value      TEXT,
    visit_detail_parent_id         BIGINT,
    visit_occurrence_id            BIGINT NOT NULL,
    unit_id                        TEXT,
    load_table_id                  TEXT,
    load_row_id                    BIGINT,
    trace_id                       TEXT
);

INSERT INTO cdm_visit_detail (
    visit_detail_id,
    person_id,
    visit_detail_concept_id,
    visit_detail_start_date,
    visit_detail_start_datetime,
    visit_detail_end_date,
    visit_detail_end_datetime,
    visit_detail_type_concept_id,
    provider_id,
    care_site_id,
    admitting_source_concept_id,
    discharge_to_concept_id,
    preceding_visit_detail_id,
    visit_detail_source_value,
    visit_detail_source_concept_id,
    admitting_source_value,
    discharge_to_source_value,
    visit_detail_parent_id,
    visit_occurrence_id,
    unit_id,
    load_table_id,
    load_row_id,
    trace_id
)
SELECT
    src.visit_detail_id,
    per.person_id,
    COALESCE(vdc.target_concept_id, 0) AS visit_detail_concept_id,
    src.start_datetime::DATE AS visit_detail_start_date,
    src.start_datetime AS visit_detail_start_datetime,
    src.end_datetime::DATE AS visit_detail_end_date,
    src.end_datetime AS visit_detail_end_datetime,
    32817 AS visit_detail_type_concept_id,
    NULL::BIGINT AS provider_id,
    cs.care_site_id,
    CASE
        WHEN src.admission_location IS NOT NULL THEN COALESCE(la.target_concept_id, 0)
        ELSE NULL
    END AS admitting_source_concept_id,
    CASE
        WHEN src.discharge_location IS NOT NULL THEN COALESCE(ld.target_concept_id, 0)
        ELSE NULL
    END AS discharge_to_concept_id,
    src.preceding_visit_detail_id,
    src.source_value AS visit_detail_source_value,
    COALESCE(vdc.source_concept_id, 0) AS visit_detail_source_concept_id,
    src.admission_location AS admitting_source_value,
    src.discharge_location AS discharge_to_source_value,
    NULL::BIGINT AS visit_detail_parent_id,
    vis.visit_occurrence_id,
    CONCAT('visit_detail.', src.unit_id) AS unit_id,
    src.load_table_id,
    src.load_row_id,
    src.trace_id
FROM
    lk_visit_detail_prev_next src
INNER JOIN
    cdm_person per 
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    cdm_visit_occurrence vis 
        ON vis.visit_source_value = CONCAT(
            CAST(src.subject_id AS TEXT), '|', 
            COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT))
        )
LEFT JOIN
    cdm_care_site cs
        ON cs.care_site_source_value = src.current_location
LEFT JOIN
    lk_visit_concept vdc
        ON vdc.source_code = src.current_location
LEFT JOIN
    lk_visit_concept la 
        ON la.source_code = src.admission_location
LEFT JOIN
    lk_visit_concept ld
        ON ld.source_code = src.discharge_location;

