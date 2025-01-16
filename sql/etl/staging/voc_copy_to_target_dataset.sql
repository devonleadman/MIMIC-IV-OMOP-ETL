-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Copy vocabulary tables from the master vocab dataset
-- (to apply custom mapping here?)
-- -------------------------------------------------------------------

-- check
-- SELECT 'VOC', COUNT(*) FROM concept
-- UNION ALL
-- SELECT 'TARGET', COUNT(*) FROM voc_concept
-- ;

-- affected by custom mapping

-- -------------------------------------------------------------------
-- Create voc_concept
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS voc_concept;
CREATE TABLE voc_concept AS
SELECT * FROM concept;

-- -------------------------------------------------------------------
-- Create voc_concept_relationship
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS voc_concept_relationship;
CREATE TABLE voc_concept_relationship AS
SELECT * FROM concept_relationship;

-- -------------------------------------------------------------------
-- Create voc_vocabulary
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS voc_vocabulary;
CREATE TABLE voc_vocabulary AS
SELECT * FROM vocabulary;

-- -------------------------------------------------------------------
-- Create voc_domain
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS voc_domain;
CREATE TABLE voc_domain AS
SELECT * FROM domain;

-- -------------------------------------------------------------------
-- Create voc_concept_class
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS voc_concept_class;
CREATE TABLE voc_concept_class AS
SELECT * FROM concept_class;

-- -------------------------------------------------------------------
-- Create voc_relationship
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS voc_relationship;
CREATE TABLE voc_relationship AS
SELECT * FROM relationship;

-- -------------------------------------------------------------------
-- Create voc_concept_synonym
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS voc_concept_synonym;
CREATE TABLE voc_concept_synonym AS
SELECT * FROM concept_synonym;

-- -------------------------------------------------------------------
-- Create voc_concept_ancestor
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS voc_concept_ancestor;
CREATE TABLE voc_concept_ancestor AS
SELECT * FROM concept_ancestor;

-- -------------------------------------------------------------------
-- Create voc_drug_strength
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS voc_drug_strength;
CREATE TABLE voc_drug_strength AS
SELECT * FROM drug_strength;

