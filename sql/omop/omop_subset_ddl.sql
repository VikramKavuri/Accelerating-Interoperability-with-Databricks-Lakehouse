CREATE TABLE IF NOT EXISTS omop_person (
  person_id BIGINT,
  fhir_patient_id STRING,
  gender_source_value STRING,
  year_of_birth INT,
  birth_datetime STRING,
  race_source_value STRING,
  ethnicity_source_value STRING,
  location_source_value STRING
);

CREATE TABLE IF NOT EXISTS omop_visit_occurrence (
  visit_occurrence_id BIGINT,
  person_id BIGINT,
  fhir_encounter_id STRING,
  visit_concept_code STRING,
  visit_source_value STRING,
  visit_start_datetime STRING,
  visit_end_datetime STRING
);

CREATE TABLE IF NOT EXISTS omop_condition_occurrence (
  condition_occurrence_id BIGINT,
  person_id BIGINT,
  visit_occurrence_id BIGINT,
  condition_source_code STRING,
  condition_source_value STRING,
  condition_start_datetime STRING
);

CREATE TABLE IF NOT EXISTS omop_measurement (
  measurement_id BIGINT,
  person_id BIGINT,
  visit_occurrence_id BIGINT,
  measurement_source_code STRING,
  measurement_source_value STRING,
  measurement_datetime STRING,
  value_as_number DOUBLE,
  unit_source_value STRING
);

CREATE TABLE IF NOT EXISTS omop_drug_exposure (
  drug_exposure_id BIGINT,
  person_id BIGINT,
  visit_occurrence_id BIGINT,
  drug_source_code STRING,
  drug_source_value STRING,
  drug_exposure_start_datetime STRING
);

CREATE TABLE IF NOT EXISTS omop_procedure_occurrence (
  procedure_occurrence_id BIGINT,
  person_id BIGINT,
  visit_occurrence_id BIGINT,
  procedure_source_code STRING,
  procedure_source_value STRING,
  procedure_datetime STRING
);

CREATE TABLE IF NOT EXISTS omop_payer_plan_period (
  payer_plan_period_id BIGINT,
  person_id BIGINT,
  payer_source_value STRING,
  plan_source_value STRING,
  period_start_date STRING
);
