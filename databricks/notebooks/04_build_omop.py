# Databricks notebook source
"""Build a focused OMOP-style subset from Silver clinical tables."""

# COMMAND ----------

# MAGIC %run ./_shared

# COMMAND ----------

from pyspark.sql.functions import abs, col, coalesce, concat_ws, hash, lit, year


create_target_schema()

patients = read_table("silver_patient")
encounters = read_table("silver_encounter")
conditions = read_table("silver_condition")
observations = read_table("silver_observation")
medications = read_table("silver_medication")
claims = read_table("silver_claim")
procedures = read_table("silver_procedure")

write_delta(
    patients.select(
        abs(hash(col("patient_id"))).alias("person_id"),
        col("patient_id").alias("fhir_patient_id"),
        col("gender").alias("gender_source_value"),
        year(col("birth_date").cast("date")).alias("year_of_birth"),
        col("birth_date").alias("birth_datetime"),
        lit(None).cast("string").alias("race_source_value"),
        lit(None).cast("string").alias("ethnicity_source_value"),
        concat_ws(", ", col("city"), col("state")).alias("location_source_value"),
    ),
    "omop_person",
)

write_delta(
    encounters.select(
        abs(hash(col("encounter_id"))).alias("visit_occurrence_id"),
        abs(hash(col("patient_id"))).alias("person_id"),
        col("encounter_id").alias("fhir_encounter_id"),
        col("encounter_class").alias("visit_concept_code"),
        col("encounter_type_display").alias("visit_source_value"),
        col("start_datetime").alias("visit_start_datetime"),
        col("end_datetime").alias("visit_end_datetime"),
    ),
    "omop_visit_occurrence",
)

write_delta(
    conditions.select(
        abs(hash(col("condition_id"))).alias("condition_occurrence_id"),
        abs(hash(col("patient_id"))).alias("person_id"),
        abs(hash(col("encounter_id"))).alias("visit_occurrence_id"),
        col("condition_code").alias("condition_source_code"),
        col("condition_display").alias("condition_source_value"),
        coalesce(col("onset_datetime"), col("recorded_date")).alias("condition_start_datetime"),
    ),
    "omop_condition_occurrence",
)

write_delta(
    observations.select(
        abs(hash(col("observation_id"))).alias("measurement_id"),
        abs(hash(col("patient_id"))).alias("person_id"),
        abs(hash(col("encounter_id"))).alias("visit_occurrence_id"),
        col("observation_code").alias("measurement_source_code"),
        col("observation_display").alias("measurement_source_value"),
        col("effective_datetime").alias("measurement_datetime"),
        col("value_number").alias("value_as_number"),
        col("value_unit").alias("unit_source_value"),
    ),
    "omop_measurement",
)

write_delta(
    medications.select(
        abs(hash(col("medication_request_id"))).alias("drug_exposure_id"),
        abs(hash(col("patient_id"))).alias("person_id"),
        abs(hash(col("encounter_id"))).alias("visit_occurrence_id"),
        col("medication_code").alias("drug_source_code"),
        col("medication_display").alias("drug_source_value"),
        col("authored_on").alias("drug_exposure_start_datetime"),
    ),
    "omop_drug_exposure",
)

write_delta(
    procedures.select(
        abs(hash(col("procedure_id"))).alias("procedure_occurrence_id"),
        abs(hash(col("patient_id"))).alias("person_id"),
        abs(hash(col("encounter_id"))).alias("visit_occurrence_id"),
        col("procedure_code").alias("procedure_source_code"),
        col("procedure_display").alias("procedure_source_value"),
        col("performed_datetime").alias("procedure_datetime"),
    ),
    "omop_procedure_occurrence",
)

write_delta(
    claims.select(
        abs(hash(col("claim_id"))).alias("payer_plan_period_id"),
        abs(hash(col("patient_id"))).alias("person_id"),
        col("claim_type_display").alias("payer_source_value"),
        col("claim_type_code").alias("plan_source_value"),
        col("created_date").alias("period_start_date"),
    ),
    "omop_payer_plan_period",
)
