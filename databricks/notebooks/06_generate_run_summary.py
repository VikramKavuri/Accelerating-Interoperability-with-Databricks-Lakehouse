# Databricks notebook source
"""Write a final run summary for job evidence and demo export."""

# COMMAND ----------

# MAGIC %run ./_shared

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit


create_target_schema()

tracked_tables = [
    "bronze_fhir_resources",
    "bronze_rejected_records",
    "silver_patient",
    "silver_encounter",
    "silver_condition",
    "silver_observation",
    "silver_medication",
    "silver_claim",
    "silver_procedure",
    "silver_provider",
    "quality_check_results",
    "quality_failed_records",
    "omop_person",
    "omop_visit_occurrence",
    "omop_condition_occurrence",
    "omop_measurement",
    "omop_drug_exposure",
    "omop_procedure_occurrence",
    "omop_payer_plan_period",
    "gold_patient_summary",
    "gold_condition_prevalence",
    "gold_encounter_utilization",
    "gold_claim_cost_summary",
    "gold_medication_usage",
    "gold_population_health_cohort",
]

rows = []
for name in tracked_tables:
    rows.append((batch_id(), source_path(), name, read_table(name).count()))

summary = spark.createDataFrame(rows, ["batch_id", "source_path", "table_name", "row_count"])
summary = summary.withColumn("generated_at", current_timestamp())
write_delta(summary, "pipeline_run_summary")

display(summary.orderBy("table_name"))
