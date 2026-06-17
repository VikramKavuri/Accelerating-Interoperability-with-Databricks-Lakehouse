# Databricks notebook source
"""Run data quality checks and publish quality result tables."""

# COMMAND ----------

# MAGIC %run ./_shared

# COMMAND ----------

from pyspark.sql.functions import col, count, current_date, current_timestamp, lit, struct, to_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType


create_target_schema()

patients = read_table("silver_patient")
encounters = read_table("silver_encounter")
conditions = read_table("silver_condition")
observations = read_table("silver_observation")
claims = read_table("silver_claim")
bronze = read_table("bronze_fhir_resources")

patient_ids = patients.select("patient_id").where(col("patient_id").isNotNull())
encounter_ids = encounters.select("encounter_id").where(col("encounter_id").isNotNull())

quality_failures = {
    "patient_id_not_null": ("silver_patient", patients.where(col("patient_id").isNull())),
    "patient_birth_date_valid": (
        "silver_patient",
        patients.where(col("birth_date").isNotNull() & (col("birth_date") > current_date().cast("string"))),
    ),
    "encounter_has_patient": (
        "silver_encounter",
        encounters.join(patient_ids, "patient_id", "left_anti"),
    ),
    "condition_has_patient": (
        "silver_condition",
        conditions.join(patient_ids, "patient_id", "left_anti"),
    ),
    "observation_has_code": (
        "silver_observation",
        observations.where(col("observation_code").isNull()),
    ),
    "claim_amount_non_negative": (
        "silver_claim",
        claims.where(col("amount").isNotNull() & (col("amount") < 0)),
    ),
    "duplicate_resource_check": (
        "bronze_fhir_resources",
        bronze.groupBy("resource_type", "resource_id")
        .agg(count("*").alias("duplicate_count"))
        .where(col("resource_id").isNotNull() & (col("duplicate_count") > 1)),
    ),
    "referential_integrity_check": (
        "clinical_references",
        conditions.select("patient_id", "encounter_id")
        .unionByName(observations.select("patient_id", "encounter_id"))
        .where(col("encounter_id").isNotNull())
        .join(encounter_ids, "encounter_id", "left_anti"),
    ),
}

result_rows = []
failed_frames = []
for check_name, (table, failed_df) in quality_failures.items():
    failed_count = failed_df.count()
    result_rows.append(
        (
            batch_id(),
            check_name,
            table,
            failed_count,
            "pass" if failed_count == 0 else "fail",
        )
    )
    if failed_count:
        failed_frames.append(
            failed_df.select(
                lit(batch_id()).alias("batch_id"),
                lit(check_name).alias("check_name"),
                lit(table).alias("table_name"),
                to_json(struct("*")).alias("failed_record_json"),
                current_timestamp().alias("checked_at"),
            )
        )

result_schema = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("failed_count", IntegerType(), False),
        StructField("status", StringType(), False),
    ]
)
results = spark.createDataFrame(result_rows, result_schema).withColumn("checked_at", current_timestamp())
write_delta(results, "quality_check_results")

if failed_frames:
    failed = failed_frames[0]
    for frame in failed_frames[1:]:
        failed = failed.unionByName(frame, allowMissingColumns=True)
else:
    failed_schema = StructType(
        [
            StructField("batch_id", StringType()),
            StructField("check_name", StringType()),
            StructField("table_name", StringType()),
            StructField("failed_record_json", StringType()),
            StructField("checked_at", TimestampType()),
        ]
    )
    failed = spark.createDataFrame([], failed_schema)
write_delta(failed, "quality_failed_records")

summary = spark.sql(
    f"""
    SELECT
      '{batch_id()}' AS batch_id,
      count(*) AS checks_run,
      sum(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS checks_passed,
      sum(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS checks_failed,
      sum(failed_count) AS failed_record_count,
      current_timestamp() AS checked_at
    FROM {table_name('quality_check_results')}
    """
)
write_delta(summary, "quality_run_summary")
