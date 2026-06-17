# Databricks notebook source
"""Ingest FHIR JSON/NDJSON files into Bronze Delta tables."""

# COMMAND ----------

# MAGIC %run ./_shared

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, current_timestamp, explode, lit, sha2


create_target_schema()

raw_files = spark.read.format("binaryFile").load(source_path())

parsed = raw_files.select(
    col("path").alias("source_file"),
    explode(parse_fhir_content_udf(col("content"))).alias("resource"),
)

bronze_all = parsed.select(
    lit(batch_id()).alias("batch_id"),
    col("source_file"),
    col("resource.resource_type").alias("resource_type"),
    col("resource.resource_id").alias("resource_id"),
    col("resource.raw_resource_json").alias("raw_resource_json"),
    sha2(col("resource.raw_resource_json"), 256).alias("raw_hash"),
    col("resource.parse_status").alias("parse_status"),
    col("resource.error_message").alias("error_message"),
    current_timestamp().alias("ingested_at"),
)

write_delta(
    bronze_all.where(col("parse_status") == "parsed").drop("error_message"),
    "bronze_fhir_resources",
)
write_delta(
    bronze_all.where(col("parse_status") != "parsed"),
    "bronze_rejected_records",
)

audit = (
    bronze_all.groupBy("batch_id", "resource_type")
    .agg(
        countDistinct("source_file").alias("source_file_count"),
        count("*").alias("resource_type_count"),
    )
    .withColumn("ingested_at", current_timestamp())
)
write_delta(audit, "bronze_ingestion_audit")
