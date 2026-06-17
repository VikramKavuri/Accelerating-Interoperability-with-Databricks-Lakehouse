# Databricks notebook source
"""Shared helpers for the Databricks FHIR-to-OMOP workflow."""

# COMMAND ----------

import json

from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType, StructField, StructType


RESOURCE_SCHEMA = ArrayType(
    StructType(
        [
            StructField("resource_type", StringType()),
            StructField("resource_id", StringType()),
            StructField("raw_resource_json", StringType()),
            StructField("parse_status", StringType()),
            StructField("error_message", StringType()),
        ]
    )
)


def get_param(name, default=None):
    try:
        value = dbutils.widgets.get(name)
        return value if value != "" else default
    except Exception:
        return default


def target_catalog():
    return get_param("target_catalog", "main")


def target_schema():
    return get_param("target_schema", "healthcare_fhir_omop_dev")


def batch_id():
    return get_param("batch_id", "manual-run")


def source_path():
    return get_param("source_path", "/Volumes/main/healthcare_fhir_omop_dev/raw/fhir-small")


def table_name(name):
    return f"{target_catalog()}.{target_schema()}.{name}"


def create_target_schema():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog()}.{target_schema()}")


def write_delta(df, name, mode="overwrite"):
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name(name))
    )


def parse_fhir_content(content):
    text = content.decode("utf-8") if isinstance(content, bytes) else str(content)
    text = text.strip()
    if not text:
        return []

    try:
        documents = [json.loads(text)]
    except json.JSONDecodeError:
        documents = []
        for line_number, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                documents.append(json.loads(stripped))
            except json.JSONDecodeError as exc:
                return [
                    {
                        "resource_type": None,
                        "resource_id": None,
                        "raw_resource_json": stripped,
                        "parse_status": "rejected",
                        "error_message": f"line {line_number}: {exc.msg}",
                    }
                ]

    rows = []
    for document in documents:
        if document.get("resourceType") == "Bundle":
            resources = [
                entry.get("resource") or {}
                for entry in document.get("entry", [])
                if entry.get("resource")
            ]
        else:
            resources = [document]

        for resource in resources:
            resource_type = resource.get("resourceType")
            rows.append(
                {
                    "resource_type": resource_type,
                    "resource_id": resource.get("id"),
                    "raw_resource_json": json.dumps(resource, sort_keys=True),
                    "parse_status": "parsed" if resource_type else "rejected",
                    "error_message": None if resource_type else "Missing resourceType",
                }
            )
    return rows


parse_fhir_content_udf = udf(parse_fhir_content, RESOURCE_SCHEMA)


def read_table(name):
    return spark.table(table_name(name))


def resource_filter(resource_type):
    return read_table("bronze_fhir_resources").where(col("resource_type") == resource_type)
