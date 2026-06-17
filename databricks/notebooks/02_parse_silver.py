# Databricks notebook source
"""Parse Bronze FHIR resources into Silver clinical tables."""

# COMMAND ----------

# MAGIC %run ./_shared

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, get_json_object, lit, regexp_extract


create_target_schema()


def json_col(path):
    return get_json_object(col("raw_resource_json"), path)


patients = resource_filter("Patient").select(
    json_col("$.id").alias("patient_id"),
    json_col("$.identifier[0].value").alias("mrn"),
    json_col("$.name[0].given[0]").alias("first_name"),
    json_col("$.name[0].family").alias("last_name"),
    json_col("$.gender").alias("gender"),
    json_col("$.birthDate").alias("birth_date"),
    json_col("$.deceasedBoolean").cast("boolean").alias("deceased"),
    json_col("$.address[0].city").alias("city"),
    json_col("$.address[0].state").alias("state"),
    json_col("$.address[0].postalCode").alias("postal_code"),
)
write_delta(patients, "silver_patient")

encounters = resource_filter("Encounter").select(
    json_col("$.id").alias("encounter_id"),
    regexp_extract(json_col("$.subject.reference"), r"([^/]+)$", 1).alias("patient_id"),
    json_col("$.status").alias("status"),
    json_col("$.class.code").alias("encounter_class"),
    json_col("$.type[0].coding[0].code").alias("encounter_type_code"),
    json_col("$.type[0].coding[0].display").alias("encounter_type_display"),
    json_col("$.period.start").alias("start_datetime"),
    json_col("$.period.end").alias("end_datetime"),
    regexp_extract(json_col("$.serviceProvider.reference"), r"([^/]+)$", 1).alias("service_provider_id"),
)
write_delta(encounters, "silver_encounter")

conditions = resource_filter("Condition").select(
    json_col("$.id").alias("condition_id"),
    regexp_extract(json_col("$.subject.reference"), r"([^/]+)$", 1).alias("patient_id"),
    regexp_extract(json_col("$.encounter.reference"), r"([^/]+)$", 1).alias("encounter_id"),
    json_col("$.clinicalStatus.coding[0].display").alias("clinical_status"),
    json_col("$.code.coding[0].code").alias("condition_code"),
    json_col("$.code.coding[0].system").alias("condition_system"),
    json_col("$.code.coding[0].display").alias("condition_display"),
    json_col("$.onsetDateTime").alias("onset_datetime"),
    json_col("$.recordedDate").alias("recorded_date"),
)
write_delta(conditions, "silver_condition")

observations = resource_filter("Observation").select(
    json_col("$.id").alias("observation_id"),
    regexp_extract(json_col("$.subject.reference"), r"([^/]+)$", 1).alias("patient_id"),
    regexp_extract(json_col("$.encounter.reference"), r"([^/]+)$", 1).alias("encounter_id"),
    json_col("$.status").alias("status"),
    json_col("$.code.coding[0].code").alias("observation_code"),
    json_col("$.code.coding[0].system").alias("observation_system"),
    json_col("$.code.coding[0].display").alias("observation_display"),
    json_col("$.effectiveDateTime").alias("effective_datetime"),
    json_col("$.valueQuantity.value").cast("double").alias("value_number"),
    json_col("$.valueQuantity.unit").alias("value_unit"),
)
write_delta(observations, "silver_observation")

medications = resource_filter("MedicationRequest").select(
    json_col("$.id").alias("medication_request_id"),
    regexp_extract(json_col("$.subject.reference"), r"([^/]+)$", 1).alias("patient_id"),
    regexp_extract(json_col("$.encounter.reference"), r"([^/]+)$", 1).alias("encounter_id"),
    json_col("$.status").alias("status"),
    json_col("$.intent").alias("intent"),
    json_col("$.authoredOn").alias("authored_on"),
    json_col("$.medicationCodeableConcept.coding[0].code").alias("medication_code"),
    json_col("$.medicationCodeableConcept.coding[0].system").alias("medication_system"),
    json_col("$.medicationCodeableConcept.coding[0].display").alias("medication_display"),
)
write_delta(medications, "silver_medication")

claims = resource_filter("Claim").select(
    json_col("$.id").alias("claim_id"),
    regexp_extract(json_col("$.patient.reference"), r"([^/]+)$", 1).alias("patient_id"),
    json_col("$.status").alias("status"),
    json_col("$.type.coding[0].code").alias("claim_type_code"),
    json_col("$.type.coding[0].display").alias("claim_type_display"),
    json_col("$.created").alias("created_date"),
    regexp_extract(json_col("$.provider.reference"), r"([^/]+)$", 1).alias("provider_id"),
    json_col("$.total.value").cast("double").alias("amount"),
    json_col("$.total.currency").alias("currency"),
)
write_delta(claims, "silver_claim")

procedures = resource_filter("Procedure").select(
    json_col("$.id").alias("procedure_id"),
    regexp_extract(json_col("$.subject.reference"), r"([^/]+)$", 1).alias("patient_id"),
    regexp_extract(json_col("$.encounter.reference"), r"([^/]+)$", 1).alias("encounter_id"),
    json_col("$.status").alias("status"),
    json_col("$.code.coding[0].code").alias("procedure_code"),
    json_col("$.code.coding[0].system").alias("procedure_system"),
    json_col("$.code.coding[0].display").alias("procedure_display"),
    json_col("$.performedDateTime").alias("performed_datetime"),
)
write_delta(procedures, "silver_procedure")

organizations = resource_filter("Organization").select(
    json_col("$.id").alias("provider_id"),
    lit("Organization").alias("provider_type"),
    json_col("$.name").alias("provider_name"),
    json_col("$.address[0].city").alias("city"),
    json_col("$.address[0].state").alias("state"),
)
practitioners = resource_filter("Practitioner").select(
    json_col("$.id").alias("provider_id"),
    lit("Practitioner").alias("provider_type"),
    concat_ws(" ", json_col("$.name[0].given[0]"), json_col("$.name[0].family")).alias("provider_name"),
    lit(None).cast("string").alias("city"),
    lit(None).cast("string").alias("state"),
)
write_delta(organizations.unionByName(practitioners), "silver_provider")
