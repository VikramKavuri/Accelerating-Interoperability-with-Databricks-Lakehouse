# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Interoperability Lakehouse
# MAGIC
# MAGIC This notebook introduces the repository workflow created by Thrivikrama Rao Kavuri.
# MAGIC The production-style Databricks workflow is defined as a Databricks Bundle in
# MAGIC `databricks.yml` and `resources/jobs/fhir_to_omop_job.yml`.
# MAGIC
# MAGIC ## What This Project Builds
# MAGIC
# MAGIC - Bronze Delta tables that preserve raw synthetic FHIR resources and lineage
# MAGIC - Silver clinical resource tables for patients, encounters, conditions, observations, claims, medications, procedures, and providers
# MAGIC - Quality result tables with failed-record evidence
# MAGIC - A focused OMOP-style subset for healthcare analytics modeling
# MAGIC - Gold tables for Databricks SQL dashboards and the public Vercel demo export
# MAGIC
# MAGIC ## Run Order
# MAGIC
# MAGIC 1. Upload or sync synthetic FHIR data to the configured workspace path.
# MAGIC 2. Validate the bundle with `databricks bundle validate -t dev`.
# MAGIC 3. Deploy and run `fhir_to_omop_job` with the Databricks CLI.
# MAGIC 4. Use `RUNME.py` only for local static demo artifact generation.
# MAGIC
# MAGIC See `README.md` for the full project documentation.
