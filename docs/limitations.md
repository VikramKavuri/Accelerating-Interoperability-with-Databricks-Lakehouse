# Limitations

This is a production-style portfolio project, not a certified production healthcare platform.

## What It Demonstrates

- Medallion lakehouse modeling.
- Raw FHIR preservation and source lineage.
- JSON and NDJSON ingestion.
- Clinical resource flattening.
- Focused OMOP-style table mapping.
- Data quality checks and failed-record capture.
- Gold tables for SQL analytics.
- Databricks Bundle workflow structure.
- Vercel-hosted demo artifacts.

## What It Does Not Claim

- HIPAA compliance.
- Real PHI handling readiness.
- Complete OMOP CDM conformance.
- Official vocabulary concept-id mapping.
- Enterprise identity, access control, audit, monitoring, backup, disaster recovery, or cost governance.
- Clinical safety validation.

## Production Hardening Backlog

- Add Unity Catalog grants, service principals, and table ownership policy.
- Add secret management and environment-specific variables.
- Add streaming Auto Loader ingestion for large object storage paths.
- Add schema drift capture for unknown FHIR fields.
- Add Great Expectations or DLT expectations if the platform standard supports it.
- Add official OHDSI vocabulary loading and concept mapping.
- Add CI/CD bundle validation in GitHub Actions.
- Add Databricks job alerting and run repair guidance.
- Add Databricks SQL dashboard objects, alerts, and dashboard screenshots for a fuller BI handoff.
