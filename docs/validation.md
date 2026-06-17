# Validation Evidence

This project separates three kinds of proof so reviewers can understand what is
verified locally, what is displayed publicly, and what was validated in a real
Databricks workspace.

## Local Verification

Run from the repository root:

```bash
python3 scripts/run_local_pipeline.py
python3 -m unittest discover -s tests
python3 -m py_compile RUNME.py 00-README.py scripts/run_local_pipeline.py src/healthcare_lakehouse/*.py databricks/notebooks/*.py
```

The local runner uses bundled synthetic FHIR data and exports JSON tables to
`demo/vercel-app/public/data`.

Expected local evidence:

- 20 Bronze FHIR resources.
- 3 Silver patients.
- 2 Gold population-health cohort members.
- 8 quality checks.
- 1 intentional quality failure for a negative claim amount.
- Gold summary tables for condition prevalence, encounter utilization, claim
  cost, medication usage, patient summary, and population health cohorting.

## Public Demo Verification

Live demo:

```text
https://vercel-app-brown-delta.vercel.app
```

The Vercel app is a static viewer for exported sample outputs. It is useful for
portfolio review because it lets people inspect pipeline tables without
Databricks access. It is not a live Databricks workspace, an API, or a FHIR file
processor.

## Databricks Workspace Evidence

Status: verified with a successful Databricks workflow run on 2026-06-17.

The workflow ran as serverless notebook tasks and completed all six stages:

- `01_ingest_bronze`
- `02_parse_silver`
- `03_run_quality_checks`
- `04_build_omop`
- `05_publish_gold`
- `06_generate_run_summary`

The README includes sanitized evidence images generated from Databricks Jobs
API/CLI output and SQL verification results:

- `docs/images/databricks-workflow-success.png`
- `docs/images/databricks-sql-verification.png`

SQL verification against the generated Unity Catalog tables returned:

| Table | Row count |
| --- | ---: |
| `bronze_fhir_resources` | 20 |
| `silver_patient` | 3 |
| `silver_encounter` | 3 |
| `silver_condition` | 3 |
| `quality_check_results` | 8 |
| `quality_failed_records` | 1 |
| `omop_person` | 3 |
| `gold_patient_summary` | 3 |
| `gold_population_health_cohort` | 2 |

Classic job clusters were blocked by exhausted account credits. The successful
route was to use Databricks serverless workflow compute by omitting job cluster
settings from the notebook tasks.

The private workspace UI is behind Microsoft Entra login, so public README
proof should avoid login-page screenshots. If authenticated UI screenshots are
added later, keep workspace URLs, tokens, account IDs, personal email addresses,
and sensitive metadata out of the images before publishing.

Use the runbook command sequence:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run fhir_to_omop_job -t dev
```

The demo workspace has already been prepared with:

- Catalog: `adb_healthcare_fhir_omop_dev`
- Schema: `healthcare_fhir_omop_dev`
- Volume path: `/Volumes/adb_healthcare_fhir_omop_dev/healthcare_fhir_omop_dev/raw/fhir-small`
