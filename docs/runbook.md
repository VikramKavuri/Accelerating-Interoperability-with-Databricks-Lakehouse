# Runbook

## Local Demo Run

The local runner uses only the Python standard library. It reads `sampledata/fhir-small`, runs the same logical layers, and exports JSON tables for the Vercel demo.

```bash
python3 scripts/run_local_pipeline.py
```

Default output:

```text
demo/vercel-app/public/data/
```

Run tests:

```bash
python3 -m unittest discover -s tests
python3 -m py_compile RUNME.py 00-README.py scripts/run_local_pipeline.py src/healthcare_lakehouse/*.py databricks/notebooks/*.py
```

## Databricks Bundle Run

The job is configured for serverless notebook tasks by omitting classic cluster
settings. This avoids classic job-cluster provisioning and is the preferred
route for the small synthetic demo workload when serverless workflows are
available.

1. Upload or sync FHIR data to a workspace-accessible path such as:

```text
/Volumes/adb_healthcare_fhir_omop_dev/healthcare_fhir_omop_dev/raw/fhir-small
```

2. Configure Databricks CLI authentication.

3. Validate and deploy:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

4. Run the workflow:

```bash
databricks bundle run fhir_to_omop_job -t dev
```

Override variables when needed:

```bash
databricks bundle run fhir_to_omop_job -t dev \
  --var target_catalog=main \
  --var target_schema=healthcare_fhir_omop_dev \
  --var fhir_source_path=/Volumes/main/healthcare_fhir_omop_dev/raw/fhir-small
```

The checked-in defaults target the demo workspace catalog
`adb_healthcare_fhir_omop_dev`. Override `target_catalog` and
`fhir_source_path` when running in a different workspace.

## Vercel Demo Run

The demo is a static Vercel app that displays exported sample outputs. It is not a live Databricks connection. Refresh the data first:

```bash
python3 scripts/run_local_pipeline.py
```

Serve locally:

```bash
cd demo/vercel-app
python3 -m http.server 4173
```

Open:

```text
http://localhost:4173
```

Deploy from `demo/vercel-app` using Vercel CLI or connect that directory as a Vercel project.
