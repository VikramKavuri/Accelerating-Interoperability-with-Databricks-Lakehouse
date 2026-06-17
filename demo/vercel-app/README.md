# Vercel Demo App

This static app reads exported JSON tables from `public/data`. It is a public demo of pipeline outputs, not a live Databricks workspace or a FHIR processing API.

Live demo:

https://vercel-app-brown-delta.vercel.app

Refresh the data from the repo root:

```bash
python3 scripts/run_local_pipeline.py
```

Serve locally:

```bash
cd demo/vercel-app
python3 -m http.server 4173
```

The app is intentionally static. Databricks runs the real Spark/Delta pipeline; this layer displays the small-dataset output, quality gates, sample SQL, and run evidence.
