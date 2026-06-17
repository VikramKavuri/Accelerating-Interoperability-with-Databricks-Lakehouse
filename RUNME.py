# Databricks notebook source
"""Local smoke-test entry point for the FHIR-to-OMOP demo artifacts.

Databricks workspace execution is defined by ``databricks.yml`` and the
multi-task job in ``resources/jobs/fhir_to_omop_job.yml``. This file exists as a
simple local entry point for GitHub Actions and reviewers who want to regenerate
the static Vercel demo data without Databricks credentials.
"""

# COMMAND ----------

import json
import os
import sys
from pathlib import Path


try:
    PROJECT_ROOT = Path(__file__).resolve().parent
except NameError:
    PROJECT_ROOT = Path.cwd()

sys.path.insert(0, str(PROJECT_ROOT / "src"))

from healthcare_lakehouse.local_runner import run_local_pipeline  # noqa: E402


os.chdir(PROJECT_ROOT)
summary = run_local_pipeline(
    "sampledata/fhir-small",
    "demo/vercel-app/public/data",
)
print(json.dumps(summary, indent=2, sort_keys=True))
