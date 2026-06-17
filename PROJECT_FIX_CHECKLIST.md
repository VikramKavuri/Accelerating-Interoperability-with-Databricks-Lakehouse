# Project Fix Checklist

This checklist tracks the work needed to make the repository runnable, clear, and credible as an original healthcare interoperability project.

## Production Run Blockers

- [x] Add a local `RUNME.py` smoke-test entrypoint for demo artifact generation.
- [x] Add a Databricks Bundle job definition for workspace execution.
- [x] Add `requirements.txt` so dependencies are explicit.
- [x] Replace hardcoded Databricks demo catalog/schema names with configurable settings.
- [x] Provide a local sample-data default and optional external data override.
- [x] Update CI workflow notebook paths so they target files that exist.
- [x] Run notebooks end to end in Databricks using serverless workflow compute.

## Authenticity And Ownership

- [x] Replace copied/demo-style README content with project-specific documentation.
- [x] Remove placeholder repository names and missing file references.
- [x] Replace third-party image hotlinks with text architecture that lives in the README.
- [x] Replace Databricks-owned notebook text with original project code and documentation.
- [x] Remove the legacy dbignite sample from the active execution path.
- [x] Update `LICENSE`, `NOTICE`, `CONTRIBUTING.md`, and `SECURITY.md` to match this repository.
- [x] Add sanitized Databricks execution evidence images generated from the verified workspace run.

## README Quality

- [x] Make the project purpose clear in the first section.
- [x] Document prerequisites, configuration, and execution paths.
- [x] Document what the sample pipeline actually creates.
- [x] Remove unsupported claims such as production-ready HIPAA compliance, benchmarks, and model metrics.
- [x] Add known limitations so reviewers understand what requires Databricks validation.

## Remaining Workarounds

- Databricks bundle validation, deployment, and full workflow execution have been verified. Classic job clusters were blocked by exhausted credits, so the job was converted to serverless notebook tasks.
- External FHIR datasets should be treated as optional. The default run uses bundled synthetic FHIR records so the project has a reproducible baseline.
