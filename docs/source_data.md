# Source Data Strategy

## Bundled Tiny Sample

`sampledata/fhir-small` contains a tiny synthetic FHIR bundle plus NDJSON resources. It is intentionally small enough for unit tests and the Vercel demo.

Included resources:

- Patient
- Encounter
- Condition
- Observation
- MedicationRequest
- Claim
- Procedure
- Organization
- Practitioner

One claim has a negative amount so the quality layer can demonstrate failed-record capture.

## External Demo Data

Use external datasets for larger validation, but do not commit them to Git:

- Kaggle Synthea FHIR JSON dataset: `https://www.kaggle.com/datasets/krsna540/synthea-dataset-jsons-ehr`
- Kaggle FHIR 1k sample: `https://www.kaggle.com/datasets/drscarlat/fhir-1ksample`

Suggested local staging:

```text
external-data/
  kaggle-fhir-1k/
  synthea-jsons-ehr/
```

Suggested Databricks staging:

```text
/Volumes/adb_healthcare_fhir_omop_dev/healthcare_fhir_omop_dev/raw/fhir-1k
/Volumes/adb_healthcare_fhir_omop_dev/healthcare_fhir_omop_dev/raw/synthea-jsons-ehr
```

Keep synthetic-data licensing notes and dataset download instructions in project documentation instead of checking large source files into the repository.
