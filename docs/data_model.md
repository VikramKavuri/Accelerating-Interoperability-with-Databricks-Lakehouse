# Data Model

## FHIR to Silver

| FHIR resource | Silver table | Notes |
| --- | --- | --- |
| `Patient` | `silver_patient` | Patient demographics, MRN, city/state, birth date. |
| `Encounter` | `silver_encounter` | Visit status, class, type, patient reference, period, service provider. |
| `Condition` | `silver_condition` | Diagnosis/problem code, patient, encounter, clinical status, dates. |
| `Observation` | `silver_observation` | LOINC-style measurement code, value, unit, patient, encounter. |
| `MedicationRequest` | `silver_medication` | Medication code, display, status, intent, patient, encounter. |
| `Claim` | `silver_claim` | Claim type, provider, amount, currency, patient. |
| `Procedure` | `silver_procedure` | CPT/SNOMED-style procedure code, date, patient, encounter. |
| `Organization`, `Practitioner` | `silver_provider` | Provider dimension-style reference data. |

## Silver to OMOP-Style

| Silver table | OMOP-style table | Mapping |
| --- | --- | --- |
| `silver_patient` | `omop_person` | FHIR patient id becomes source id; year of birth is derived from birth date. |
| `silver_encounter` | `omop_visit_occurrence` | Encounter id maps to visit occurrence; class/type become source values. |
| `silver_condition` | `omop_condition_occurrence` | FHIR condition code/display become condition source fields. |
| `silver_observation` | `omop_measurement` | Observation code/value/unit become measurement source and value fields. |
| `silver_medication` | `omop_drug_exposure` | MedicationRequest code/display become drug source fields. |
| `silver_procedure` | `omop_procedure_occurrence` | Procedure code/display become procedure source fields. |
| `silver_claim` | `omop_payer_plan_period` | Claim type and date become payer/plan source fields. |

## Intentional Boundary

This project demonstrates OMOP-style engineering structure. It does not include the full OMOP CDM, official OHDSI vocabulary loading, concept id standardization, relationship tables, cost tables, or clinical validation. Those can be added as a second phase by adopting the Databricks `omop-cdm` accelerator pattern more deeply.
