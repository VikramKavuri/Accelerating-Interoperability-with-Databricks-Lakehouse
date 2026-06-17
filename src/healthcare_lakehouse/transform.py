"""Local FHIR-to-OMOP-style transformations for the portfolio demo."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from typing import Any

from healthcare_lakehouse.fhir_io import FhirResource


Row = dict[str, Any]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stable_int(value: str | None, modulus: int = 2_147_483_647) -> int | None:
    if not value:
        return None
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:12], 16) % modulus


def raw_hash(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def reference_id(reference: Any) -> str | None:
    if isinstance(reference, dict):
        reference = reference.get("reference")
    if not isinstance(reference, str):
        return None
    return reference.split("/")[-1] if "/" in reference else reference


def coding(resource: dict[str, Any], path: str = "code") -> dict[str, Any]:
    node: Any = resource
    for part in path.split("."):
        if isinstance(node, list):
            node = node[0] if node else {}
        if not isinstance(node, dict):
            return {}
        node = node.get(part)
    if isinstance(node, list):
        node = node[0] if node else {}
    if isinstance(node, dict):
        codes = node.get("coding") or []
        if codes:
            return codes[0]
        return {"display": node.get("text")}
    return {}


def display_from_codeable(resource: dict[str, Any], path: str = "code") -> str | None:
    code = coding(resource, path)
    return code.get("display") or code.get("code") or resource.get(path, {}).get("text")


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def birth_year(birth_date: str | None) -> int | None:
    if not birth_date:
        return None
    try:
        return datetime.strptime(birth_date[:10], "%Y-%m-%d").date().year
    except ValueError:
        return None


def age_group(birth_date: str | None, as_of_year: int = 2026) -> str:
    year = birth_year(birth_date)
    if year is None:
        return "unknown"
    age = as_of_year - year
    if age < 18:
        return "0-17"
    if age < 35:
        return "18-34"
    if age < 50:
        return "35-49"
    if age < 65:
        return "50-64"
    return "65+"


def build_bronze(resources: list[FhirResource], batch_id: str) -> dict[str, list[Row]]:
    ingestion_time = now_utc()
    rows = [
        {
            "batch_id": batch_id,
            "source_file": resource.source_file,
            "resource_type": resource.resource_type,
            "resource_id": resource.resource_id,
            "raw_resource_json": resource.payload,
            "raw_hash": raw_hash(resource.payload),
            "ingested_at": ingestion_time,
            "parse_status": "parsed",
        }
        for resource in resources
    ]
    counts = Counter(row["resource_type"] for row in rows)
    audit = [
        {
            "batch_id": batch_id,
            "source_file_count": len({row["source_file"] for row in rows}),
            "resource_count": len(rows),
            "resource_type": resource_type,
            "resource_type_count": count,
            "ingested_at": ingestion_time,
        }
        for resource_type, count in sorted(counts.items())
    ]
    return {
        "bronze_fhir_resources": rows,
        "bronze_ingestion_audit": audit,
        "bronze_rejected_records": [],
    }


def _resources_by_type(bronze_rows: list[Row], resource_type: str) -> list[dict[str, Any]]:
    return [
        row["raw_resource_json"]
        for row in bronze_rows
        if row["resource_type"] == resource_type
    ]


def parse_patient(resource: dict[str, Any]) -> Row:
    name = (resource.get("name") or [{}])[0]
    address = (resource.get("address") or [{}])[0]
    identifiers = resource.get("identifier") or []
    mrn = next(
        (
            item.get("value")
            for item in identifiers
            if (item.get("type") or {}).get("text") in {"MRN", "Medical record number"}
        ),
        None,
    )
    return {
        "patient_id": resource.get("id"),
        "mrn": mrn,
        "first_name": ((name.get("given") or [None])[0]),
        "last_name": name.get("family"),
        "gender": resource.get("gender"),
        "birth_date": resource.get("birthDate"),
        "deceased": bool(resource.get("deceasedBoolean", False)),
        "city": address.get("city"),
        "state": address.get("state"),
        "postal_code": address.get("postalCode"),
    }


def parse_encounter(resource: dict[str, Any]) -> Row:
    period = resource.get("period") or {}
    enc_type = coding(resource, "type")
    return {
        "encounter_id": resource.get("id"),
        "patient_id": reference_id(resource.get("subject")),
        "status": resource.get("status"),
        "encounter_class": (resource.get("class") or {}).get("code"),
        "encounter_type_code": enc_type.get("code"),
        "encounter_type_display": enc_type.get("display"),
        "start_datetime": period.get("start"),
        "end_datetime": period.get("end"),
        "service_provider_id": reference_id(resource.get("serviceProvider")),
    }


def parse_condition(resource: dict[str, Any]) -> Row:
    code = coding(resource)
    return {
        "condition_id": resource.get("id"),
        "patient_id": reference_id(resource.get("subject")),
        "encounter_id": reference_id(resource.get("encounter")),
        "clinical_status": display_from_codeable(resource, "clinicalStatus"),
        "condition_code": code.get("code"),
        "condition_system": code.get("system"),
        "condition_display": code.get("display"),
        "onset_datetime": resource.get("onsetDateTime"),
        "recorded_date": resource.get("recordedDate"),
    }


def parse_observation(resource: dict[str, Any]) -> Row:
    code = coding(resource)
    value_quantity = resource.get("valueQuantity") or {}
    return {
        "observation_id": resource.get("id"),
        "patient_id": reference_id(resource.get("subject")),
        "encounter_id": reference_id(resource.get("encounter")),
        "status": resource.get("status"),
        "observation_code": code.get("code"),
        "observation_system": code.get("system"),
        "observation_display": code.get("display"),
        "effective_datetime": resource.get("effectiveDateTime"),
        "value_number": parse_float(value_quantity.get("value")),
        "value_unit": value_quantity.get("unit") or value_quantity.get("code"),
    }


def parse_medication(resource: dict[str, Any]) -> Row:
    medication = resource.get("medicationCodeableConcept") or {}
    code = coding({"code": medication})
    return {
        "medication_request_id": resource.get("id"),
        "patient_id": reference_id(resource.get("subject")),
        "encounter_id": reference_id(resource.get("encounter")),
        "status": resource.get("status"),
        "intent": resource.get("intent"),
        "authored_on": resource.get("authoredOn"),
        "medication_code": code.get("code"),
        "medication_system": code.get("system"),
        "medication_display": code.get("display") or medication.get("text"),
    }


def parse_claim(resource: dict[str, Any]) -> Row:
    claim_type = coding(resource, "type")
    total = resource.get("total") or {}
    return {
        "claim_id": resource.get("id"),
        "patient_id": reference_id(resource.get("patient")),
        "status": resource.get("status"),
        "claim_type_code": claim_type.get("code"),
        "claim_type_display": claim_type.get("display"),
        "created_date": resource.get("created"),
        "provider_id": reference_id(resource.get("provider")),
        "amount": parse_float(total.get("value")),
        "currency": total.get("currency"),
    }


def parse_procedure(resource: dict[str, Any]) -> Row:
    code = coding(resource)
    return {
        "procedure_id": resource.get("id"),
        "patient_id": reference_id(resource.get("subject")),
        "encounter_id": reference_id(resource.get("encounter")),
        "status": resource.get("status"),
        "procedure_code": code.get("code"),
        "procedure_system": code.get("system"),
        "procedure_display": code.get("display"),
        "performed_datetime": resource.get("performedDateTime"),
    }


def parse_provider(resource: dict[str, Any]) -> Row:
    resource_type = resource.get("resourceType")
    if resource_type == "Practitioner":
        name = (resource.get("name") or [{}])[0]
        return {
            "provider_id": resource.get("id"),
            "provider_type": "Practitioner",
            "provider_name": " ".join(
                part for part in [*((name.get("given") or [])), name.get("family")] if part
            ),
            "city": None,
            "state": None,
        }
    address = (resource.get("address") or [{}])[0]
    return {
        "provider_id": resource.get("id"),
        "provider_type": "Organization",
        "provider_name": resource.get("name"),
        "city": address.get("city"),
        "state": address.get("state"),
    }


def build_silver(bronze_rows: list[Row]) -> dict[str, list[Row]]:
    patients = [parse_patient(resource) for resource in _resources_by_type(bronze_rows, "Patient")]
    encounters = [
        parse_encounter(resource) for resource in _resources_by_type(bronze_rows, "Encounter")
    ]
    conditions = [
        parse_condition(resource) for resource in _resources_by_type(bronze_rows, "Condition")
    ]
    observations = [
        parse_observation(resource) for resource in _resources_by_type(bronze_rows, "Observation")
    ]
    medications = [
        parse_medication(resource)
        for resource in _resources_by_type(bronze_rows, "MedicationRequest")
    ]
    claims = [parse_claim(resource) for resource in _resources_by_type(bronze_rows, "Claim")]
    procedures = [
        parse_procedure(resource) for resource in _resources_by_type(bronze_rows, "Procedure")
    ]
    providers = [
        parse_provider(resource)
        for resource in [
            *_resources_by_type(bronze_rows, "Organization"),
            *_resources_by_type(bronze_rows, "Practitioner"),
        ]
    ]
    return {
        "silver_patient": patients,
        "silver_encounter": encounters,
        "silver_condition": conditions,
        "silver_observation": observations,
        "silver_medication": medications,
        "silver_claim": claims,
        "silver_procedure": procedures,
        "silver_provider": providers,
    }


def run_quality_checks(tables: dict[str, list[Row]], batch_id: str) -> dict[str, list[Row]]:
    checks: list[tuple[str, str, list[Row]]] = []
    patients = tables["silver_patient"]
    patient_ids = {row["patient_id"] for row in patients if row.get("patient_id")}
    encounter_ids = {
        row["encounter_id"] for row in tables["silver_encounter"] if row.get("encounter_id")
    }
    resource_keys = [
        (row.get("resource_type"), row.get("resource_id"))
        for row in tables["bronze_fhir_resources"]
        if row.get("resource_id")
    ]
    duplicates = [
        {"resource_type": resource_type, "resource_id": resource_id, "duplicate_count": count}
        for (resource_type, resource_id), count in Counter(resource_keys).items()
        if count > 1
    ]

    checks.append(("patient_id_not_null", "silver_patient", [row for row in patients if not row.get("patient_id")]))
    checks.append(
        (
            "patient_birth_date_valid",
            "silver_patient",
            [
                row
                for row in patients
                if row.get("birth_date")
                and not _is_valid_date(row["birth_date"])
            ],
        )
    )
    checks.append(
        (
            "encounter_has_patient",
            "silver_encounter",
            [row for row in tables["silver_encounter"] if row.get("patient_id") not in patient_ids],
        )
    )
    checks.append(
        (
            "condition_has_patient",
            "silver_condition",
            [row for row in tables["silver_condition"] if row.get("patient_id") not in patient_ids],
        )
    )
    checks.append(
        (
            "observation_has_code",
            "silver_observation",
            [row for row in tables["silver_observation"] if not row.get("observation_code")],
        )
    )
    checks.append(
        (
            "claim_amount_non_negative",
            "silver_claim",
            [
                row
                for row in tables["silver_claim"]
                if row.get("amount") is not None and row["amount"] < 0
            ],
        )
    )
    checks.append(("duplicate_resource_check", "bronze_fhir_resources", duplicates))
    checks.append(
        (
            "referential_integrity_check",
            "clinical_references",
            [
                row
                for table_name in [
                    "silver_condition",
                    "silver_observation",
                    "silver_medication",
                    "silver_procedure",
                ]
                for row in tables[table_name]
                if row.get("encounter_id") and row.get("encounter_id") not in encounter_ids
            ],
        )
    )

    checked_at = now_utc()
    results = [
        {
            "batch_id": batch_id,
            "check_name": name,
            "table_name": table_name,
            "failed_count": len(failures),
            "status": "pass" if not failures else "fail",
            "checked_at": checked_at,
        }
        for name, table_name, failures in checks
    ]
    failed_records = [
        {
            "batch_id": batch_id,
            "check_name": name,
            "table_name": table_name,
            "failed_record": failure,
            "checked_at": checked_at,
        }
        for name, table_name, failures in checks
        for failure in failures
    ]
    summary = [
        {
            "batch_id": batch_id,
            "checks_run": len(results),
            "checks_passed": sum(1 for row in results if row["status"] == "pass"),
            "checks_failed": sum(1 for row in results if row["status"] == "fail"),
            "failed_record_count": len(failed_records),
            "checked_at": checked_at,
        }
    ]
    return {
        "quality_check_results": results,
        "quality_failed_records": failed_records,
        "quality_run_summary": summary,
    }


def _is_valid_date(value: str) -> bool:
    try:
        parsed = datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return False
    return parsed <= date.today()


def build_omop(tables: dict[str, list[Row]]) -> dict[str, list[Row]]:
    person = [
        {
            "person_id": stable_int(row["patient_id"]),
            "fhir_patient_id": row["patient_id"],
            "gender_source_value": row.get("gender"),
            "year_of_birth": birth_year(row.get("birth_date")),
            "birth_datetime": row.get("birth_date"),
            "race_source_value": None,
            "ethnicity_source_value": None,
            "location_source_value": ", ".join(
                part for part in [row.get("city"), row.get("state")] if part
            ),
        }
        for row in tables["silver_patient"]
    ]
    visit = [
        {
            "visit_occurrence_id": stable_int(row["encounter_id"]),
            "person_id": stable_int(row["patient_id"]),
            "fhir_encounter_id": row["encounter_id"],
            "visit_concept_code": row.get("encounter_class"),
            "visit_source_value": row.get("encounter_type_display"),
            "visit_start_datetime": row.get("start_datetime"),
            "visit_end_datetime": row.get("end_datetime"),
        }
        for row in tables["silver_encounter"]
    ]
    condition = [
        {
            "condition_occurrence_id": stable_int(row["condition_id"]),
            "person_id": stable_int(row["patient_id"]),
            "visit_occurrence_id": stable_int(row.get("encounter_id")),
            "condition_source_code": row.get("condition_code"),
            "condition_source_value": row.get("condition_display"),
            "condition_start_datetime": row.get("onset_datetime") or row.get("recorded_date"),
        }
        for row in tables["silver_condition"]
    ]
    measurement = [
        {
            "measurement_id": stable_int(row["observation_id"]),
            "person_id": stable_int(row["patient_id"]),
            "visit_occurrence_id": stable_int(row.get("encounter_id")),
            "measurement_source_code": row.get("observation_code"),
            "measurement_source_value": row.get("observation_display"),
            "measurement_datetime": row.get("effective_datetime"),
            "value_as_number": row.get("value_number"),
            "unit_source_value": row.get("value_unit"),
        }
        for row in tables["silver_observation"]
    ]
    drug = [
        {
            "drug_exposure_id": stable_int(row["medication_request_id"]),
            "person_id": stable_int(row["patient_id"]),
            "visit_occurrence_id": stable_int(row.get("encounter_id")),
            "drug_source_code": row.get("medication_code"),
            "drug_source_value": row.get("medication_display"),
            "drug_exposure_start_datetime": row.get("authored_on"),
        }
        for row in tables["silver_medication"]
    ]
    procedure = [
        {
            "procedure_occurrence_id": stable_int(row["procedure_id"]),
            "person_id": stable_int(row["patient_id"]),
            "visit_occurrence_id": stable_int(row.get("encounter_id")),
            "procedure_source_code": row.get("procedure_code"),
            "procedure_source_value": row.get("procedure_display"),
            "procedure_datetime": row.get("performed_datetime"),
        }
        for row in tables["silver_procedure"]
    ]
    payer = [
        {
            "payer_plan_period_id": stable_int(row["claim_id"]),
            "person_id": stable_int(row["patient_id"]),
            "payer_source_value": row.get("claim_type_display"),
            "plan_source_value": row.get("claim_type_code"),
            "period_start_date": row.get("created_date"),
        }
        for row in tables["silver_claim"]
    ]
    return {
        "omop_person": person,
        "omop_visit_occurrence": visit,
        "omop_condition_occurrence": condition,
        "omop_measurement": measurement,
        "omop_drug_exposure": drug,
        "omop_procedure_occurrence": procedure,
        "omop_payer_plan_period": payer,
    }


def build_gold(tables: dict[str, list[Row]]) -> dict[str, list[Row]]:
    patients_by_id = {row["patient_id"]: row for row in tables["silver_patient"]}

    patient_summary = [
        {
            "patient_id": patient_id,
            "age_group": age_group(patient.get("birth_date")),
            "gender": patient.get("gender"),
            "condition_count": sum(
                1 for row in tables["silver_condition"] if row.get("patient_id") == patient_id
            ),
            "encounter_count": sum(
                1 for row in tables["silver_encounter"] if row.get("patient_id") == patient_id
            ),
            "medication_count": sum(
                1 for row in tables["silver_medication"] if row.get("patient_id") == patient_id
            ),
            "valid_claim_amount": round(
                sum(
                    row.get("amount") or 0
                    for row in tables["silver_claim"]
                    if row.get("patient_id") == patient_id and (row.get("amount") or 0) >= 0
                ),
                2,
            ),
        }
        for patient_id, patient in sorted(patients_by_id.items())
    ]

    prevalence_counter: Counter[tuple[str, str]] = Counter()
    for condition in tables["silver_condition"]:
        patient = patients_by_id.get(condition.get("patient_id"), {})
        prevalence_counter[
            (
                condition.get("condition_display") or "unknown",
                age_group(patient.get("birth_date")),
            )
        ] += 1

    condition_prevalence = [
        {
            "condition_display": condition_display,
            "age_group": group,
            "patient_count": count,
        }
        for (condition_display, group), count in sorted(prevalence_counter.items())
    ]

    encounter_counter: Counter[str] = Counter()
    for encounter in tables["silver_encounter"]:
        month = (encounter.get("start_datetime") or "unknown")[:7]
        encounter_counter[month] += 1
    encounter_utilization = [
        {"encounter_month": month, "encounter_count": count}
        for month, count in sorted(encounter_counter.items())
    ]

    condition_by_patient = {
        row["patient_id"]: row.get("condition_display") or "unmapped condition"
        for row in tables["silver_condition"]
        if row.get("patient_id")
    }
    costs: defaultdict[str, float] = defaultdict(float)
    claim_counts: Counter[str] = Counter()
    for claim in tables["silver_claim"]:
        amount = claim.get("amount")
        if amount is None or amount < 0:
            continue
        condition = condition_by_patient.get(claim.get("patient_id"), "no linked condition")
        costs[condition] += amount
        claim_counts[condition] += 1
    claim_cost_summary = [
        {
            "condition_display": condition,
            "claim_count": claim_counts[condition],
            "total_claim_amount": round(amount, 2),
            "average_claim_amount": round(amount / claim_counts[condition], 2),
        }
        for condition, amount in sorted(costs.items())
    ]

    medication_usage_counter: Counter[tuple[str, str]] = Counter()
    for medication in tables["silver_medication"]:
        condition = condition_by_patient.get(medication.get("patient_id"), "no linked condition")
        medication_usage_counter[
            (medication.get("medication_display") or "unknown medication", condition)
        ] += 1
    medication_usage = [
        {
            "medication_display": medication,
            "condition_display": condition,
            "request_count": count,
        }
        for (medication, condition), count in sorted(medication_usage_counter.items())
    ]

    cohort = []
    for patient_id, patient in patients_by_id.items():
        condition_names = {
            (row.get("condition_display") or "").lower()
            for row in tables["silver_condition"]
            if row.get("patient_id") == patient_id
        }
        if any("diabetes" in name or "hypertens" in name for name in condition_names):
            cohort.append(
                {
                    "patient_id": patient_id,
                    "age_group": age_group(patient.get("birth_date")),
                    "gender": patient.get("gender"),
                    "cohort_reason": ", ".join(sorted(condition_names)),
                }
            )

    return {
        "gold_patient_summary": patient_summary,
        "gold_condition_prevalence": condition_prevalence,
        "gold_encounter_utilization": encounter_utilization,
        "gold_claim_cost_summary": claim_cost_summary,
        "gold_medication_usage": medication_usage,
        "gold_population_health_cohort": cohort,
    }
