from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from healthcare_lakehouse.fhir_io import FhirResource, load_fhir_resources, load_json_documents  # noqa: E402
from healthcare_lakehouse.local_runner import run_local_pipeline  # noqa: E402
from healthcare_lakehouse.transform import build_bronze, build_silver, run_quality_checks  # noqa: E402


class LocalFhirToOmopPipelineTest(unittest.TestCase):
    def test_pipeline_exports_expected_tables_and_quality_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = run_local_pipeline(
                PROJECT_ROOT / "sampledata" / "fhir-small",
                tmpdir,
                batch_id="unit-test",
            )

            self.assertEqual(summary["table_counts"]["bronze_fhir_resources"], 20)
            self.assertEqual(summary["table_counts"]["silver_patient"], 3)
            self.assertEqual(summary["table_counts"]["silver_encounter"], 3)
            self.assertEqual(summary["table_counts"]["silver_condition"], 3)
            self.assertEqual(summary["table_counts"]["silver_observation"], 3)
            self.assertEqual(summary["table_counts"]["silver_medication"], 2)
            self.assertEqual(summary["table_counts"]["silver_claim"], 3)
            self.assertEqual(summary["table_counts"]["omop_person"], 3)
            self.assertEqual(summary["table_counts"]["gold_population_health_cohort"], 2)
            self.assertEqual(summary["quality"]["checks_run"], 8)
            self.assertEqual(summary["quality"]["checks_failed"], 1)
            self.assertEqual(summary["quality"]["failed_record_count"], 1)

            quality_results = self._read_table(tmpdir, "quality_check_results")
            failed_checks = [row["check_name"] for row in quality_results if row["status"] == "fail"]
            self.assertEqual(failed_checks, ["claim_amount_non_negative"])

            patient_summary = self._read_table(tmpdir, "gold_patient_summary")
            patient_001 = next(row for row in patient_summary if row["patient_id"] == "patient-001")
            self.assertEqual(patient_001["condition_count"], 1)
            self.assertEqual(patient_001["valid_claim_amount"], 350.75)

    def test_loader_accepts_standalone_json_resource(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir, "patient.json")
            source.write_text(
                json.dumps(
                    {
                        "resourceType": "Patient",
                        "id": "patient-single",
                        "gender": "female",
                        "birthDate": "1985-01-10",
                    }
                ),
                encoding="utf-8",
            )

            resources = load_fhir_resources(source)

            self.assertEqual(len(resources), 1)
            self.assertEqual(resources[0].resource_type, "Patient")
            self.assertEqual(resources[0].resource_id, "patient-single")

    def test_loader_rejects_invalid_ndjson_with_line_number(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir, "bad.ndjson")
            source.write_text('{"resourceType":"Patient","id":"ok"}\n{bad json}\n', encoding="utf-8")

            with self.assertRaisesRegex(ValueError, r"bad\.ndjson:2 is not valid JSON"):
                load_json_documents(source)

    def test_quality_checks_detect_duplicates_bad_dates_and_missing_references(self) -> None:
        resources = [
            FhirResource(
                source_file="unit",
                resource_type="Patient",
                resource_id="patient-dup",
                payload={
                    "resourceType": "Patient",
                    "id": "patient-dup",
                    "birthDate": "2999-01-01",
                },
            ),
            FhirResource(
                source_file="unit",
                resource_type="Patient",
                resource_id="patient-dup",
                payload={
                    "resourceType": "Patient",
                    "id": "patient-dup",
                    "birthDate": "2999-01-01",
                },
            ),
            FhirResource(
                source_file="unit",
                resource_type="Condition",
                resource_id="condition-missing-patient",
                payload={
                    "resourceType": "Condition",
                    "id": "condition-missing-patient",
                    "subject": {"reference": "Patient/not-found"},
                    "code": {"coding": [{"code": "44054006", "display": "Diabetes mellitus"}]},
                },
            ),
        ]
        tables = build_bronze(resources, "quality-unit")
        tables.update(build_silver(tables["bronze_fhir_resources"]))
        quality = run_quality_checks(tables, "quality-unit")

        failed_checks = {
            row["check_name"]
            for row in quality["quality_check_results"]
            if row["status"] == "fail"
        }
        self.assertIn("patient_birth_date_valid", failed_checks)
        self.assertIn("duplicate_resource_check", failed_checks)
        self.assertIn("condition_has_patient", failed_checks)

    @staticmethod
    def _read_table(directory: str, table_name: str) -> list[dict]:
        return json.loads(Path(directory, f"{table_name}.json").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
