"""FHIR file loading helpers used by the local demo pipeline.

The Databricks notebooks use Spark and Delta. These helpers intentionally stay
stdlib-only so tests and the Vercel demo artifact generator can run anywhere.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


FHIR_EXTENSIONS = {".json", ".ndjson"}


@dataclass(frozen=True)
class FhirResource:
    """A flattened FHIR resource plus source lineage."""

    source_file: str
    resource_type: str
    resource_id: str | None
    payload: dict[str, Any]


def discover_fhir_files(source_path: Path) -> list[Path]:
    """Return JSON and NDJSON files from a file or directory path."""
    if source_path.is_file():
        return [source_path]

    files = [
        path
        for path in source_path.rglob("*")
        if path.is_file() and path.suffix.lower() in FHIR_EXTENSIONS
    ]
    return sorted(files)


def load_json_documents(path: Path) -> list[dict[str, Any]]:
    """Load a JSON bundle/resource or an NDJSON file."""
    if path.suffix.lower() == ".ndjson":
        documents: list[dict[str, Any]] = []
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                documents.append(json.loads(stripped))
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number} is not valid JSON") from exc
        return documents

    try:
        return [json.loads(path.read_text(encoding="utf-8"))]
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path} is not valid JSON") from exc


def extract_resources(document: dict[str, Any], source_file: str) -> Iterable[FhirResource]:
    """Yield resources from a FHIR Bundle or standalone FHIR resource."""
    if document.get("resourceType") == "Bundle":
        for entry in document.get("entry", []):
            resource = entry.get("resource") or {}
            resource_type = resource.get("resourceType")
            if resource_type:
                yield FhirResource(
                    source_file=source_file,
                    resource_type=resource_type,
                    resource_id=resource.get("id"),
                    payload=resource,
                )
        return

    resource_type = document.get("resourceType")
    if resource_type:
        yield FhirResource(
            source_file=source_file,
            resource_type=resource_type,
            resource_id=document.get("id"),
            payload=document,
        )


def load_fhir_resources(source: str | Path) -> list[FhirResource]:
    """Load and flatten all FHIR resources under ``source``."""
    source_path = Path(source)
    resources: list[FhirResource] = []
    for file_path in discover_fhir_files(source_path):
        for document in load_json_documents(file_path):
            resources.extend(extract_resources(document, str(file_path)))
    return resources
