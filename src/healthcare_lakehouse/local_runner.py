"""Run the small FHIR-to-OMOP pipeline locally and export demo artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from healthcare_lakehouse.fhir_io import load_fhir_resources
from healthcare_lakehouse.transform import build_bronze, build_gold, build_omop, build_silver, run_quality_checks


DEFAULT_BATCH_ID = "local-demo-001"


def write_json_table(output_dir: Path, table_name: str, rows: list[dict]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / f"{table_name}.json").write_text(
        json.dumps(rows, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def run_local_pipeline(source: str | Path, output: str | Path, batch_id: str = DEFAULT_BATCH_ID) -> dict:
    resources = load_fhir_resources(source)
    tables = build_bronze(resources, batch_id)
    tables.update(build_silver(tables["bronze_fhir_resources"]))
    tables.update(run_quality_checks(tables, batch_id))
    tables.update(build_omop(tables))
    tables.update(build_gold(tables))

    run_summary = {
        "batch_id": batch_id,
        "source": str(source),
        "output": str(output),
        "table_counts": {name: len(rows) for name, rows in sorted(tables.items())},
        "quality": tables["quality_run_summary"][0],
    }
    tables["run_summary"] = [run_summary]

    output_dir = Path(output)
    for table_name, rows in tables.items():
        write_json_table(output_dir, table_name, rows)
    (output_dir / "manifest.json").write_text(
        json.dumps(
            {
                "batch_id": batch_id,
                "tables": sorted(tables),
                "table_counts": run_summary["table_counts"],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return run_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="sampledata/fhir-small", help="FHIR JSON/NDJSON path")
    parser.add_argument(
        "--output",
        default="demo/vercel-app/public/data",
        help="Directory where JSON tables are written",
    )
    parser.add_argument("--batch-id", default=DEFAULT_BATCH_ID)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_local_pipeline(args.source, args.output, args.batch_id)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
