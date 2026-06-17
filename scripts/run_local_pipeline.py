#!/usr/bin/env python3
"""Convenience wrapper for running the local pipeline from the repo root."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from healthcare_lakehouse.local_runner import main  # noqa: E402


if __name__ == "__main__":
    main()
