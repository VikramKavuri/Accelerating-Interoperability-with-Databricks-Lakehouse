# Contributing

Contributions are welcome through GitHub Issues and Pull Requests.

Before opening a pull request:

- keep changes focused on the interoperability pipeline or documentation
- run `python3 -m unittest discover -s tests`
- run `python3 -m py_compile RUNME.py 00-README.py scripts/run_local_pipeline.py src/healthcare_lakehouse/*.py databricks/notebooks/*.py`
- document any Databricks workspace settings needed to reproduce the change
- avoid committing secrets, tokens, workspace URLs, or real patient data
