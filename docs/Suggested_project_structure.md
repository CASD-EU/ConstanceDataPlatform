# constance data platform project structure

We choose `python` as the main language for data transformation for constance. In this document, we propose some
best practices for python project development(e.g. `PEP-8` of python).

## 1. Project structure

The below figure shows the suggested python project structure

```text
constances_etl/
│
├── src
│   ├── scripts/
│       ├── script1_validate.py
│       ├── script2_clean.py
│       ...
│   ├── pipelines/
│       ├── pipeline1.py
│       ├── pipeline1.py
│       ...
│   ...
│
├── tests/
│   ├── unit/
│   │   ├── test_script1.py
│   │   ├── test_script2.py
│   │   ...
│   │
│   ├── integration/
│   │   ├── test_pipeline1.py
│   │   ├── test_pipeline2.py
│   │
│   ├── data/
│       ├── input/
│            ├── unit_input.csv
│            ├── integration_input.csv
│       ├── output/
│            ├── expected_unit_output.csv
│            ├── expected_integration_output.csv
│            ├── expected_metadata.json
│            └── expected_stats.csv
├── docs/       # Documentation (Sphinx, Markdown, etc.)
│   └── index.md
│
├── .gitignore
├── MANIFEST.in      # To include extra resource into the packages
├── requirements.txt      # Frozen dependencies for production
├── requirements-dev.txt  # Dev/test dependencies (e.g., pytest, flake8)
├── README.md
├── pyproject.toml        # Modern build/configuration file
```

## 2. Unit Testing Strategy

The goal of `unit testing` is to prove that each script works correctly with given controlled inputs and without dependencies on other scripts.
We recommend the [pytest](https://pypi.org/project/pytest/) framework to do the unit test automation.

### 2.1 Isolate Each Script

- Mock inputs from previous scripts.
- Mock external dependencies (databases, APIs, file reads).
- Use small synthetic datasets.

### 2.2  Test Data Design
The test dataset must be:
- **Representative**: includes valid and invalid rows, nulls, unusual date formats, edge numeric values
- **Small enough** to run quickly (< a few seconds)
- **Deterministic** — no random ordering unless fixed with seeds


### 2.3. Best practice

- Keep test data small but representative.
- For deterministic tests, fix random seeds.
- Store the input test datasets and expected results under `/tests/data/`.

## 3. Integration Testing Strategy

**Integration tests for a data pipeline** must answer three questions:

1. Do all scripts work together in sequence?
2. Is the data correctly transformed at each stage?
3. Does the final output match the expected business/technical requirements?

### 3.1 Types of Integration Tests You Should Have

- Use clean dataset, check final outputs match exactly.
- Use bad data, check if pipeline fails gracefully, or produces a validation error log.
- Use empty file input.
- Use extreme date ranges.
- Use non-ASCII characters.
- Use moderate dataset size to ensure runtime stays under a threshold.


### 3.2 Best Practices
- Keep integration test data medium-sized so full runs are quick.
- Use a temp working directory so real production data isn’t overwritten.
- Run integration tests in a controlled environment (same Python env, same data paths).