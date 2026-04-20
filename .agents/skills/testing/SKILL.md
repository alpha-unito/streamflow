---
name: StreamFlow Testing
description: This skill should be used when the user asks to "write a test", "run tests", "add a test for", "test this function", "check test coverage", "run the test suite", or when writing pytest tests for StreamFlow features or bug fixes.
version: 0.1.0
---

# StreamFlow Testing Skill

Guidelines for writing and running tests in StreamFlow.

## Test Commands

```bash
# Run full test suite
uv run make test

# Run a specific test file
uv run pytest tests/test_file.py

# Run a single test function
uv run pytest tests/test_file.py::test_function_name

# Run with coverage report
uv run make testcov

# Test specific connectors only
uv run pytest --deploys local,docker tests/test_remotepath.py

# CWL conformance tests
./cwl-conformance-test.sh
# Supports env vars: VERSION, DOCKER, EXCLUDE
```

## Requirements

Most tests require external services:

| Service | Used by |
|---|---|
| Docker | Most connector tests |
| Singularity / Apptainer | Singularity connector tests |
| Kubernetes (minikube) | Kubernetes connector tests |

All connectors are tested in CI — local runs only need the services available on the current machine.

## Writing Tests

Use pytest with async support. Tests live in `tests/`.

**New feature or bug fix:** Always add a test. For bugs, write a regression test that fails before the fix and passes after.

**Async test pattern:**

```python
import pytest

async def test_workflow_execution(context: StreamFlowContext) -> None:
    """Verify basic workflow execution completes successfully."""
    workflow = await build_workflow(context)
    result = await workflow.execute()
    assert result.status == "completed"
```

**Fixture:** The `context: StreamFlowContext` fixture is available project-wide. Import from `tests/conftest.py` or rely on auto-discovery.

**Connector-specific tests:** Use the `--deploys` flag to scope tests to available connectors:

```bash
uv run pytest --deploys local,docker,ssh tests/test_remotepath.py
```

## Coverage

Coverage reports: https://app.codecov.io/gh/alpha-unito/streamflow

Run locally with `uv run make testcov` — generates an HTML report in `htmlcov/`.
