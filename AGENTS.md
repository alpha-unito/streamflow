# StreamFlow Agent Guidelines

This document provides essential guidelines for agentic coding agents working on the StreamFlow codebase.

## Project Overview

StreamFlow is a container-native Workflow Management System (WMS) written in Python 3 (versions 3.10-3.14). It implements the Common Workflow Language (CWL) standard (v1.0-v1.3) for multi-cloud/HPC hybrid workflow executions.

**Key Architecture:**
- **Deployment** → **Service** → **Location** (hierarchical execution units)
- Supports multiple connectors: local, docker, kubernetes, ssh, slurm, pbs, singularity, etc.

## Setup & Installation

```bash
# Clone and install dependencies
git clone git@github.com:alpha-unito/streamflow.git
cd streamflow
uv sync --all-extras
```

## Essential Commands

### Testing
```bash
# Run all tests
uv run make test

# Run specific test file
uv run pytest tests/test_file.py

# Run single test function
uv run pytest tests/test_file.py::test_function_name

# Run tests with coverage
uv run make testcov

# Test specific connectors only (all tested in CI)
uv run pytest --deploys local,docker tests/test_remotepath.py
```

**Requirements:** Docker (for most connector tests), Singularity/Apptainer, Kubernetes (minikube)

### Linting & Formatting (REQUIRED BEFORE COMMIT)
```bash
# Check all (must pass before committing)
uv run make format-check flake8 codespell-check typing

# Auto-fix formatting
uv run make format codespell

# Apply pyupgrade for Python 3.10+ compatibility
uv run make pyupgrade

# Run type checking with mypy (strict mode)
uv run make typing
```

## Mandatory Agent Behavior

All agents **MUST** adhere to these non-negotiable rules:

### Package & Dependency Management (MANDATORY)

**MUST** obtain explicit user permission before installing packages or updating dependencies. Specify what is being installed/updated, why, and await confirmation before proceeding.

### ⚠️ FORBIDDEN TYPES - CRITICAL CONSTRAINT (MANDATORY)

**AGENTS MUST NEVER USE THESE TYPES:**

- ❌ `Any` (bare type)
- ❌ `object` (bare type)
- ❌ `MutableMapping[str, Any]`, `MutableSequence[Any]`
- ❌ `list[Any]`, `dict[str, Any]`, `dict[Any, Any]`, `tuple[Any, ...]`

**For validation procedures and detailed mypy workflows:** See `.claude/skills/mypy/SKILL.md`

### Git Commit Requirements (MANDATORY)

**CRITICAL: Agents MUST NEVER create git commits without explicit user approval.**

**Required sequence:**

1. **Validate changes:**
   - Run `uv run make format-check flake8 codespell-check typing` (all must pass)
   - For type changes: See `.claude/skills/mypy/SKILL.md` for validation procedures
   - Fix any failures and re-run checks before proceeding

2. **Present to user:**
   - Show complete commit message (following Git Commit Message Guidelines)
   - Show `git diff --stat` and full `git diff`
   - **Ask explicitly:** "Should I create this commit? Yes/No"
   - **Wait for explicit approval**

3. **Create commit (only after approval):**
   - If "No" → keep changes in working directory for review
   - If "Yes" → run `git commit` and verify with `git log -1`

## Code Style Guidelines

**Target:** Python 3.10-3.14 | **Line length:** 88 chars | **Format:** Black + isort | **Type checking:** mypy (strict mode) | **Exclude:** `streamflow/cwl/antlr`

### Import Organization
```python
from __future__ import annotations  # Always first

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from typing_extensions import Self  # Third-party

from streamflow.core.context import StreamFlowContext  # Local
from streamflow.log_handler import logger

if TYPE_CHECKING:  # Avoid circular imports
    from streamflow.core.data import DataManager
```

### Type Hints & Async
```python
# Always use concrete type hints
def process_workflow(
    self,
    workflow: Workflow,
    config: WorkflowConfig  # Concrete type, not dict[str, Any]
) -> bool:  # Concrete return type
    pass

# Use Self for classmethods
@classmethod
async def load(cls, context: StreamFlowContext) -> Self:
    pass

# Proper async cleanup
async def close(self) -> None:
    try:
        await asyncio.gather(
            asyncio.create_task(self.manager.close()),
            asyncio.create_task(self.scheduler.close()),
        )
    except Exception as e:
        logger.exception(e)
    finally:
        await self.database.close()
```

### Naming & Error Handling
- **Classes:** `PascalCase` | **Functions:** `snake_case` | **Constants:** `UPPER_SNAKE_CASE`
- **Private:** `_method_name` | **Type vars:** `_KT`, `_VT`

```python
# Use custom exceptions from streamflow.core.exception
from streamflow.core.exception import WorkflowExecutionException
from streamflow.log_handler import logger

try:
    result = await process()
except SpecificException as e:
    logger.exception(e)
    raise WorkflowExecutionException(f"Failed: {e}") from e
```

**Available exceptions:** `ProcessorTypeError`, `WorkflowException`, `WorkflowDefinitionException`, `WorkflowExecutionException`, `WorkflowProvenanceException`, `FailureHandlingException`, `InvalidPluginException`

### Documentation

**For Sphinx documentation (.rst files):** See `.claude/skills/docs/SKILL.md` for comprehensive guidelines on writing reStructuredText documentation.

**Python docstrings:** Use standard Python docstring format with field lists:

```python
def process_workflow(workflow: Workflow, config: dict[str, Any]) -> bool:
    """
    Process a workflow with the given configuration.

    :param workflow: The workflow to process
    :param config: Configuration dictionary for processing
    :returns: True if processing succeeded, False otherwise
    :raises WorkflowExecutionException: If workflow processing fails
    """
    pass
```

**Language:** American English for all documentation and docstrings.

### Testing (REQUIRED for new features/bugfixes)
```python
# Use pytest with async support
async def test_workflow_execution(context: StreamFlowContext) -> None:
    """Test basic workflow execution."""
    workflow = await build_workflow(context)
    result = await workflow.execute()
    assert result.status == "completed"
```

**Coverage:** https://app.codecov.io/gh/alpha-unito/streamflow

## Git Commit Message Guidelines

**Format:**
```
<type>(<scope>): <subject>

<body>
```

**Types:** `Add`, `Fix`, `Refactor`, `Update`, `Remove`, `Bump`, `Docs`, `Test`, `Chore`

**Rules:**
- **Subject:** Imperative mood, capitalize, no period, max 50 chars
- **Scope (optional):** Module/component (e.g., `cwl`, `deployment`, `scheduling`)
- **Body (required):** Explain *what* and *why* (not *how*), wrap at 72 chars, separate with blank line, include issue refs (e.g., `Fixes #123`). Exception: trivial changes like typo fixes.
- **Language:** American English

**Examples:**
```
Add restore method to DataManager

Implement restore method to enable workflow recovery from checkpoints.
This allows jobs to resume from the last completed step.

Fix SSH connector authentication timeout (Fixes #931)

Increase default timeout for SSH authentication from 5s to 30s to handle
slow networks and high-latency connections.

Bump kubernetes-asyncio from 33.3.0 to 34.3.3
```

## Common Workflows

**Adding a feature:**
1. Write tests first in `tests/`
2. Implement feature with type hints and docstrings
3. Run `uv run make format` to auto-format
4. Run `uv run make format-check flake8 codespell-check typing`
5. Run `uv run pytest` to verify tests pass
6. Update docs if needed
7. Follow Git Commit Requirements section for submitting changes

**Fixing a bug:**
1. Add regression test in `tests/`
2. Fix the bug with proper type annotations
3. Follow linting/formatting guidelines (steps 3-4 above)
4. Verify with tests
5. Follow Git Commit Requirements section for submitting changes

**For mypy type error fixes:** See `.claude/skills/mypy/SKILL.md`

## Key Project Structure

```
streamflow/
├── core/           # Abstractions (context, deployment, exception, workflow)
├── cwl/            # CWL implementation (v1.0-v1.3)
├── deployment/     # Connectors (docker, k8s, ssh, slurm, pbs, singularity)
├── workflow/       # Workflow execution engine
├── data/           # Data management
├── persistence/    # Database (SQLite)
├── scheduling/     # Scheduling policies
├── recovery/       # Checkpointing/fault tolerance
└── ext/            # Plugin system
tests/              # Pytest test suite
docs/               # Sphinx documentation
```

## Quick Reference

**Extension Points:** Connector, BindingFilter, CWLDockerTranslator, Scheduler, Database, DataManager, CheckpointManager, FailureManager

**CWL Conformance:** `./cwl-conformance-test.sh` (supports VERSION, DOCKER, EXCLUDE env vars)

**Documentation:** `uv run make html` | Update checksum: `cd docs && uv run make checksum`

**Resources:** [Website](https://streamflow.di.unito.it/) | [Docs](https://streamflow.di.unito.it/documentation/0.2/) | [GitHub](https://github.com/alpha-unito/streamflow) | [Contributing](CONTRIBUTING.md)
