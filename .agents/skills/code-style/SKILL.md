---
name: Code Style
description: This skill should be used when the user asks to "add docstrings", "write error handling", "use naming conventions", "handle exceptions", or when writing new Python code for StreamFlow that requires docstring format, naming conventions, exception handling, or async cleanup patterns.
version: 0.1.0
---

# Code Style Skill

Conventions for writing Python code in StreamFlow that are **not** automatically enforced by the formatter. For type annotations, see `.agents/skills/mypy/SKILL.md`.

Run auto-fix before committing:

```bash
uv run make format codespell pyupgrade
```

Exclude `streamflow/cwl/antlr` from all checks. Use American English in all code, docstrings, and comments.

## Imports: Two Manual Rules

Everything else is handled by isort automatically. Two things isort never does for you:

**1. Always add `from __future__ import annotations`** as the first import in every file:

```python
from __future__ import annotations
```

**2. Use `TYPE_CHECKING` to break circular imports:**

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.data import DataManager
```

## Naming Conventions

| Kind | Convention | Example |
|---|---|---|
| Classes | `PascalCase` | `WorkflowExecutor` |
| Functions / methods | `snake_case` | `run_workflow` |
| Constants | `UPPER_SNAKE_CASE` | `MAX_RETRIES` |
| Private members | `_prefix` | `_internal_state` |
| Type variables | Short uppercase | `_KT`, `_VT`, `_T` |

## Error Handling

Use StreamFlow's custom exceptions — never raise bare `Exception` or `RuntimeError`.

```python
from streamflow.core.exception import WorkflowExecutionException
from streamflow.log_handler import logger

try:
    result = await process()
except SpecificException as e:
    logger.exception(e)
    raise WorkflowExecutionException(f"Failed to process: {e}") from e
```

**Available exceptions** (from `streamflow.core.exception`):

| Exception | When to use |
|---|---|
| `WorkflowException` | General workflow errors |
| `WorkflowDefinitionException` | Invalid workflow definition |
| `WorkflowExecutionException` | Runtime execution failures |
| `WorkflowProvenanceException` | Provenance/tracking errors |
| `FailureHandlingException` | Fault tolerance failures |
| `InvalidPluginException` | Plugin loading/validation errors |
| `ProcessorTypeError` | Type mismatches in processors |

## Async Cleanup Pattern

Use `asyncio.gather` with `create_task` for concurrent teardown, with a `finally` block to guarantee database closure:

```python
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

## Docstrings

Use Sphinx-style field lists. Every non-trivial public function/method should have a docstring.

```python
def deploy_connector(
    self, name: str, config: ConnectorConfig, location: ExecutionLocation
) -> Connector:
    """
    Deploy a connector at the given execution location.

    :param name: Unique name for the connector instance
    :param config: Deployment configuration for the connector
    :param location: Target execution location
    :returns: The deployed connector instance
    :raises WorkflowExecutionException: If deployment fails
    """
```

- One-line summary on the first line, no blank line before it
- Blank line before `:param` block if a body paragraph follows
- Use `:returns:` (not `:return:`)
- Only document `:raises:` for exceptions callers should handle
- Do not repeat the type in the param description — types live in annotations

## See Also

- `.agents/skills/mypy/SKILL.md` — type annotations and forbidden types
- `.agents/skills/git/SKILL.md` — commit message format
