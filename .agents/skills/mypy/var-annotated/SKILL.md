---
name: Mypy var-annotated Fixer
description: This skill should be used when the user encounters "var-annotated" mypy errors, asks to "fix var-annotated errors", "add type annotations to variables", or mentions "Need type annotation for" errors. Provides workflow for fixing missing variable type annotations while respecting forbidden type constraints (no Any, dict[str, Any], list[Any], etc.).
version: 0.1.0
---

# Mypy Sub-Skill: var-annotated

**Error:** `[var-annotated]` — mypy cannot infer a variable's type. Fix: add an explicit annotation (`variable: Type = value`). If the correct type requires forbidden types, skip the error.

## Workflow

1. **Read surrounding code** — what gets stored/retrieved from the variable?
2. **Determine the concrete type** from usage, signatures, or similar patterns nearby
3. **Check:** does the type need `Any` or other forbidden types? If YES → skip
4. **Apply:** `variable: ConcreteType = value`
5. **Validate** — must return no output:
   ```bash
   git diff | grep -E "Any\b|dict\[.*Any"
   uv run make format-check flake8 codespell-check typing
   ```

## Fix Patterns

**Empty collections** — examine what gets stored:
```python
cache: dict[str, Port] = {}
items: list[Token] = []
ports: dict[str, list[str]] = {}
```

**Generic types** — specify type parameters:
```python
future: asyncio.Future[str] = asyncio.Future()
cache: LRUCache[int, int] = LRUCache(maxsize=5)
```

For generic type parameters, prefer covariant alternatives when you only need read access: use `Sequence[T]` over `MutableSequence[T]`, `Mapping[K, V]` over `MutableMapping[K, V]`. Check [typeshed](https://github.com/python/typeshed) for variance of standard library types.

**StreamFlow objects** — import the concrete type:
```python
from streamflow.core.workflow import Port, Token
from streamflow.core.deployment import ExecutionLocation

storage: dict[str, Token] = {}
```

Common StreamFlow types: `Port`, `Token` (`streamflow.core.workflow`) · `Storage` (`streamflow.data.utils`) · `ExecutionLocation` (`streamflow.core.deployment`)

**Nested dicts with known keys — prefer TypedDict:**
```python
from typing import TypedDict

class Config(TypedDict):
    name: str
    count: int

config: Config = {"name": "test", "count": 42}

# Only use generic dict when keys are truly dynamic:
extensions: dict[str, dict[str, str]] = {}
```

Don't guess types — always verify by reading code. Don't use bare generics (`list` instead of `list[str]`).

## See Also

- **`../SKILL.md`** — General workflow, allowed types reference
- **AGENTS.md** — Forbidden types list, commit approval rule
