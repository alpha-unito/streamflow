---
name: Mypy no-untyped-def Fixer
description: This skill should be used when the user encounters "no-untyped-def" mypy errors, asks to "fix no-untyped-def errors", "add type annotations to functions", or mentions "Function is missing a type annotation". Provides specific workflow for fixing missing type annotations while respecting forbidden type constraints.
version: 0.1.0
---

# Mypy Sub-Skill: no-untyped-def

**Error:** `[no-untyped-def]` — function or method missing type annotations.

## Decision Process

For each function: can all parameters and the return type use concrete types?
- **YES** → fix it
- **NO** (any type needs `Any`, `dict[str, Any]`, etc.) → skip the entire function

## Workflow

1. **Identify errors:**
   ```bash
   uv run mypy path/to/file.py | grep "no-untyped-def"
   ```
2. **Classify** each function as FIXABLE or UNFIXABLE (see above)
3. **Add annotations** to FIXABLE functions only
4. **Validate & commit:** See `../SKILL.md` — General Workflow steps 3–5

## Key Patterns

```python
# Simple return types
async def close(self) -> None: ...
def is_ready(self) -> bool: ...

# Literal for fixed value sets (preferred over bare str/int)
def get_status(self) -> Literal["pending", "running", "completed", "failed"]: ...
def set_mode(self, mode: Literal["sync", "async"]) -> None: ...

# Broad arguments, narrow returns
from collections.abc import Iterable, Sequence, Mapping

def process(self, items: Iterable[Token]) -> list[str]: ...    # broad arg, narrow return
def validate(self, cfg: Mapping[str, str]) -> bool: ...        # read-only dict arg

# Optional
def get_connector(self, name: str) -> Connector | None: ...

# Self for classmethods
@classmethod
async def load(cls, persistent_id: int) -> Self: ...

# TypedDict for structured dicts (avoids dict[str, Any])
class ConnectorConfig(TypedDict):
    name: str
    enabled: bool

def get_config(self) -> ConnectorConfig: ...
```

**SKIP these — require forbidden types:**
```python
def get_config(self): return {"key": self.value}      # needs dict[str, Any]
def get_items(self): return [self.id, self.cfg]        # needs list[Any]
def save(self): return self.config                     # MutableMapping[str, Any]
```

**Common abstract argument types:** `Iterable[T]` (iteration only) · `Sequence[T]` (indexing) · `Mapping[K, V]` (read-only dict)

If any return path needs a forbidden type, skip the whole function.

## See Also

- **`../SKILL.md`** — General workflow, allowed types reference
- **AGENTS.md** — Forbidden types list, commit approval rule
