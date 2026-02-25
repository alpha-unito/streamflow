---
name: Mypy no-untyped-def Fixer
description: This skill should be used when the user encounters "no-untyped-def" mypy errors, asks to "fix no-untyped-def errors", "add type annotations to functions", or mentions "Function is missing a type annotation". Provides specific workflow for fixing missing type annotations while respecting forbidden type constraints.
version: 0.1.0
---

# Mypy Sub-Skill: Fixing no-untyped-def Errors

**Error Code:** `[no-untyped-def]` - Function is missing type annotations for parameters or return type.

## Decision Process

For each function with this error:
1. **Can return type use concrete types?** (e.g., `None`, `bool`, `Connector`, `str | None`) → If NO, skip function
2. **Can all parameters use concrete types?** → If NO, skip function  
3. **Both YES?** → Function is FIXABLE, proceed to add annotations

**Key rule:** If ANY type needs forbidden types (`Any`, `dict[str, Any]`, etc.), skip the entire function.

## Workflow

### 1. Identify & Classify
```bash
uv run mypy path/to/file.py | grep "no-untyped-def"
```
Create two lists: **FIXABLE** (concrete types only) vs **UNFIXABLE** (need forbidden types - skip these)

### 2. Add Type Annotations
For FIXABLE functions only, add concrete types to parameters and return values.

### 3. Validate & Quality Check
```bash
# Check for forbidden types (must return no matches)
git diff | grep -E "Any\b|MutableMapping\\[.*Any|MutableSequence\\[.*Any|list\\[Any|dict\\[.*Any"

# Run all quality checks (must pass)
uv run make format-check flake8 codespell-check typing
```

### 4. Commit
See AGENTS.md "Git Commit Requirements" and "Git Commit Message Guidelines"

## Key Patterns

**Fixable Examples:**
```python
# Async functions returning None
async def close(self) -> None:
    await self.manager.close()

# Boolean checks
def is_ready(self) -> bool:
    return bool(self.status == "ready")

# Literal types for fixed value sets (PREFERRED over generic str/int)
from typing import Literal

def get_status(self) -> Literal["pending", "running", "completed", "failed"]:
    return self.status

def set_mode(self, mode: Literal["sync", "async"]) -> None:
    self.mode = mode

def get_connector_type(self) -> Literal["docker", "kubernetes", "ssh", "slurm"]:
    return self.connector_type

# Broad arguments, narrow returns (container types)
from collections.abc import Iterable, Sequence, Mapping

def process_connectors(self, connectors: Iterable[Connector]) -> list[str]:
    """Argument: Iterable (broad), Return: list (narrow)"""
    return [c.name for c in connectors]

def filter_configs(self, configs: Sequence[Config]) -> list[Config]:
    """Argument: Sequence (broad - supports indexing), Return: list (narrow)"""
    return [c for c in configs if c.enabled]

def validate_settings(self, settings: Mapping[str, str]) -> bool:
    """Argument: Mapping (broad - read-only), Return: bool (narrow)"""
    return "host" in settings and "port" in settings

def get_config_dict(self) -> dict[str, str]:
    """Return: dict (narrow - specific mutable mapping)"""
    return {"host": self.host, "port": str(self.port)}

# Optional returns
def get_connector(self, name: str) -> Connector | None:
    return self.connectors.get(name)

# Classmethods with Self
@classmethod
async def load(cls, context: StreamFlowContext) -> Self:
    instance = cls(context)
    await instance.initialize()
    return instance

# Multiple parameters with Literal types
def deploy(
    self, 
    name: str, 
    config: DeploymentConfig, 
    location: Location,
    mode: Literal["foreground", "background"] = "foreground"
) -> Connector:
    connector = self.create_connector(name, config)
    connector.deploy(location, mode)
    return connector
```

**Unfixable Examples (SKIP these):**
```python
# Dynamic dict - requires dict[str, Any]
def get_config(self):
    return {"name": self.name, "config": self.config}  # SKIP

# Mixed list - requires list[Any]
def get_items(self):
    return [self.id, self.config, self.status]  # SKIP

# Generic mapping from interface - requires MutableMapping[str, Any]
def save(self):
    return self.config  # self.config is MutableMapping[str, Any] - SKIP
```

## Quick Tips

**Prefer `Literal` over generic types:**
```python
# GOOD - Specific literal values
def set_status(self, status: Literal["active", "inactive"]) -> None:
    self.status = status

# LESS GOOD - Generic str allows any value
def set_status(self, status: str) -> None:
    self.status = status
```

**Broad arguments, narrow returns:**
```python
from collections.abc import Iterable, Sequence, Mapping

# GOOD - Accepts any iterable, returns specific list
def pretty_print(array: Iterable[str]) -> list[str]:
    for i in array:
        print(i)
    return [i for i in array]

# LESS GOOD - Requires list, but only uses iteration
def pretty_print(array: list[str]) -> list[str]:
    for i in array:
        print(i)
    return [i for i in array]
```

**Common abstract types for arguments:**
- `Iterable[T]` - Only iteration needed (broadest)
- `Sequence[T]` - Indexing + iteration (list-like, read-only)
- `Mapping[K, V]` - Read-only dict-like access

**TypedDict for structured dicts:**
```python
from typing import TypedDict

class ConnectorConfig(TypedDict):
    name: str
    type: str
    enabled: bool

def get_config(self) -> ConnectorConfig:
    return {"name": "docker", "type": "container", "enabled": True}
```

**Complex control flow:** Analyze ALL return paths. If any path needs forbidden types, skip the function.

**New mypy errors after adding types?** Either fix them if simple, or revert if they indicate forbidden types are needed.

## See Also

Parent skill: `../SKILL.md` | AGENTS.md: Forbidden Types, Git Commit Requirements, Code Style Guidelines
