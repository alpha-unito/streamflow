---
name: Mypy var-annotated Fixer
description: This skill should be used when the user encounters "var-annotated" mypy errors, asks to "fix var-annotated errors", "add type annotations to variables", or mentions "Need type annotation for" errors. Provides workflow for fixing missing variable type annotations while respecting forbidden type constraints (no Any, dict[str, Any], list[Any], etc.).
version: 0.1.0
---

# Mypy var-annotated Error Fixer

Fix `[var-annotated]` errors by adding explicit type annotations to variables where mypy cannot infer types.

## What is var-annotated?

This error occurs when mypy cannot infer a variable's type, typically with:
- Empty collections: `cache = {}` or `items = []`
- Generic types: `future = asyncio.Future()`
- Complex object creation: `port = workflow.create_port()`

**Solution:** Add explicit type annotation: `variable: Type = value`

## Critical Constraint

**Never use forbidden types** (see AGENTS.md or parent mypy/SKILL.md):
- ❌ `Any`, `dict[str, Any]`, `list[Any]`, `MutableMapping[str, Any]`
- ✅ If a fix requires forbidden types → **skip the error entirely**

## Quick Fix Patterns

### Pattern 1: Empty Collections

**Strategy:** Examine what gets stored/retrieved to determine element types.

```python
# ❌ Error
cache = {}
items = []

# ✅ Fix - Check usage to find types
cache: dict[str, Port] = {}         # Port is value type
items: list[Token] = []             # Token is element type
ports: dict[str, list[str]] = {}    # Nested: dict of string lists
```

**Common types:**
- `dict[str, Port]`, `dict[str, Token]`, `dict[str, Storage]`
- `list[Token]`, `list[Port]`, `list[ExecutionLocation]`
- `dict[str, list[str]]`, `dict[int, str]`

### Pattern 2: Generic Types

**Strategy:** Specify type parameters based on what the generic holds and variance rules.

```python
# ❌ Error
future = asyncio.Future()
cache = LRUCache(maxsize=5)

# ✅ Fix - Use concrete type parameters
future: asyncio.Future[T] = asyncio.Future()           # T from function signature
cache: LRUCache[int, int] = LRUCache(maxsize=5)       # key, value types
```

**Variance Rules:**

When choosing type parameters for generic types, follow these variance principles:

- **Contravariant types** (type parameters marked with `contravariant=True`):
  - Prefer the **most specific type** possible
  - Use the `bound` argument for `TypeVar` objects
  - Example: For function parameters that accept callbacks

- **Covariant types** (type parameters marked with `covariant=True`):
  - Prefer the **most generic type** possible (excluding forbidden `Any`)
  - Example: For return types and read-only containers like `Sequence[T]`

- **Invariant types** (default, no variance specified):
  - Check if the invariant generic can be substituted with a **covariant alternative**
  - If substitution not possible, use the **exact type** that matches usage
  - Example: Use `Mapping[K, V]` (covariant) instead of `MutableMapping[K, V]` (invariant) if you only need read access
  - Example: Use `Sequence[T]` (covariant) instead of `MutableSequence[T]` (invariant) if you only need read access

**Reference:** Check variance in Python's [typeshed repository](https://github.com/python/typeshed) for standard library types

```python
# Examples of variance-aware typing:

# Covariant (Sequence is covariant in T) - use more generic types
items: Sequence[Token] = []  # Can accept list[Token], tuple[Token], etc.
results: Mapping[str, int] = {}  # Read-only dict, covariant in value type

# Invariant (dict/MutableMapping/list are invariant) - use exact types
cache: dict[str, int] = {}  # Exact types required
mutable_items: MutableSequence[Token] = []  # Exact type required

# Better: If you only need read access, use covariant alternatives
# ✅ Instead of: items: MutableSequence[Token] = []
# ✅ Use:        items: Sequence[Token] = []  (if you only read)

# Contravariant (rarely seen in variable annotations)
# Usually appears in function signatures, not variable declarations
```

### Pattern 3: StreamFlow Objects

**Strategy:** Import the concrete type from streamflow modules.

```python
# ❌ Error
storage = {}

# ✅ Fix - Import and use concrete types
from streamflow.data.utils import Storage

storage: dict[str, Storage] = {}
```

**Common StreamFlow types:**
- `Port`, `Token` - from `streamflow.core.workflow`
- `Storage` - from `streamflow.data.utils`
- `ExecutionLocation` - from `streamflow.core.deployment`
- `ValueFromTransformer` - from `streamflow.cwl.step`

### Pattern 4: Nested Dicts with Known Keys

**Strategy:** Prefer TypedDict for known keys; use generic dicts only for dynamic keys.

```python
# ❌ Error
config = {}
config["name"] = "test"
config["count"] = 42

# ✅ Best - TypedDict for known structure
from typing import TypedDict

class ConfigDict(TypedDict):
    name: str
    count: int

config: ConfigDict = {"name": "test", "count": 42}

# ⚠️ Fallback - Generic nested dict (only if keys are dynamic)
extensions: dict[str, dict[str, dict[str, str]]] = {}         # Use when keys unknown
```

## Workflow

**For each error:**

1. **Read surrounding code** (10-20 lines) - What gets assigned/retrieved?

2. **Find the concrete type:**
   - Check function signatures for parameter/return types
   - Examine usage: what gets stored, retrieved, or called?
   - Search for similar patterns in the codebase
   - Look up type definitions and imports

3. **Verify no forbidden types** - Does the type use `Any`?
   - ✅ No → apply fix
   - ❌ Yes → skip this error

4. **Apply annotation:** `variable: Type = value`

5. **Validate:**
   ```bash
   uv run make format                              # Auto-format
   uv run make format-check flake8 codespell-check # Must pass
   git diff | grep -E "Any\b|dict\[.*Any"         # Must be empty
   ```

## Type Discovery Commands

```bash
# Find function signatures
grep -n "def function_name" file.py

# Find type definitions
grep -rn "class TypeName" streamflow/

# Find similar annotations
grep -rn "variable_name: " streamflow/

# Check imports
head -50 file.py | grep "from streamflow"
```

## Common Mistakes

**❌ Don't guess types** - Always verify by reading code  
**❌ Don't use `Any`** - Skip error if concrete type not possible  
**❌ Don't forget imports** - Add necessary imports for types  
**❌ Don't use bare generics** - Use `list[str]` not `list`  
**❌ Don't use nested dicts for known keys** - Use TypedDict instead

## Validation Commands

```bash
# List all var-annotated errors
uv run make typing 2>&1 | grep "\[var-annotated\]"

# Count remaining errors
uv run make typing 2>&1 | grep "\[var-annotated\]" | wc -l

# Check for forbidden types in changes
git diff | grep -E "^\+.*: .*(Any|object)\b"
```

## Related Documentation

- **AGENTS.md** - Forbidden types, validation commands, commit requirements
- **mypy/SKILL.md** - General mypy workflow, concrete types reference
