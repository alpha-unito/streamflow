---
name: Mypy Type Checking
description: This skill should be used when the user asks to "fix mypy errors", "add type annotations", "fix type checking", "resolve no-untyped-def", "fix mypy type errors", or when working with type hints while respecting StreamFlow's forbidden type constraints (no Any, dict[str, Any], etc.).
version: 0.1.0
---

# Mypy Type Checking Skill

Comprehensive guidance for fixing mypy type errors while respecting forbidden type constraints.

## Core Constraint: Forbidden Types

**See AGENTS.md** for the complete list. **Key rule: Never use `Any` in any form.**

**Examples of forbidden types:**
- `Any` (bare type)
- `dict[str, Any]`, `list[Any]`, `tuple[Any, ...]`
- `MutableMapping[str, Any]`, `MutableSequence[Any]`
- `object` (bare type)

**If a fix requires a forbidden type, DO NOT FIX IT.** Skip the error entirely.

## When to Use This Skill

- Fixing mypy type errors
- Adding missing type annotations
- Improving type coverage without violating constraints

## Sub-Skills by Error Type

This skill covers multiple mypy error types. Select the appropriate sub-skill for the specific error:

### 1. no-untyped-def
→ See `no-untyped-def/SKILL.md`

**Error:** Function/method missing type annotations  
**Most common error type**

Example:
```python
# Error: Function is missing a type annotation [no-untyped-def]
def process_data(self, config):
    return {"result": config}
```

### 2. var-annotated
→ See `var-annotated/SKILL.md`

**Error:** Variable missing type annotation  
**Common with empty collections**

Example:
```python
# Error: Need type annotation for "cache" [var-annotated]
cache = {}
```

### 3. type-arg (Coming Soon)

**Error:** Generic type missing type arguments  
**Example:** Using `list` instead of `list[str]`

### 4. attr-defined (Coming Soon)

**Error:** Attribute not defined on type  
**Solutions:** Protocols, unions, type guards

## General Workflow (All Error Types)

For any mypy error, follow this high-level process. Each sub-skill provides detailed steps for specific error types.

1. **Identify Error Type:** Determine which sub-skill to use (no-untyped-def, var-annotated, etc.)

2. **Analyze & Classify:** Determine if concrete types suffice
   - **FIXABLE:** Can use concrete types only → proceed with fix
   - **UNFIXABLE:** Requires forbidden types → skip entirely

3. **Fix:** Add type annotations using concrete types (see sub-skill for details)

4. **Validate:** Run forbidden types check **(MANDATORY)**
   ```bash
   git diff | grep -E "Any\b|MutableMapping\\[.*Any|MutableSequence\\[.*Any|list\\[Any|dict\\[.*Any"
   ```
   - If ANY match found → STOP and revert immediately
   - If no match → proceed

5. **Quality Check:** Run all linters/formatters
   ```bash
   uv run make format-check flake8 codespell-check typing
   ```

6. **Commit:** Follow git approval workflow (see AGENTS.md)

## Concrete Types Quick Reference

**Allowed types:**
- **Primitives:** `None`, `bool`, `int`, `str`, `float`, `bytes`
- **Concrete classes:** `Workflow`, `Connector`, `Port`, `Token`, etc.
- **Unions:** `str | None`, `int | bool`
- **Generics:** `list[str]`, `dict[str, int]`, `set[Token]`
- **Special:** `Self`, `type[MyClass]`, `Literal["value1", "value2"]`

**Prefer `Literal` over generic types:** For fixed sets of values (status strings, connector types, mode flags), use `Literal["docker", "kubernetes"]` instead of generic `str` for stronger type safety.

**Broad arguments, narrow returns:** 
- **Arguments:** Use abstract types (`Iterable`, `Sequence`, `Mapping`) when only basic operations are needed
- **Returns:** Use concrete types (`list`, `dict`, `set`) to provide specific guarantees
- Example: `def process(items: Iterable[T]) -> list[T]`

## Tips for Success

1. **Start with easy fixes** - Functions with clear return types like `-> None` or `-> bool`
2. **Use TypedDict** - When a dict has known keys, define a TypedDict instead of `dict[str, Any]`
3. **Check existing patterns** - Look at similar functions in the codebase for guidance
4. **Don't force it** - If a function requires forbidden types, skip it
5. **Test changes** - Run mypy after adding types to ensure no new errors

**Key principle:** When in doubt, skip it. Better to leave an error than violate the forbidden types constraint.

## Related Documentation

See AGENTS.md sections: Mandatory Agent Behavior, Git Commit Requirements, Git Commit Message Guidelines, Code Style Guidelines
