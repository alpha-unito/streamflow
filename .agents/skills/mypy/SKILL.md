---
name: Mypy Type Checking
description: This skill should be used when the user asks to "fix mypy errors", "add type annotations", "fix type checking", "resolve no-untyped-def", "fix mypy type errors", or when working with type hints while respecting StreamFlow's forbidden type constraints (no Any, dict[str, Any], etc.).
version: 0.1.0
---

# Mypy Type Checking Skill

## Core Constraint

**See AGENTS.md** for the authoritative forbidden types list. **If a fix requires a forbidden type, DO NOT FIX IT.** Skip the error entirely.

## Sub-Skills

| Error code | Description | Sub-skill |
|---|---|---|
| `[no-untyped-def]` | Function missing type annotations | `no-untyped-def/SKILL.md` |
| `[var-annotated]` | Variable missing type annotation | `var-annotated/SKILL.md` |

## General Workflow

1. **Classify:** Can the fix use concrete types only? If NO → skip entirely
2. **Fix:** Add annotations (see sub-skill for patterns)
3. **Validate** — must return no output:
   ```bash
   git diff | grep -E "Any\b|MutableMapping\[.*Any|MutableSequence\[.*Any|list\[Any|dict\[.*Any"
   ```
4. **Quality check** — must pass:
   ```bash
   uv run make format-check flake8 codespell-check typing
   ```
5. **Commit:** See `.agents/skills/git/SKILL.md`

## Allowed Types Quick Reference

- **Primitives:** `None`, `bool`, `int`, `str`, `float`, `bytes`
- **Concrete classes:** `Workflow`, `Connector`, `Port`, `Token`, etc.
- **Unions:** `str | None`, `int | bool`
- **Generics:** `list[str]`, `dict[str, int]`, `set[Token]`
- **Special:** `Self`, `type[MyClass]`, `Literal["a", "b"]`

**Prefer `Literal`** over `str`/`int` for fixed value sets (statuses, connector types, mode flags).

**Broad arguments, narrow returns:** parameters use abstract types (`Iterable`, `Sequence`, `Mapping`); return types use concrete types (`list`, `dict`, `set`).

## Annotation Patterns

```python
# Always annotate with concrete types
def process(self, workflow: Workflow, config: WorkflowConfig) -> bool: ...

# Self for classmethods
@classmethod
async def load(cls, context: StreamFlowContext) -> Self: ...

# Optional: use X | None, not Optional[X]
def get(self, name: str) -> Connector | None: ...

# Always annotate return types — mypy strict mode requires it
async def close(self) -> None: ...
```

**When in doubt, skip it.** Better to leave an error than violate the forbidden types constraint.

## See Also

- **AGENTS.md** — Forbidden types list, mandatory rules
- **`.agents/skills/git/SKILL.md`** — Commit message format and approval workflow
- **`.agents/skills/code-style/SKILL.md`** — Code style guidelines
