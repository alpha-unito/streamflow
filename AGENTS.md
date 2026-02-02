# StreamFlow Agent Guidelines

StreamFlow is a container-native Workflow Management System implementing CWL (v1.0-v1.3) for multi-cloud/HPC hybrid workflow executions. Written in Python 3.10-3.14. Package manager: **`uv`**.

## Mandatory Rules

### Package & Dependency Management

Obtain explicit user permission before installing or updating any packages or dependencies. Specify what is being installed/updated and why, then wait for confirmation.

### Git Commits

Never create git commits without explicit user approval. Required sequence:
1. Run `uv run make format-check flake8 codespell-check typing` — all must pass
2. Present the full commit message + `git diff --stat` + full `git diff`
3. Ask explicitly for approval and wait — do not commit until confirmed

See `.agents/skills/git/SKILL.md` for commit message format and examples.

## Forbidden Types

Never use these types in any form:

- `Any`, `object`
- `dict[str, Any]`, `list[Any]`, `tuple[Any, ...]`, `dict[Any, Any]`
- `MutableMapping[str, Any]`, `MutableSequence[Any]`

If a fix requires any of the above, skip it entirely. See `.agents/skills/mypy/SKILL.md` for allowed alternatives and validation procedures.

## Project Structure

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

## Skills

| Task | Skill                                |
|---|--------------------------------------|
| Type checking, mypy errors, forbidden types | `.agents/skills/mypy/SKILL.md`       |
| Code style: imports, naming, error handling, docstrings | `.agents/skills/code-style/SKILL.md` |
| Git commit message format | `.agents/skills/git/SKILL.md`        |
| Writing and running tests | `.agents/skills/testing/SKILL.md`    |
