---
name: StreamFlow Documentation — Adding Examples
description: This skill should be used when the user asks to "add an example", "write a tutorial", "document a new workflow example", or when creating a new worked example under `docs/source/examples/`.
version: 0.1.0
---

# StreamFlow Documentation — Adding Examples

Creates a worked example under `docs/source/examples/<name>/`.
Load **StreamFlow Documentation** for general RST style rules.

## Directory Layout

```text
docs/source/examples/<name>/
├── <name>.rst          # tutorial · streamflow.yml  # primary config
├── cwl/main.cwl · config.yml · clt/*.cwl
├── data/               # input files
└── environment/<connector>/
```

## Typical Sections

Intro → Concepts covered → Prerequisites → Project layout → Steps
(source files, CWL tools, Workflow, inputs, environment, `streamflow.yml`)
→ Run the workflow → Expected output → Alternative deployments → Troubleshooting

## Key Rules

- Explanations **before** code listings — never after.
- Use `.. literalinclude::` for file content; `.. code-block::` only when
  the file contains values the user must substitute.
- Use `:emphasize-lines:` for lines that differ from a previous listing.
- `data/` is at the project root; `config.yml` uses `../data/<file>`.
- Never reference a file without first telling the user how to create it.
- Authoring-aid files (e.g. `streamflow-k8s.yml`) **must not appear** in
  the tutorial — only `streamflow.yml` is visible to the reader.
- Alternative backends go in a `.. _Alternative deployments:` section.

## Register and Test

**`docs/source/index.rst`** — add to the `Examples` toctree:
```rst
   examples/<name>/<name>.rst
```

**`docs/tests/test_examples.py`** — one function per `streamflow*.yml`:
```python
@pytest.mark.asyncio
async def test_<name>_<backend>() -> None:
    """Run the <Name> example using the <Backend> backend."""
    await _run(
        pathlib.Path(__file__).parent.parent
        / "source" / "examples" / "<name>" / "streamflow[-<backend>].yml"
    )
```
Naming: `test_<example>_<backend>` (e.g. `test_mpi_kubernetes`).
Verify with `make test` from `docs/`.
