---
name: StreamFlow Documentation
description: This skill should be used when the user asks to "write documentation", "add a doc page", "update RST files", "add a tutorial", or when writing or editing Sphinx RST documentation for StreamFlow.
version: 0.1.0
---

# StreamFlow Documentation Skill

Guidelines for writing Sphinx RST documentation for StreamFlow.
Docs live under `docs/source/`. The Sphinx build uses `sphinx.ext.autosectionlabel`,
so every section title is automatically a valid `:ref:` target.

## Writing Style

- Use American English throughout.
- Use **double backticks** for all code entities: file names, environment
  variables, class names, function names, CLI commands, YAML keys, CWL fields.
- Never reference a file without first telling the user how to create it.
- Keep source files shown via `literalinclude` within **80 characters** per line.

## RST Conventions

All documentation files use `.rst` format.

### Section title underlines

Every title underline (and overline) must be **exactly as long as the title
text** — no shorter, no longer. Count Unicode characters, not bytes.

```rst
MPI Application
===============    ← 15 chars, underline is 15 chars  ✓

Run with Kubernetes
-------------------  ← 19 chars, underline is 19 chars  ✓
```

After editing headings, verify with:

```python
python3 -c "
with open('docs/source/.../file.rst') as f:
    lines = f.readlines()
ul = set('=-~^\"\\'\`#*+')
for i,l in enumerate(lines):
    s = l.rstrip()
    if i+1 < len(lines):
        n = lines[i+1].rstrip()
        if n and len(set(n))==1 and n[0] in ul and len(s)!=len(n):
            print(f'Line {i+1}: title={len(s)}, underline={len(n)}: {s!r}')
"
```

### Highlighting changed lines

Use `:emphasize-lines:` with both `code-block` and `literalinclude` directives.
Prefer `literalinclude` whenever the file exists on disk.

When a tutorial shows multiple versions of the same file for different
environments (e.g. Docker Compose → Kubernetes → Helm variants of
`streamflow.yml`), use `:emphasize-lines:` to highlight the lines that differ
from the base version, so the reader can spot the changes at a glance:

```rst
.. literalinclude:: streamflow-k8s.yml
   :language: yaml
   :emphasize-lines: 12, 16, 20-25
```

### Cross-references

Use `:ref:` to link to other sections. Targets are section titles verbatim:

```rst
:ref:`DockerComposeConnector`
:ref:`Put it all together`
```

Common targets: `Install`, `Write your workflow`, `Put it all together`,
`Binding steps and deployments`, `Import your environment`, `CWL Runner`,
`DockerComposeConnector`, `KubernetesConnector`, `Helm4Connector`.

## Subskills

| Task | Subskill |
|---|---|
| Add a new worked example tutorial | **StreamFlow Documentation — Adding Examples** |

## See Also

- **StreamFlow Code Style** skill — Python code style and docstring conventions
- **StreamFlow Git Workflow** skill — commit message format
