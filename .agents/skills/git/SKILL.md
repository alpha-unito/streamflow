---
name: StreamFlow Git Workflow
description: This skill should be used when the user asks to "write a commit message", "create a commit", "format a commit", "what commit type to use", or when preparing changes for a git commit in StreamFlow. Provides commit message format, type conventions, and examples.
version: 0.1.0
---

# StreamFlow Git Workflow Skill

Commit message conventions and CHANGELOG update rules for StreamFlow. The approval rule (never commit without explicit user approval) lives in `AGENTS.md`.

## Commit Message Format

```
<Type> <subject>

<body>
```

## Types

| Type | When to use |
|---|---|
| `Add` | Wholly new feature or file |
| `Fix` | Bug fix |
| `Refactor` | Code change with no behavior change |
| `Update` | Enhancement to an existing feature |
| `Remove` | Deletion of code or files |
| `Bump` | Dependency version update |
| `Implement` | New implementation of an existing interface |
| `Improve` | Performance or quality improvement |
| `Docs` | Documentation only |
| `Test` | Test additions or changes |

## Rules

**Subject line:**
- Start with a type word (imperative mood), capitalized, no trailing period
- Max 50 characters
- Example: `Add restore method to DataManager`

**Body** (required except for trivial changes like typo fixes):
- Blank line separating subject from body
- Explain *what* and *why*, not *how*
- Wrap at 72 characters
- Use backticks around package names, rule/error codes, file names,
  and Python identifiers (functions, classes, variables, methods)
- Reference issues: `Fixes #123`, `Closes #456`

## CHANGELOG

Before every commit, `CHANGELOG.md` must be updated under `## [Unreleased]`. Load the **StreamFlow Changelog Update** skill for the full rules on which subsection to use, entry format, and what to avoid.

## Examples

```
Add restore method to DataManager

Implement restore method to enable workflow recovery from checkpoints.
This allows jobs to resume from the last completed step.
```

```
Fix SSH connector authentication timeout (Fixes #931)

Increase default timeout for SSH authentication from 5s to 30s to handle
slow networks and high-latency connections.
```

```
Bump kubernetes-asyncio from 33.3.0 to 34.3.3
```

```
Refactor DirectedGraph class

Extract graph traversal logic into dedicated methods to reduce coupling
and improve testability of scheduling algorithms.
```
