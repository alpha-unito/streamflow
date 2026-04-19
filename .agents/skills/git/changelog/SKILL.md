---
name: StreamFlow Changelog Update
description: This skill should be used when the user asks to "update the changelog", "add a changelog entry", "write a CHANGELOG entry", or when preparing a git commit in StreamFlow that requires updating the `## [Unreleased]` section of `CHANGELOG.md`.
version: 0.1.0
---

# StreamFlow Changelog Update Skill

`CHANGELOG.md` must be updated **before every commit**, under the `## [Unreleased]` section.

## Which subsection to use

| Change | Subsection |
|---|---|
| New user-facing feature | `### Added` |
| Enhancement to existing behaviour | `### Changed` |
| Bug fix | `### Fixed` |
| Removed functionality | `### Removed` |
| Breaking API change | `### Breaking Changes` |
| Runtime dependency bump | `### Dependencies` |
| Dev/CI/tooling dependency bump | `### Dev Dependencies` |

## Entry format

```
- <Short description of the change> ([#NNN](https://github.com/alpha-unito/streamflow/pull/NNN))
```

- One entry per logical change (not per file modified)
- Sentence case, no trailing period
- PR link always wrapped in parentheses: `([#NNN](url))`
- Dependency bumps: `- Bump <package> from <old> to <new> ([#NNN](url))`
- Multiple bumps of the same package collapse into one entry: use the
  first `from` version, the last `to` version, and list all PR links
  comma-separated inside the same parentheses:
  `- Bump foo from 1.0 to 1.2 ([#10](url), [#11](url), [#12](url))`

## What NOT to do

- Do not add entries to already-released sections (`## [0.2.0rc1]`, etc.)
- Do not create a new release section — only edit `## [Unreleased]`
- Do not duplicate an entry that already exists for the same PR
- Do not create separate entries for consecutive bumps of the same package — collapse them
