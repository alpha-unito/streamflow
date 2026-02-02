---
name: Documentation Writing
description: This skill should be used when the user asks to "create documentation", "add docs", "update documentation", "write user guide", "document commands", "verify commands in docs", "fix hallucinated commands", or when creating/editing .rst files in docs/source/. Ensures all commands are tested before documenting to prevent hallucinations.
version: 1.1.0
---

# Documentation Writing Skill

## Purpose

Ensure accurate technical documentation for StreamFlow. **ZERO TOLERANCE for command hallucinations** - verify every command, flag, and output before documenting.

## Critical Principle

**TEST EVERY COMMAND BEFORE DOCUMENTING**

Documentation is NOT complete until all commands are verified.

## Documentation Structure

```
docs/source/
├── user-guide/              # Tutorial-style guides
├── developer-guide/         # Contributor docs (TODO)
└── reference/               # Technical specs, CLI, schemas
```

**Section-Specific Guidelines:**
- **User Guide:** See [user-guide/SKILL.md](user-guide/SKILL.md) - Tutorial writing, dependency ordering, progressive examples
- **Reference:** See [reference/SKILL.md](reference/SKILL.md) - CLI docs, schema-driven config, lookup content

## Mandatory Verification Workflow

### Before Writing ANY Documentation

1. **Test Command Exists:** `uv run <command> --help`
2. **Test Flags:** `uv run streamflow run --help | grep -i debug`
3. **Capture Actual Output:** Use EXACT output in documentation
4. **Document Limitations:** Note platform/Docker requirements

### Common Hallucination Traps

**NEVER assume commands exist - verify first!**

See `references/verified-commands.md` for complete list.

## File Requirements

Every file must include:

```rst
============
Document Title
============

.. meta::
   :description: Brief description
   :keywords: keyword1, keyword2
   :audience: users|developers
   :difficulty: beginner|intermediate|advanced

**Prerequisites:**

* :doc:`prerequisite1`

**What You'll Learn:**

* Learning objective 1
```

**Writing Style:** See `references/style-guide.md`

- Imperative mood: "Run the command" (not "You should run")
- Technical and concise
- Show actual outputs
- Document edge cases

## Complete Verification Process

### Phase 1: Identify Commands

```bash
grep -n "^\s*streamflow\|^\s*docker" docs/source/file.rst
```

### Phase 2: Test Each Command

```bash
uv run streamflow --help
uv run streamflow run --help
uv run cwl-runner --help  # Separate command!
```

### Phase 3: Write Documentation

Use ONLY verified commands. No assumptions.

### Phase 4: Final Verification

```bash
# Build documentation
cd docs
uv run make clean && uv run make html

# Check for errors
grep -i "error" build.log
```

## Quality Checklist

**Accuracy:**
- [ ] All commands tested with `--help`
- [ ] All flags verified
- [ ] All outputs match actual output
- [ ] Platform limitations documented

**Completeness:**
- [ ] Meta tags included
- [ ] Prerequisites listed
- [ ] Cross-references added

**Style:**
- [ ] Technical style
- [ ] Imperative mood
- [ ] Unified command format ($ prompt)

**Build:**
- [ ] `make clean && make html` succeeds
- [ ] No errors in build output

## Emergency Fix Protocol

If hallucinated command discovered:

1. **STOP** - Do not proceed
2. **Find correct command** - Test alternatives
3. **Find ALL instances** - `grep -rn "wrong-command" docs/source/`
4. **Fix ALL instances**
5. **Test the fix**
6. **Rebuild docs**
7. **Document in commit**

## Additional Resources

**Reference Files:**
- `references/verified-commands.md` - Complete command reference
- `references/style-guide.md` - Detailed style guidelines

**Section-Specific Skills:**
- `user-guide/SKILL.md` - Tutorial-oriented docs
- `reference/SKILL.md` - Lookup-oriented docs

## Remember

**Task is NOT complete until:**

1. ✅ Every command tested and verified
2. ✅ Every flag confirmed to exist
3. ✅ Every output matches reality
4. ✅ Documentation builds without errors

**If in doubt, TEST IT. NO HALLUCINATIONS. EVER.**
