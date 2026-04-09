---
name: Reference Documentation
description: This skill should be used when the user asks to "document CLI command", "create reference docs", "document connector", "write API reference", "document configuration schema", or when editing files in docs/source/reference/. Provides lookup-oriented documentation with schema-driven configuration and comprehensive technical specifications.
version: 1.1.0
parent_skill: ../SKILL.md
---

# Reference Documentation Subskill

## Purpose

Reference documentation is **lookup-oriented** - providing complete, accurate technical specifications for quick consultation.

## Parent Skill

Extends [Documentation Writing Skill](../SKILL.md). **All rules apply**, especially:
- **TEST EVERY COMMAND** before documenting
- **ZERO TOLERANCE** for hallucinations

## Reference Structure

```
docs/source/reference/
├── cli/                           # run, list, report, prov, plugin, ext, schema, cwl-runner
├── configuration/                 # streamflow-yml, workflow-config, deployment-config, binding-config
├── connectors/                    # docker, singularity, kubernetes, ssh, slurm, pbs (FLAT)
├── cwl-docker-translators/        # docker, kubernetes, singularity, no-container
└── glossary.rst
```

## Critical: Use Schema Directive

**MANDATORY:** Use `sphinx-jsonschema` for ALL configuration options.

```rst
Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/connector/docker.json
    :lift_description: true
```

**NEVER** manually create configuration tables. Schemas are source of truth.

**Schema URLs:**
- **Connectors:** `https://streamflow.di.unito.it/schemas/deployment/connector/{name}.json`
- **CWL Docker Translators:** `https://streamflow.di.unito.it/schemas/cwl/requirement/docker/{name}.json`

## File Requirements

Every Reference file MUST include:

1. **Meta tags** - Keywords, description
2. **Overview** - Brief introduction
3. **Quick Reference** - Summary table
4. **Examples** - Practical usage (BEFORE configuration)
5. **Configuration** - Schema directive (AT END, before Related Docs)
6. **Related Documentation** - Cross-references

See `references/templates.md` for complete templates.

## Standard Section Order

### For Connector/Translator Pages

1. **Title, Meta, Overview**
2. **Quick Reference** - Essential info table
3. **Examples** - Basic → advanced
4. **Prerequisites**
5. **Platform Support**
6. **Configuration** - Schema-driven (AT END)
7. **Related Documentation**

### For CLI Pages

1. **Title, Meta, Overview**
2. **Synopsis** - Command syntax
3. **Arguments**
4. **Options** - All flags from --help
5. **Examples**
6. **Exit Status** (if applicable)
7. **Related Documentation**

### For Configuration Pages

1. **Title, Meta, Overview**
2. **File Structure**
3. **Schema** - jsonschema directive
4. **Examples**
5. **Validation**
6. **Related Documentation**

## Quick Reference Tables

```rst
Quick Reference
===============

============  ====================================
Type          ``connector-type``
Category      Container|Cloud|HPC
Scalability   Single node|Multi-node|Cluster
Best For      Use case description
============  ====================================
```

## Synopsis Format

```rst
Synopsis
========

.. code-block:: text

   streamflow run [OPTIONS] FILE
   streamflow list [OPTIONS] [NAME]
```

## Option Format

```rst
``--flag-name VALUE``
   Description. Include default if applicable.
   
   **Type:** string|integer|boolean  
   **Default:** value (if applicable)  
   **Required:** Yes|No
```

## Verification Process

### CLI Commands

```bash
# 1. Get actual help text
uv run streamflow run --help > /tmp/run-help.txt

# 2. Document ONLY what's in help
cat /tmp/run-help.txt

# 3. Test every flag
uv run streamflow run --debug streamflow.yml
uv run streamflow run --quiet streamflow.yml
```

### Configuration Schemas

```bash
# 1. Verify schema URL exists
curl -I https://streamflow.di.unito.it/schemas/deployment/connector/docker.json

# 2. Test jsonschema renders
cd docs
uv run make clean && uv run make html

# 3. Check HTML output
open build/html/reference/connectors/docker.html
```

### Connectors

```bash
# 1. Test connector works
cat > test-streamflow.yml << 'EOF'
version: v1.0
deployments:
  test:
    type: docker
    config:
      image: alpine:latest
EOF

uv run streamflow run test-streamflow.yml

# 2. Document only tested configurations
```

## Writing Style

### DO
- **Be comprehensive** - Document every option
- **Be precise** - Exact syntax, types, defaults
- **Use tables** - Quick Reference for scanning
- **Show examples** - Simple → complex
- **Link to schemas** - Use jsonschema directive
- **Organize logically** - Group related options

### DON'T
- **Explain how to use** - That's User Guide
- **Manual config tables** - Use jsonschema
- **Assume knowledge** - Define all terms
- **Skip options** - Document everything from --help

## Cross-Reference Strategy

**From User Guide to Reference:**
```rst
For complete options, see :doc:`/reference/connectors/docker`.
For all CLI flags, see :doc:`/reference/cli/run`.
```

**From Reference to User Guide:**
```rst
For tutorial, see :doc:`/user-guide/configuring-deployments`.
```

**Within Reference:**
```rst
See :doc:`streamflow-yml` for file structure.
See :doc:`../cli/run` for execution commands.
```

## Quality Checklist

**Accuracy:**
- [ ] CLI flags verified with --help
- [ ] Configuration uses jsonschema directive
- [ ] Examples tested and produce results
- [ ] Schema URLs valid

**Completeness:**
- [ ] Every --help option documented
- [ ] Configuration at end (before Related Docs)
- [ ] Quick Reference table included
- [ ] Platform Support documented

**Structure:**
- [ ] Follows standard section order
- [ ] Examples before Configuration
- [ ] Configuration uses jsonschema only
- [ ] No manual config tables

**Validation:**
- [ ] jsonschema renders in HTML
- [ ] Cross-references work
- [ ] Build produces no errors

## Additional Resources

**Templates:**
- `references/templates.md` - Complete file templates

**Parent Skill:**
- `../SKILL.md` - Main documentation skill

## Remember

**Reference = Lookup**

Users find specific technical details quickly:
- Quick Reference tables
- Exact syntax in Synopsis
- Complete details from jsonschema
- Working examples

**If it's not in `--help` or JSON schema, don't document it.**
**If you haven't tested it, don't claim it works.**
