---
name: User Guide Documentation
description: This skill should be used when the user asks to "write user guide", "create tutorial", "document workflow execution", "write installation guide", "document quickstart", or when editing files in docs/source/user-guide/. Provides tutorial-oriented documentation guidelines with dependency-based ordering and progressive examples.
version: 1.1.0
parent_skill: ../SKILL.md
---

# User Guide Documentation Subskill

## Purpose

User Guide documentation is **tutorial-oriented** - teaching users through practical examples and step-by-step instructions.

## Parent Skill

Extends [Documentation Writing Skill](../SKILL.md). **All rules apply**, especially:
- **TEST EVERY COMMAND** before documenting
- **ZERO TOLERANCE** for hallucinations

## User Guide Structure

```
docs/source/user-guide/
├── installation.rst              # No prerequisites
├── quickstart.rst                # Prerequisites: installation
├── writing-workflows.rst         # Prerequisites: installation
├── configuring-deployments.rst   # Prerequisites: installation
├── binding-workflows.rst         # Prerequisites: writing + configuring
├── running-workflows.rst         # Prerequisites: binding
├── inspecting-results.rst        # Prerequisites: running
├── advanced-patterns/            # Prerequisites: running
└── troubleshooting.rst           # Prerequisites: all
```

## Critical: Dependency-Based Ordering

**Documents MUST be ordered by prerequisites.**

❌ **WRONG:** Any other order violates dependency principle

## File Requirements

Every User Guide file MUST include:

1. **Meta tags** - Keywords, description, audience
2. **Prerequisites section** - What to read first (with WHY)
3. **What You'll Learn** - Specific, measurable objectives
4. **Progressive examples** - Simple to complex
5. **Next Steps** - Where to go next
6. **Related Documentation** - Cross-references

See `references/templates.md` for complete templates.

## Writing Style

### DO
- **Teach step-by-step** - Guide through tasks
- **Use real examples** - Show working configurations
- **Explain concepts** - Don't just list commands
- **Provide context** - Why would you do this?
- **Show output** - What users should see
- **Handle errors** - Common problems and solutions

### DON'T
- **Assume knowledge** - Link to prerequisites
- **Skip steps** - Be explicit
- **Use placeholders** - Show real, runnable examples
- **Omit output** - Users need to verify

## Prerequisites Format

Always explain WHY:

```rst
Prerequisites
=============

Before reading this guide:

* :doc:`installation` - StreamFlow must be installed
* :doc:`writing-workflows` - Understanding CWL structure
* Docker installed and running
```

## Learning Objectives Format

✅ **GOOD:**
```rst
* Run workflow with ``streamflow run`` command
* Configure Docker deployments in ``streamflow.yml``
* Debug execution with ``--debug`` flag
```

❌ **BAD:**
```rst
* Learn about workflows
* Understand deployments
```

## Example Structure

Progressive examples - start simple, add complexity:

```rst
Basic Example
=============

.. code-block:: yaml
   :caption: streamflow.yml - Simple deployment

   version: v1.0
   deployments:
     docker-local:
       type: docker

.. code-block:: bash

   $ streamflow run streamflow.yml

   [ACTUAL TESTED OUTPUT HERE]

Advanced Example
================

.. code-block:: yaml
   :caption: streamflow.yml - With resource limits

   version: v1.0
   deployments:
     docker-limited:
       type: docker
       config:
         memory: 4g
         cpus: 2
```

## Cross-References

**Within User Guide:**
```rst
See :doc:`writing-workflows` for CWL syntax.
```

**To Reference:**
```rst
See :doc:`/reference/cli/run` for all flags.
See :doc:`/reference/connectors/docker` for options.
```

## Verification Workflow

```bash
# 1. Create example files
mkdir /tmp/streamflow-test
cd /tmp/streamflow-test

# 2. Write actual streamflow.yml
cat > streamflow.yml << 'EOF'
version: v1.0
workflows:
  example:
    type: cwl
    config:
      file: workflow.cwl
EOF

# 3. Test EVERY command
uv run streamflow run streamflow.yml

# 4. Capture actual output
uv run streamflow run streamflow.yml > /tmp/output.txt 2>&1

# 5. Use EXACT output in docs
```

## Quality Checklist

**Content:**
- [ ] Prerequisites explicit and accurate
- [ ] Learning objectives measurable
- [ ] Examples progress simple → complex
- [ ] All commands tested, output verified
- [ ] Next steps guide to logical next doc

**Structure:**
- [ ] Ordered by dependencies
- [ ] Follows template
- [ ] Cross-references bidirectional
- [ ] Advanced patterns after basics

**Examples:**
- [ ] All YAML valid and tested
- [ ] All commands produce documented output
- [ ] File paths realistic
- [ ] Captions describe examples

## Additional Resources

**Templates:**
- `references/templates.md` - Complete file templates

**Parent Skill:**
- `../SKILL.md` - Main documentation skill

## Remember

**User Guide = Teaching**

Every document should enable users to **do something** by the end.

If you can't test it, don't document it. If users can't reproduce it, rewrite it.
