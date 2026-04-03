# Documentation Style Guide

This reference contains detailed writing style guidelines for StreamFlow documentation.

## Writing Style Principles

### DO

- **Use imperative mood:** "Run the command" (not "You should run")
- **Be technical and concise:** No fluff or marketing language
- **Show actual command outputs:** Use real, tested examples
- **Document edge cases:** Include limitations and requirements
- **Use American English:** Consistent spelling and grammar
- **Include captions:** All code blocks need `:caption:`

### DON'T

- **Use blog-style language:** No "Let's dive in!", emojis, or exclamation marks
- **Add quick reference tables with Time/Difficulty:** Not needed
- **Assume command syntax:** Always verify with --help
- **Skip platform limitations:** Document OS requirements
- **Use placeholders:** Show real, runnable examples

## Command Output Format

Use unified command format with `$` prompt:

```rst
.. code-block:: bash

   $ streamflow version

   StreamFlow version 0.2.0
```

## Code Block Captions

Always include captions for context:

```rst
.. code-block:: yaml
   :caption: streamflow.yml - Docker deployment

   deployments:
     docker-local:
       type: docker
```

## Cross-Reference Style

### Within Same Section

```rst
See :doc:`other-document` for details.
```

### To Other Sections

```rst
See :doc:`/user-guide/installation` for setup.
See :doc:`/reference/cli/run` for all flags.
```

### External Links

```rst
See `Docker Documentation <https://docs.docker.com>`_ for more.
```

## Platform Support Format

Always document OS support:

```rst
Platform Support
================

**Linux:** Full support  
**macOS:** Full support  
**Windows:** Not supported

.. note::
   StreamFlow only supports Linux and macOS. Windows is not supported.
```

## Note and Warning Directives

Use Sphinx directives for important information:

```rst
.. note::
   
   This command requires Docker running.

.. warning::
   
   This will delete all data.

.. important::
   
   Back up your data first.
```

## Dependency-Based Ordering

Documents must be ordered by prerequisites:

**Correct Order:**
1. Documents with no prerequisites first
2. Documents requiring earlier documents next
3. Advanced topics last

**Example:**
- Installation (no prerequisites)
- Quickstart (requires Installation)
- Writing Workflows (requires Installation)
- Running Workflows (requires Writing Workflows + Deployments)

## Meta Tags Format

Every file needs meta tags for AI optimization:

```rst
.. meta::
   :keywords: keyword1, keyword2, keyword3
   :description: Brief 1-2 sentence description
   :audience: users|developers
   :difficulty: beginner|intermediate|advanced
   :reading_time_minutes: 10
```

## File Structure Template

```rst
============
Document Title
============

.. meta::
   :description: Brief description
   :keywords: keyword1, keyword2, keyword3

[Prerequisites section - if applicable]

[What You'll Learn section - if applicable]

[Main content sections]

[Related Documentation section]
```
