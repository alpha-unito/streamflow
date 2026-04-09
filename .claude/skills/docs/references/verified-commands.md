# StreamFlow Verified Commands Reference

This file contains all verified StreamFlow commands with their exact syntax and flags.

## Main Commands

```bash
# Core commands
streamflow version              ✅ Show version
streamflow run FILE             ✅ Run workflow  
streamflow list [NAME]          ✅ List workflows
streamflow report WORKFLOWS     ✅ Generate report
streamflow prov WORKFLOW        ✅ Generate provenance
streamflow ext list             ✅ List extensions
streamflow plugin list          ✅ List plugins
streamflow schema               ✅ Show JSON schema
```

## Flags for streamflow run

```bash
--debug                         ✅ Debug logging
--quiet                         ✅ Minimal output
--color                         ✅ Colored output
--outdir DIR                    ✅ Output directory
--name NAME                     ✅ Workflow name
```

## Flags for streamflow report

```bash
--all                          ✅ Include all executions
--format [html,json,...]       ✅ Output format
--outdir DIR                   ✅ Output directory
--name NAME                    ✅ Report name
```

## CWL Runner (Standalone Command)

```bash
cwl-runner workflow.cwl inputs.yml                    ✅
cwl-runner --streamflow-file config.yml workflow.cwl  ✅
cwl-runner --debug workflow.cwl inputs.yml            ✅
```

## Common Hallucinations to Avoid

### ❌ WRONG Commands (Never Document)

```bash
streamflow --version              # Wrong - use: streamflow version
streamflow run --log-level DEBUG  # Wrong - use: --debug
streamflow cwl-runner             # Wrong - use: cwl-runner (separate!)
streamflow list connectors        # Wrong - use: streamflow ext list
streamflow report --execution 0   # Wrong - use: --all
--keep-intermediates              # Doesn't exist yet
```

### ✅ CORRECT Commands (Verified)

```bash
streamflow version                # Show version
streamflow run FILE --debug       # Run with debug logging
cwl-runner workflow.cwl           # CWL runner (standalone command)
streamflow ext list               # List extensions
streamflow report NAME --all      # Include all executions
```

## Verification Workflow

Test every command before documenting:

```bash
# 1. Check command exists
uv run streamflow --help

# 2. Check subcommand exists
uv run streamflow run --help

# 3. Test specific flags
uv run streamflow run --help | grep -i debug

# 4. Test actual execution
uv run streamflow run streamflow.yml --debug

# 5. Capture output for documentation
uv run streamflow run streamflow.yml > /tmp/output.txt 2>&1
```
