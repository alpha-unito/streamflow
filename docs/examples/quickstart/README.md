# StreamFlow Quickstart Example

This directory contains a minimal working example for the StreamFlow quickstart guide.

## Files

- `hello-workflow.cwl` - Simple CWL workflow that echoes a message
- `inputs.yml` - Input parameters for the workflow
- `streamflow.yml` - StreamFlow configuration for local execution
- `streamflow-docker.yml` - Alternative configuration using Docker (optional)

## Running the Example

```bash
# Run locally
streamflow run streamflow.yml

# Check output
cat output.txt

# Run with Docker (optional - requires Docker)
streamflow run streamflow-docker.yml
```

## Expected Output

The workflow should create an `output.txt` file containing:
```
Hello from StreamFlow!
```

## What It Demonstrates

- Basic CWL CommandLineTool definition
- StreamFlow configuration structure
- Local execution (default)
- Docker deployment (optional)

## Documentation

See the complete quickstart guide: `docs/source/user-guide/quickstart.rst`
