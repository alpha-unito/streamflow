# Generate the documentation
The documentation is written in the reStructuredText markup language, and [Sphinx](https://www.sphinx-doc.org/en/master/) software is used to generate different output formats. The command below installs all the required packages.
```bash
pip install -r requirements.txt
```

Generate the documentation in HTML format is done using
```bash
make html
```

For more information to generate different formats, use
```bash
make help
```


# Contribute to the documentation

Before creating a Pull Request for any changes to the documentation, it is necessary to update the Continuous Integration test.
In particular, the checksum of the documentation in the HTML format is tested to avoid misleading changes when new Sphinx versions are released.

The command below shows in stdout the checksum
```bash
make get-checksum
```

The output checksum must be copied and pasted into the `streamflow/.github/workflows/ci-tests.yaml` file.
The new checksum must replace the value of the environment variable `CHECKSUM` of the job `documentation`.
```yaml
name: "CI Tests"
jobs:
  documentation:
    name: "Build Sphinx documentation"
    steps:
      env:
        CHECKSUM: "NEW_CHECKSUM"
```
