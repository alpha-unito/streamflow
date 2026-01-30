# Contribute to StreamFlow

As a first step, clone StreamFlow from [GitHub](https://github.com/alpha-unito/streamflow)
```bash
git clone git@github.com:alpha-unito/streamflow.git
```

StreamFlow uses the [uv package manager](https://docs.astral.sh/uv/) to manage dependencies. If `uv` is not present on your system, you can install it as described in the [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Then, you can install all the required packages using the `uv sync` command

```bash
cd streamflow
uv sync --all-extras
```

## Continuous Integration

StreamFlow relies on [GitHub Actions](https://github.com/features/actions) for Continuous Integration (CI) and Continuous Distribution (CD).
Only maintainers take care of the CD pipeline. In order to publish a new version of the software on PyPI and Docker Hub distributions, a maintainer only has to augment the version number in the `pyproject.toml` file and run a `uv sync --all-extras` command to update the `uv.lock` file accordingly.

Instead, everyone in the community can contribute to the StreamFlow codebase by opening a Pull Request (PR). Running the entire suite of StreamFlow tests is part of the CI pipeline.
However, it is also possible (and advisable) to run tests locally before opening a PR, as explained [below](#streamflow-tests).


### CWL conformance
StreamFlow complies with all stable versions of the [Common Workflow Language](https://www.commonwl.org/) (CWL) open standard (v1.0, v1.1, and v1.2). Plus, it implements the `cwltool:Loop` extension (see [here](https://cwltool.readthedocs.io/en/latest/loop.html)).
You can check if your PR does not compromise CWL compliance by running the CWL conformance tests suite, using the following command
```bash
./cwl-conformance-test.sh
```

### StreamFlow tests
Some non regression tests are supplied using [pytest](https://docs.pytest.org/en/7.3.x/getting-started.html). You can execute all the available tests with the following command
```bash 
uv run make test
```

Otherwise, you can execute only a specific test file. For example, to verify compliance with the `cwltool:Loop` CWL extension, you can use the following command
```bash
uv run pytest tests/test_cwl_loop.py
```

### Testing connectors
StreamFlow comes with many different connectors OOTB, and some of the tests in the suite are used to verify their behaviour. Currently, the tested connectors are: `local`, `docker`, `docker-compose`, `ssh`, `slurm`, `kubernetes`, and `singularity`. The plan is to add non regression tests for all connectors.
Executing all these tests requires to install and configure several software packages:
- [Docker](https://docs.docker.com/engine/install/), which is required to test the `docker`, `docker-compose`, `slurm`, and `ssh` connectors and the CWL `DockerRequirement` implementation with Docker containers;
- [Singularity](https://docs.sylabs.io/guides/3.0/user-guide/installation.html), which is required to test the `singularity` connector and the CWL `DockerRequirement` conversion to Singularity containers;
- A Kubernetes distribution, e.g., [minikube](https://minikube.sigs.k8s.io/docs/start/), which is used to test the `kubernetes` connector and the CWL `DockerRequirement` conversion to Kubernetes pods.

For development purposes, it is possible to execute local tests only on a specific subset of connectors using the `--deploys` option as follows
```bash 
uv run pytest --deploys local,docker,singularity tests/test_remotepath.py
```
However, all connectors are always tested in the CI pipeline.



### Code style
StreamFlow supports Python 3.10 to 3.14, so use compatible code features.

The StreamFlow code style complies with [PEP 8](https://peps.python.org/pep-0008/). You can verify that your PR respects the code style with the following command
```bash
uv run make format-check flake8 codespell-check
```

You can also apply the suggested changes in bulk with the following command 
```bash
uv run make format codespell
```
Use it carefully as it modifies the target files in place.

Finally, the StreamFlow documentation relies on the [Sphinx](https://www.sphinx-doc.org/en/master/) framework, which follows the reStructuredText format. Follow this style to add documentation for new functions.
```
This is a reST style.

:param param1: this is a first param
:param param2: this is a second param
:returns: this is a description of what is returned
:raises keyError: raises an exception
```

## Contribute to the documentation

### Generate the documentation
The documentation is written in the reStructuredText markup language, and the [Sphinx](https://www.sphinx-doc.org/en/master/) framework is used to generate the HTML pages published on the [StreamFlow website](https://streamflow.di.unito.it/documentation/latest/). Currently, there is no CD for documentation: maintainers manually transfer files on the StreamFlow website at every release.

To check your local version of the documentation, generate the HTML format with the following command
```bash
uv run make html
```

If you want, you can also generate documentation in a different format from the ones supported by Sphinx. However, note that StreamFlow officially maintains only the HTML format. For more information on the available documentation formats, use the following command
```bash
make help
```

### Documentation changes
Whenever a commit changes the documentation, it is necessary to update the dedicated test on the CI pipeline.
The checksum of the documentation in HTML format is tested in the CI pipeline to avoid unwanted changes when Sphinx or other dependencies are updated.


The command below prints the new checksum on the standard output
```bash
cd docs
uv run make checksum
```

This new checksum must coincide with the one in the `.github/workflows/ci-tests.yaml` file, i.e., the value of the `CHECKSUM` environment variable in the `documentation` job
```yaml
name: "CI Tests"
jobs:
  documentation:
    name: "Build Sphinx documentation"
    steps:
      env:
        CHECKSUM: "NEW_CHECKSUM"
```


