# Contribute to StreamFlow

As a first step, clone StreamFlow from [GitHub](https://github.com/alpha-unito/streamflow)
```bash
git clone git@github.com:alpha-unito/streamflow.git
```

Then, install all the required packages using the `pip install` command

```bash
cd streamflow
pip install -r requirements.txt
```

Finally, install StreamFlow with the following command
```
pip install .
```

## Continuous Integration

StreamFlow relies on [GitHub Actions](https://github.com/features/actions) for Continuous Integration (CI) and Continuous Distribution (CD).
Only maintainers take care of the CD pipeline. In order to publish a new version of the software on PyPI and Docker Hub distributions, a maintainer only has to augment the version number in the `version.py` file.

Instead, everyone in the community can contribute to the StreamFlow codebase by opening a Pull Request (PR). Running the entire suite of StreamFlow tests is part of the CI pipeline.
However, it is also possible (and advisable) to run tests locally before opening a PR, as explained [below](#streamflow-tests).


### CWL conformance
StreamFlow complies with all stable versions of the [Common Workflow Language](https://www.commonwl.org/) (CWL) open standard (v1.0, v1.1, and v1.2). Plus, it implements the `cwltool:Loop` extension (see [here](https://cwltool.readthedocs.io/en/latest/loop.html)).
You can check if your PR does not compromise CWL compliance by running the CWL conformance tests suite, using the following command
```bash
./cwl-conformance-test.sh
```

### StreamFlow tests
Some regression tests are supplied using [pytest](https://docs.pytest.org/en/7.3.x/getting-started.html). Install the required packages using the following command
```bash
pip install -r test-requirements.txt
```

Then, you can execute all the available tests with the following command
```bash 
make test
```

Otherwise, you can execute only a specific test file. For example, to verify compliance with the `cwltool:Loop` CWL extension, you can use the following command
```bash
pytest tests/test_cwl_loop.py
```

### Testing connectors
StreamFlow comes with many different connectors OOTB, and some of the tests in the suite are used to verify their behaviour. Currently, the tested connectors are: `local`, `docker`, `ssh`, `kubernetes`, and `singularity`. The plan is to add no regression tests for all connectors.
Executing all these tests requires to install and configure several software packages:
- [Docker](https://docs.docker.com/engine/install/), which is required to test the `docker` and `ssh` connectors and the CWL `DockerRequirement` implementation with Docker containers;
- [Singularity](https://docs.sylabs.io/guides/3.0/user-guide/installation.html), which is required to test the `singularity` connector and the CWL `DockerRequirement` conversion to Singularity containers;
- A Kubernetes distribution, e.g., [minikube](https://minikube.sigs.k8s.io/docs/start/), which is used to test the `kubernetes` connector and the CWL `DockerRequirement` conversion to Kubernetes pods.

For development purposes, it is possible to execute local tests only on a specific subset of connectors using the `--deploys` option as follows
```bash 
pytest --deploys local,docker,singularity tests/test_remotepath.py
```
However, all connectors are always tested in the CI pipeline.



### Code style
StreamFlow supports Python 3.8 to 3.12, so use compatible code features.

The StreamFlow code style complies with [PEP 8](https://peps.python.org/pep-0008/).
Some automatic style checkers can be installed with
```bash
pip install -r lint-requirements.txt
```

You can verify that your PR respects the code style with the following command
```bash
make format-check flake8 codespell-check
```

Otherwise, you can apply automatically the suggested changes. Use them carefully because they will write the file back. 
```bash
make format codespell
```





## Contribute to the documentation

### Generate the documentation
The documentation is written in the reStructuredText markup language, and the [Sphinx](https://www.sphinx-doc.org/en/master/) framework is used to generate the HTML pages published on the [StreamFlow website](https://streamflow.di.unito.it/documentation/latest/). Currently, there is no CD for documentation: maintainers manually transfer files on the StreamFlow website at every release.

To check your local version of the documentation, first, install all the required packages using the following command
```bash
cd docs
pip install -r requirements.txt
```

Then, generate the documentation in HTML format with the following command
```bash
make html
```

If you want, you can also generate documentation in a different format from the ones supported by Sphinx. However, note that StreamFlow officially maintains only the HTML format. For more information on the available documentation formats, use the following command
```bash
make help
```

### Documentation changes
For any changes to the documentation, it is necessary to update the dedicated test on the CI pipeline.
In particular, the checksum of the documentation in the HTML format is tested to avoid misleading changes when new Sphinx versions are released.


The command below displays the new checksum in the shell
```bash
cd docs
make get-checksum
```

This new checksum must be copied and pasted into the `.github/workflows/ci-tests.yaml` file.
It must replace the value of the environment variable `CHECKSUM` of the job `documentation`.
```yaml
name: "CI Tests"
jobs:
  documentation:
    name: "Build Sphinx documentation"
    steps:
      env:
        CHECKSUM: "NEW_CHECKSUM"
```


