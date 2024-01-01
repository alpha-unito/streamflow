# Contribute to StreamFlow

As a first step, get StreamFlow from [GitHub](https://github.com/alpha-unito/streamflow) 
```bash
git clone git@github.com:alpha-unito/streamflow.git
```

Then, you can install all the required packages using the `pip install` command.

```bash
cd streamflow
pip install -r requirements.txt
```

Finally, you can install StreamFlow with the following command 
```
pip install .
```

## Continuous Integration

StreamFlow relies on [GitHub Actions](https://github.com/features/actions) for Continuous Integration (CI) and Continuous Distribution (CD).
The maintainers take care of the CD pipeline. In order to publish a new version of the software on PyPI and Docker Hub distributions, the maintainer only has to augment the version number in the `version.py` file.

Instead, everyone in the community can contribute to the StreamFlow codebase by opening a Pull Request (PR). Running the entire suite of StreamFlow tests is part of the CI pipeline.
However, it is suggested that some tests be done locally before opening the PR, below it is explained how to do it.


### CWL conformance
StreamFlow complies with all stable versions of the [Common Workflow Language](https://www.commonwl.org/) (CWL) open standard (1.0, 1.1, 1.2). Plus, it implements the `cwltool:Loop` extension (see [here](https://cwltool.readthedocs.io/en/latest/loop.html)).
You can check if your PR does not compromise CWL compliance by running the CWL conformance tests suite, using the following command
```bash
./cwl-conformance-test.sh
```

### StreamFlow tests
It is necessary that the contribution not introduce some errors in the current software features.
Some regression tests are supplied using [pytest](https://docs.pytest.org/en/7.3.x/getting-started.html). Before it is necessary to install the required packages for the testing
```bash
pip install -r test-requirements.txt
```

You can execute all the available tests with the command
```bash 
make test
```
Otherwise, specific tests with the command
```bash
pytest tests/test_scheduler.py tests/test_data_manager.py
```

StreamFlow has many different connectors, and some tests on these connectors are done. Currently, the tested connectors are local, Docker, SSH, Kubernetes, and Singularity.
Execute all these tests locally required installed: 
- [Docker](https://docs.docker.com/engine/install/). It is also required for SSH tests.
- [Singularity](https://docs.sylabs.io/guides/3.0/user-guide/installation.html)
- A local Kubernetes, e.g. [minikube](https://minikube.sigs.k8s.io/docs/start/)

For development goals, it is possible to execute local tests only on some specific connectors using the flag `--deploys`
```bash 
pytest --deploys local,docker,singularity tests/test_remotepath.py
```
However, all the deployments are always checked in the CI tests.



### Code style
StreamFlow supports Python 3.8 to 3.12, so use compatible code features.

The StreamFlow code style is compliant with [PEP 8](https://peps.python.org/pep-0008/).
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
The documentation is written in the reStructuredText markup language, and the [Sphinx](https://www.sphinx-doc.org/en/master/) framework is used to generate different output formats. The command below installs all the required packages.
```bash
cd docs
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

### Documentation changes
For any changes to the documentation, it is necessary to update the CI test.
In particular, the checksum of the documentation in the HTML format is tested to avoid misleading changes when new Sphinx versions are released.

The command below shows in stdout the checksum
```bash
cd docs
make get-checksum
```

The new checksum must be copied and pasted into the `.github/workflows/ci-tests.yaml` file.
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


