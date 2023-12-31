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


StreamFlow relies on [GitHub Actions](https://github.com/features/actions) for PyPI and Docker Hub distributions. Therefore, in order to publish a
new version of the software, you only have to augment the version number in the `version.py` file.


StreamFlow relies on GitHub Actions, as well as for the Continuous Integration (CI) tests.
Therefore, it is suggested that some tests be done locally before opening a Pull Request.


### CWL conformance
StreamFlow complies with the [Common Workflow Language](https://www.commonwl.org/) (CWL) from the 1.0 to 1.2 version.
You can check if your contribution is still standard compliant with the command
```bash
./cwl-conformance-test.sh
```

### StreamFlow no regression tests
It is necessary that the contribution not introduce some errors in the correctness of the software.
Some regression tests are supplied using [pytest](https://docs.pytest.org/en/7.3.x/getting-started.html). Before it is necessary to install the required packages for the testing
```bash
pip install -r test-requirements.txt
```

You can execute all the available tests with the command
```bash 
pytest tests/test_*
```

StreamFlow has many different connectors, and some tests on these connectors are done. Currently, the tested connectors are local, docker, ssh, Kubernetes, and singularity.
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

The StreamFlow codebase is [PEP 8](https://peps.python.org/pep-0008/) compliant with code style.
Some automatic style checkers can be installed with
```bash
pip install -r lint-requirements.txt
```

You can verify that your contribution is compliant with the command
```bash
make format-check flake8 codespell-check
```

Otherwise, you can apply automatically the suggested changes. Use them carefully because they will write the file back. 
```bash
make format codespell
```





## Contribute to the documentation

### Generate the documentation
The documentation is written in the reStructuredText markup language, and [Sphinx](https://www.sphinx-doc.org/en/master/) software is used to generate different output formats. The command below installs all the required packages.
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


