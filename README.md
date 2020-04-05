# StreamFlow

[![Build Status](https://travis-ci.com/alpha-unito/streamflow.svg?branch=master)](https://travis-ci.com/alpha-unito/streamflow)

The StreamFlow framework is a container-native *Workflow Management System (WMS)* written in Python 3.
It has been designed around two main principles:
* Allow the execution of tasks in **multi-container environments**, in order to support concurrent execution
of multiple communicating tasks in a multi-agent ecosystem.
* Relax the requirement of a single shared data space, in order to allow for **hybrid workflow** executions on top of
multi-cloud or hybrid cloud/HPC infrastructures.

## Use StreamFlow

#### PyPI

The StreamFlow module is available on [PyPI](https://pypi.org/project/streamflow/), so you can install it using pip.

```bash
pip install streamflow
```

Please note that StreamFlow requires `python >= 3.7`. Then you can execute it directly from the CLI

```bash
streamflow /path/to/streamflow.yml
```

#### Docker

StreamFlow Docker images are available on [Docker Hub](https://hub.docker.com/r/alphaunito/streamflow). In order to run
a workflow inside the StreaFlow image
 - A StreamFlow project, containing a `streamflow.yml` file and all the other relevant dependencies (e.g. a CWL
   description of the workflow steps and a Helm description of the execution environment) need to be mounted as a volume
   inside the container, for example in the `/streamflow/project` folder
 - Workflow outputs, if any, will be stored in the `/streamflow/results` folder. Therefore, it is necessary to mount
   such location as a volume in order to persist the results
 - StreamFlow will save all its temporary files inside the `/tmp/streamflow` location. For debugging purposes, or in
   order to improve I/O performances in case of huge files, it could be useful to mount also such location as a volume
 - The path of the `streamflow.yml` file **inside the container** (e.g. `/streamflow/project/streamflow.yml`) must be
   passed as an argument to the Docker container

The script below gives an example of StreamFlow execution in a Docker container

```bash
docker run -d \
    --mount type=bind,source="$(pwd)"/my-project,target=/streamflow/project \
    --mount type=bind,source="$(pwd)"/results,target=/streamflow/results \
    --mount type=bind,source="$(pwd)"/tmp,target=/tmp/streamflow \
    alphaunito/streamflow \
    /streamflow/project/streamflow.yml
```

#### Kubernetes

It is also possible to execute the StreamFlow container as a `Job` in [Kubernetes](https://kubernetes.io/).
In this case, StreamFlow is able to deploy `Helm` models directly on the parent cluster through the
`ServiceAccount` credentials. In order to do that, the `inCluster` option must be set to `true` for each
involved module on the `streamflow.yml` file

```yaml
models:
  helm-model:
    type: helm
    config:
      inCluster: true
      ...
```

A `Helm` template of a StreamFlow `Job` can be found in the `helm/chart` folder.

Please note that, in case [RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) is active on the
Kubernetes cluster, a proper `RoleBinding` must be attached to the `ServiceAccount` object, in order to give
StreamFlow the permissions to manage deployments of pods and executions of tasks.

## Contribute to StreamFlow

StreamFlow uses [pipenv](https://pipenv.kennethreitz.org/en/latest/) to guarantee deterministic builds.
Therefore, the recommended way to manage dependencies is by means of the `pipenv` command.

As a first step, get StreamFlow from [GitHub](https://github.com/alpha-unito/streamflow) 
```bash
git clone git@github.com:alpha-unito/streamflow.git
```

Then you can install all the requred packages using the `pipenv` command
```bash
pip install --user pipenv
cd streamflow
pipenv install
```

Finally, you can run StreamFlow in the generated virtual environment
```bash
pipenv run python -m streamflow
```

StreamFlow relies on [Travis CI](https://travis-ci.com/) for PyPI and Docker Hub distributions. Therefore, in order to publish a
new version of the software, you only have to augment the version number in `version.py` file.

## StreamFlow Team

Iacopo Colonnelli <iacopo.colonnelli@unito.it> (creator and maintainer)  
Barbara Cantalupo <barbara.cantalupo@unito.it> (maintainer)  
Marco Aldinucci <aldinuc@di.unito.it> (maintainer)

Gaetano Saitta <gaetano.saitta@edu.unito.it> (contributor)
