# StreamFlow

[![CWL Conformance](https://github.com/alpha-unito/streamflow/actions/workflows/cwl-conformance.yaml/badge.svg?branch=master)](https://github.com/alpha-unito/streamflow/actions/workflows/cwl-conformance.yaml)

The [StreamFlow](https://streamflow.di.unito.it/) framework is a container-native *Workflow Management System (WMS)* written in Python 3.
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

Please note that StreamFlow requires `python >= 3.8`. Then you can execute it directly from the CLI

```bash
streamflow run /path/to/streamflow.yml
```

#### Docker

StreamFlow Docker images are available on [Docker Hub](https://hub.docker.com/r/alphaunito/streamflow). In order to run
a workflow inside the StreamFlow image
 - A StreamFlow project, containing a `streamflow.yml` file and all the other relevant dependencies (e.g. a CWL
   description of the workflow steps and a Helm description of the execution environment) needs to be mounted as a volume
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
    alphaunito/streamflow run /streamflow/project/streamflow.yml
```

#### Kubernetes

It is also possible to execute the StreamFlow container as a `Job` in [Kubernetes](https://kubernetes.io/).
In this case, StreamFlow is able to deploy `Helm` charts directly on the parent cluster through the
`ServiceAccount` credentials. In order to do that, the `inCluster` option must be set to `true` for each
involved module on the `streamflow.yml` file

```yaml
deployments:
  helm-deployment:
    type: helm
    config:
      inCluster: true
      ...
```

A `Helm` template of a StreamFlow `Job` can be found in the `helm/chart` folder.

Please note that, in case [RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) is active on the
Kubernetes cluster, a proper `RoleBinding` must be attached to the `ServiceAccount` object, in order to give
StreamFlow the permissions to manage deployments of pods and executions of tasks.

## CWL Compatibility

StreamFlow relies on the [Common Workflow Language](https://www.commonwl.org/) (CWL) standard to design workflow models. CWL conformance badges for StreamFlow are reported below.

### CWL v1.0

#### Classes

![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/command_line_tool.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/expression_tool.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/workflow.json)

#### Required features

![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/required.json)

#### Optional features

![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/docker.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/env_var.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/initial_work_dir.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/inline_javascript.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/multiple_input.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/resource.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/scatter.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/schema_def.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/shell_command.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/step_input.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.0/subworkflow.json)

### CWL v1.1

#### Classes

![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/command_line_tool.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/expression_tool.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/workflow.json)

#### Required features

![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/required.json)

#### Optional features

![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/docker.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/env_var.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/format_checking.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/initial_work_dir.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/inline_javascript.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/inplace_update.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/input_object_requirements.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/multiple_input.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/networkaccess.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/resource.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/scatter.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/schema_def.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/shell_command.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/step_input_expression.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/step_input.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/subworkflow.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.1/timelimit.json)

### CWL v1.2

#### Classes

![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/command_line_tool.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/expression_tool.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/workflow.json)

#### Required features

![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/required.json)

#### Optional features

![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/conditional.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/docker.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/env_var.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/format_checking.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/initial_work_dir.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/inline_javascript.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/inplace_update.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/input_object_requirements.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/load_listing.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/multiple_input.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/multiple.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/networkaccess.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/resource.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/scatter.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/schema_def.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/shell_command.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/step_input.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/subworkflow.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/timelimit.json)
![](https://badgen.net/https/streamflow.di.unito.it/cwl-conformance/v1.2/work_reuse.json)

## Contribute to StreamFlow

As a first step, get StreamFlow from [GitHub](https://github.com/alpha-unito/streamflow) 
```bash
git clone git@github.com:alpha-unito/streamflow.git
```

Then you can install all the requred packages using the `pip install` command

```bash
cd streamflow
pip install -r requirements.txt
```

StreamFlow relies on [GitHub Actions](https://github.com/features/actions) for PyPI and Docker Hub distributions. Therefore, in order to publish a
new version of the software, you only have to augment the version number in `version.py` file.

## StreamFlow Team

Iacopo Colonnelli <iacopo.colonnelli@unito.it> (creator and maintainer)  
Barbara Cantalupo <barbara.cantalupo@unito.it> (maintainer)  
Marco Aldinucci <aldinuc@di.unito.it> (maintainer)

Gaetano Saitta <gaetano.saitta@edu.unito.it> (contributor)  
Alberto Mulone <alberto.mulone@edu.unito.it> (contributor)
