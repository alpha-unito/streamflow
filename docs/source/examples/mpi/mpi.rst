===============
MPI Application
===============

This tutorial walks you through creating and running a StreamFlow workflow that compiles and
executes a simple MPI C++ program. By the end you will have a working project that:

- describes a two-step :ref:`CWL workflow <Write your workflow>` (``compile`` → ``execute``),
- binds the steps to :ref:`Docker Compose <DockerComposeConnector>` containers that provide an OpenMPI environment, and
- runs the program across two MPI nodes managed by StreamFlow.

The example uses Docker Compose for local execution. Kubernetes and Helm alternatives are
described in :ref:`Alternative deployments`.

Concepts covered
================

- **Multi-step CWL workflows** — composing ``CommandLineTool`` steps with a
  :ref:`CWL workflow <Write your workflow>` document that passes outputs between them.
- **StreamFlow configuration** — the ``workflows``, ``bindings``, and ``deployments`` keys
  in ``streamflow.yml`` that wire CWL steps to execution environments
  (see :ref:`Put it all together`).
- **DockerComposeConnector** —  launching a multi-replica Docker Compose service as the
  deployment backend for local execution (see :ref:`DockerComposeConnector`).
- **Multi-location execution** — using the ``locations`` binding key to allocate multiple
  container replicas for a single step, and consuming the injected ``$STREAMFLOW_HOSTS``
  variable to pass the host list to ``mpirun``.
- **Swappable deployment backends** — replacing Docker Compose with Kubernetes or Helm by
  changing only ``streamflow.yml``, without touching the CWL files (see :ref:`KubernetesConnector`
  and :ref:`Helm4Connector`).
- **Running workflows** — invoking ``streamflow run`` or the :ref:`CWL Runner` interface.

Prerequisites
=============

- StreamFlow installed — see :ref:`Install`.
- Docker and Docker Compose installed and the Docker daemon running.
- Basic familiarity with CWL `Workflow <https://www.commonwl.org/user_guide/topics/workflows.html>`_ and `CommandLineTool <https://www.commonwl.org/user_guide/topics/command-line-tool.html>`_ documents.
- **Optional:** a Kubernetes cluster (with ``kubectl``) and Helm — only needed for
  the :ref:`Alternative deployments` section.

Project layout
==============

Create the following directory structure. Each file is explained and created step by step in the
sections below:

.. code-block:: text

    mpi/
    ├── streamflow.yml                  # StreamFlow entrypoint
    ├── cwl/
    │   ├── main.cwl                    # CWL Workflow
    │   ├── config.yml                  # Workflow inputs
    │   └── clt/
    │       ├── compile.cwl             # CommandLineTool: mpicxx
    │       └── execute.cwl             # CommandLineTool: mpirun
    ├── data/
    │   └── cs.cxx                      # MPI C++ source program
    └── environment/
        ├── id_rsa                      # Demo SSH private key
        ├── id_rsa.pub                  # Demo SSH public key
        └── docker-compose/
            └── docker-compose.yml      # Docker Compose manifest

Run the following commands to create the directory tree, then change into the
project root (all subsequent paths are relative to ``mpi/``):

.. code-block:: bash

    mkdir -p mpi/cwl/clt mpi/data mpi/environment/docker-compose
    cd mpi

Step 1 — Write the MPI program
==============================

Create ``data/cs.cxx``. This is a minimal MPI client-server program (courtesy of
`Prof. Marco Aldinucci <https://alpha.di.unito.it/marco-aldinucci/>`_, 2010). Process ``0``
acts as a server that squares numbers sent by client processes; each other process sends a
random number of requests and then signals termination.

.. literalinclude:: data/cs.cxx
   :language: cpp

Step 2 — Create the CWL command line tools
==========================================

Compile tool (``cwl/clt/compile.cwl``)
--------------------------------------

This ``CommandLineTool`` invokes:

.. code-block:: bash

    mpicxx -O3 -o <name> <source_file>

The output is the compiled executable, whose name is derived from the source file's basename
(without extension) via the
`parameter reference <https://www.commonwl.org/user_guide/topics/parameter-references.html>`_
``$(inputs.source_file.nameroot)``.

Create ``cwl/clt/compile.cwl``:

.. literalinclude:: cwl/clt/compile.cwl
   :language: yaml

Execute tool (``cwl/clt/execute.cwl``)
--------------------------------------

This ``CommandLineTool`` invokes:

.. code-block:: bash

    mpirun -np <num_processes> --host $STREAMFLOW_HOSTS --allow-run-as-root <executable_file>

Three details deserve attention:

- `ShellCommandRequirement <https://www.commonwl.org/v1.1/CommandLineTool.html#ShellCommandRequirement>`_ is required because ``$STREAMFLOW_HOSTS`` is passed unquoted
  (``shellQuote: false``) so that the shell expands it as a comma-separated host list.
- ``$STREAMFLOW_HOSTS`` is an environment variable injected by StreamFlow at runtime when a step
  is bound to ``locations: 2`` (or more). It contains the hostnames of all allocated nodes,
  separated by commas, exactly as ``mpirun --host`` expects.
- ``stdout: mpi_output.log`` captures the tool's standard output as a named CWL output file.
  See `Capturing Standard Output <https://www.commonwl.org/user_guide/topics/outputs.html#capturing-standard-output>`_
  in the CWL user guide for details.

Create ``cwl/clt/execute.cwl``:

.. literalinclude:: cwl/clt/execute.cwl
   :language: yaml

Step 3 — Write the CWL Workflow
===============================

The workflow connects the two tools in sequence. The ``compile`` step receives ``source_file``
from the workflow inputs and produces ``executable_file``. The ``execute`` step receives
``executable_file`` from ``compile`` and ``num_processes`` from the workflow inputs. Its
``mpi_output`` port is exported as the workflow output ``result``.

.. code-block:: text

    source_file       num_processes
         |                  |
         v                  |
    +-----------+           |
    |  compile  |           |
    +-----------+           |
         |                  |
         v                  v
    +----------------------------+
    |          execute           |
    +----------------------------+
                 |
                 v
           mpi_output
                 |
                 v
              result

Create ``cwl/main.cwl``:

.. literalinclude:: cwl/main.cwl
   :language: yaml

Step 4 — Provide workflow inputs
================================

Create ``cwl/config.yml``. This file tells StreamFlow which file to compile and how many MPI
processes to launch. The path ``../data/cs.cxx`` is relative to ``config.yml`` itself (inside
``cwl/``), so it resolves to the ``data/`` directory at the project root.

.. literalinclude:: cwl/config.yml
   :language: yaml

Step 5 — Set up the Docker Compose environment
==============================================

The ``everpeace/kube-openmpi:0.7.0`` image provides a ready-to-use OpenMPI environment with an
SSH daemon. StreamFlow needs passwordless SSH between nodes to run ``mpirun``; the Docker Compose
file achieves this by bind-mounting a key pair into each container.

Generate an SSH key pair
------------------------

From the ``mpi/`` directory, generate an SSH key pair:

.. code-block:: bash

    ssh-keygen -t rsa -b 2048 -N "" -f environment/id_rsa

This creates ``environment/id_rsa`` (private key) and
``environment/id_rsa.pub`` (public key).

.. note::

   If you already have a compatible RSA key pair, copy the private key to
   ``environment/id_rsa`` and the public key to ``environment/id_rsa.pub``
   instead of generating a new one.

Write the Docker Compose manifest
---------------------------------

Key points:

- ``deploy.replicas: 2`` defines two container replicas. StreamFlow reads this when allocating
  locations for the ``execute`` step.
- The ``id_rsa.pub`` is mounted as both the public key and ``authorized_keys`` so that each
  container trusts any holder of ``id_rsa``.
- Port ``2022`` is the SSH port used by the image.
- ``stdin_open: true`` keeps the container's standard input open. The
  ``everpeace/kube-openmpi`` entrypoint (the SSH daemon) requires this to stay alive.
- ``platform: linux/amd64`` pins the image to the x86_64 variant. The
  ``everpeace/kube-openmpi:0.7.0`` image is ``linux/amd64`` only, so this key is
  required on non-x86_64 hosts (e.g. Apple Silicon).

Create ``environment/docker-compose/docker-compose.yml``:

.. literalinclude:: environment/docker-compose/docker-compose.yml
   :language: yaml

Step 6 — Write the StreamFlow configuration
===========================================

``streamflow.yml`` ties together all the components — see :ref:`Put it all together` for the
full reference. It declares:

- **workflows**: which CWL file to run and which inputs file to use.
- **bindings**: which deployment handles each step, and how many locations to allocate
  (see :ref:`Binding steps and deployments`).
- **deployments**: the connectors that manage execution environments
  (see :ref:`Import your environment` and :ref:`DockerComposeConnector`).

A few items in the file deserve explanation:

- **Shebang** (``#!/usr/bin/env streamflow``): makes the file directly executable on systems
  where ``streamflow`` is on the ``PATH``, as an alternative to
  ``streamflow run streamflow.yml``.
- ``compatibility: true`` enables Docker Compose v1-style compatibility mode, which allows
  ``deploy.replicas`` to be honoured outside of Swarm mode. Required for the
  ``deploy.replicas`` key to take effect with Docker Compose v2.
- ``projectName: openmpi`` sets a fixed project name for the Docker Compose stack so that
  container names are predictable regardless of the directory from which StreamFlow is invoked.

Binding notes:

- ``step: /compile`` is the CWL step path (``/`` + step name from ``main.cwl``).
- The ``/compile`` binding has no ``locations`` key because the compilation step runs on a
  single container — no multi-host coordination is needed.
- ``locations: 2`` tells StreamFlow to allocate two replicas of the ``openmpi`` service for
  the ``execute`` step. StreamFlow then sets ``$STREAMFLOW_HOSTS`` to the comma-separated
  list of those two hostnames. This value must be ≤ ``deploy.replicas`` in
  ``docker-compose.yml``.

Create ``streamflow.yml``:

.. literalinclude:: streamflow.yml
   :language: yaml

Step 7 — Run the workflow
=========================

From the ``mpi/`` directory:

.. code-block:: bash

    streamflow run streamflow.yml

StreamFlow will:

1. Deploy the ``dc-mpi`` Docker Compose environment (two ``openmpi`` containers).
2. Run the ``compile`` step inside one container to produce the ``cs`` executable.
3. Run the ``execute`` step across both containers using ``mpirun --host $STREAMFLOW_HOSTS``.
4. Copy output files back to the local output directory.
5. Undeploy the Docker Compose environment.

You can also use the :ref:`CWL Runner` interface:

.. code-block:: bash

    cwl-runner --streamflow-file streamflow.yml cwl/main.cwl cwl/config.yml

For verbose output during development:

.. code-block:: bash

    streamflow run --debug streamflow.yml

Expected output
===============

StreamFlow prints a JSON summary of the workflow outputs. The ``path`` field gives the
absolute local path to ``mpi_output.log``; StreamFlow copies the file back to your project
root so the path ends inside the ``mpi/`` directory you created:

.. code-block:: json

    {
        "result": {
            "class": "File",
            "location": "file:///absolute/path/to/mpi/mpi_output.log",
            "path": "/absolute/path/to/mpi/mpi_output.log",
            "basename": "mpi_output.log",
            "dirname": "/absolute/path/to/mpi",
            "nameroot": "mpi_output",
            "nameext": ".log",
            "checksum": "sha1$...",
            "size": 123
        }
    }

Because the file is copied to the project root, you can open it directly from the ``mpi/``
directory:

.. code-block:: bash

    cat mpi_output.log

The file ``mpi_output.log`` contains lines similar to:

.. code-block:: console

    Warning: Permanently added '[172.18.0.2]:2022' (RSA) to the list of known hosts.
    Hello I'm the server with id 0 on <hostname> out of 2 I'm the server
    Hello I'm 1 on <hostname> out of 2 I'm a client
    [server] Request from 1 : 3 --> 9
    EOS from 1 received
    Total time (MPI) 0 is 0.000105
    Total time (MPI) 1 is 0.000120

The exact values and order of lines will vary between runs because the number of requests
sent by each client is randomised.

.. note::

   The first line (``Warning: Permanently added ...``) is an SSH host-key notice that
   appears the first time StreamFlow connects to a container. It is harmless and can be
   ignored. Additional lines from MPI's internal stderr (e.g. ``N. req`` counts) may also
   appear mixed in with the program output.

.. _Alternative deployments:

Alternative deployments
=======================

The CWL files remain exactly the same for every backend. **Pick one** of the subsections
below and follow only that one. Each subsection provides a complete ``streamflow.yml``
for that backend; use it *instead of* the one created in Step 6, then run
``streamflow run streamflow.yml`` as usual.

.. _Run with Kubernetes:

Run with Kubernetes
-------------------

Ensure a Kubernetes cluster is accessible via your default ``kubeconfig``
(``~/.kube/config``). Verify connectivity before continuing:

.. code-block:: bash

    kubectl cluster-info

Create the SSH secret directly with ``kubectl``, pointing at the key pair generated in
Step 5:

.. code-block:: bash

    kubectl create secret generic streamflow-ssh-key \
      --from-file=id_rsa=environment/id_rsa \
      --from-file=id_rsa.pub=environment/id_rsa.pub \
      --from-file=authorized_keys=environment/id_rsa.pub

Create the directory ``environment/k8s/`` and add ``deployment.yaml``. It defines a
two-replica ``Deployment`` that mounts the ``streamflow-ssh-key`` secret into each pod at
``/ssh-key/openmpi``:

.. literalinclude:: environment/k8s/deployment.yaml
   :language: yaml

Replace ``streamflow.yml`` with the following (see :ref:`KubernetesConnector` for all
available options). The highlighted lines show the parts that differ from the Docker
Compose configuration: the deployment name (``k8s-mpi``), its type (``kubernetes``),
the manifest file path, and ``workdir``:

.. literalinclude:: streamflow-k8s.yml
   :language: yaml
   :emphasize-lines: 12, 16, 20-25

.. note::

   ``workdir: /tmp`` is required. StreamFlow uses ``workdir`` as the base for the
   temporary directory (``TMPDIR``) it sets inside each pod. OpenMPI's PMIx layer
   creates Unix domain sockets under ``TMPDIR``; if the resulting socket path exceeds
   Linux's 108-character ``UNIX_PATH_MAX`` limit, ``mpirun`` fails silently with
   exit code 213. Setting ``workdir: /tmp`` keeps the path short enough to stay within
   that limit.

Run with Helm
-------------

`Helm <https://helm.sh>`_ is a package manager for Kubernetes that bundles manifests into
reusable *charts*. The steps below create a minimal chart equivalent to the Kubernetes
manifest from the previous subsection.

Ensure Helm is configured and ``kubectl`` can reach your cluster via the default
``kubeconfig`` (``~/.kube/config``).

Create the SSH secret directly with ``kubectl``, pointing at the key pair generated in
Step 5. If you already created it for the Kubernetes deployment, skip this step:

.. code-block:: bash

    kubectl create secret generic streamflow-ssh-key \
      --from-file=id_rsa=environment/id_rsa \
      --from-file=id_rsa.pub=environment/id_rsa.pub \
      --from-file=authorized_keys=environment/id_rsa.pub

Scaffold the chart:

.. code-block:: bash

    mkdir -p environment/helm
    helm create environment/helm/openmpi

This generates ``environment/helm/openmpi/`` with a default chart structure.
Remove the template files that were scaffolded for a generic web application and
are not needed for an OpenMPI deployment:

.. code-block:: bash

    rm environment/helm/openmpi/templates/{hpa,httproute,ingress,service,serviceaccount}.yaml
    rm environment/helm/openmpi/templates/NOTES.txt
    rm -r environment/helm/openmpi/templates/tests/

You can verify that only ``_helpers.tpl`` and ``deployment.yaml`` remain:

.. code-block:: bash

    ls environment/helm/openmpi/templates/

``Chart.yaml`` — replace its contents with the following to set the correct
description and application version:

.. literalinclude:: environment/helm/openmpi/Chart.yaml
   :language: yaml

``values.yaml`` — replace its contents with the following, which removes the
scaffolded Nginx defaults and adds the OpenMPI image and SSH secret name:

.. literalinclude:: environment/helm/openmpi/values.yaml
   :language: yaml

``templates/_helpers.tpl`` is generated by ``helm create`` with six named template
blocks. Remove the final ``openmpi.serviceAccountName`` block — the chart does not
use a service account. After editing, the file should look like this:

.. literalinclude:: environment/helm/openmpi/templates/_helpers.tpl
   :language: text

``templates/deployment.yaml`` — replace its contents with the following, which
configures the OpenMPI container and mounts the secret named by ``sshSecretName``
into each pod at ``/ssh-key/openmpi``:

.. literalinclude:: environment/helm/openmpi/templates/deployment.yaml
   :language: yaml

Replace ``streamflow.yml`` with the following (see :ref:`Helm4Connector` for all
available options). The highlighted lines show the parts that differ from the Docker
Compose configuration: the deployment name (``helm-mpi``), its type (``helm``), the
chart path, the release name, and ``workdir``:

.. literalinclude:: streamflow-helm.yml
   :language: yaml
   :emphasize-lines: 12, 16, 20-25

.. note::

   ``workdir: /tmp`` is required here for the same reason as with Kubernetes: it keeps
   the PMIx Unix-socket path within Linux's 108-character ``UNIX_PATH_MAX`` limit.
   See the note in the :ref:`Run with Kubernetes` subsection above for details.

Troubleshooting
===============

- **mpirun refuses to run as root**: some images disallow running ``mpirun`` as root. The
  ``--allow-run-as-root`` flag is already set in ``execute.cwl``; if the error persists, switch
  to a non-root user in the container image.
- **Execute step only gets one host**: verify that the chosen deployment supports multiple
  replicas and that ``locations: 2`` is set in the binding. Check StreamFlow debug logs for
  allocation details.
- **SSH connection refused**: confirm the key pair files are present in
  ``environment/`` (``id_rsa`` and ``id_rsa.pub``) and that the Docker daemon can
  bind-mount them.
- **Image pull fails (Kubernetes / Helm)**: ensure the cluster nodes can reach Docker Hub.
  Pull the image manually to verify: ``docker pull everpeace/kube-openmpi:0.7.0``.
- **Wrong cluster / context (Kubernetes / Helm)**: run ``kubectl config current-context``
  to confirm you are targeting the intended cluster.
- **Helm not found**: ensure ``helm`` is on your ``PATH`` and run ``helm version`` to
  verify the installation.
- **mpirun exits with code 213**: this is a PMIx error caused by a Unix domain socket
  path that exceeds Linux's 108-character ``UNIX_PATH_MAX`` limit. Ensure
  ``workdir: /tmp`` is set in the ``deployments`` section of your ``streamflow.yml``.

Cleanup
=======

StreamFlow undeploys Docker Compose, Kubernetes, and Helm environments automatically
when the workflow finishes. The only resource that requires manual cleanup is the SSH
secret created in the Kubernetes and Helm subsections:

.. code-block:: bash

    kubectl delete secret streamflow-ssh-key

