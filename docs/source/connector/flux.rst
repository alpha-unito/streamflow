=============
FluxConnector
=============

The `Flux Framework <https://flux-framework.org/>`_ connector allows running jobs on a cluster with Flux Framework in a High Performance Computing Context. Although Flux can work in a local testing container or a cloud environment and has a Python SDK, to match the design here, we follow suit and inherit from the :ref:`QueueManagerConnector <QueueManagerConnector>`. In this way, users can offload jobs to local or remote PBS controllers using the :ref:`stacked locations <Stacked locations>` mechanism. The HPC facility is supposed to be constantly active, reducing the deployment phase to deploy the inner connector (e.g., to create an :ref:`SSHConnection <SSHConnection>` pointing to an HPC login node).

.. warning::

   Note that in StreamFlow ``v0.1``, the ``QueueManagerConnector`` directly inherited from the :ref:`SSHConnector <SSHConnector>` at the implementation level. Consequently, all the properties needed to open an SSH connection to the HPC login node (e.g., ``hostname``, ``username``, and ``sshKey``) were defined directly in the ``QueueManagerConnector``. This path is still supported by StreamFlow ``v0.2``, but it is deprecated and will be removed in StreamFlow ``v0.3``.

Interaction with the Flux scheduler happens through a Bash script with ``#flux`` directives. Users can pass the path of a custom script to the connector using the ``file`` attribute of the :ref:`FluxService <FluxService>` configuration. This file is interpreted as a `Jinja2 <https://jinja.palletsprojects.com/>`_ template and populated at runtime by the connector. Alternatively, users can pass PBS options directly from YAML using the other options of a :ref:`FluxService <FluxService>` object.

As an example, suppose to have a Flux template script called ``batch.sh``, with the following content:

.. code-block:: bash

    #!/bin/bash

    #flux --nodes=1
    #flux --queue=queue_name

    {{streamflow_command}}

A PBS deployment configuration which uses the ``batch.sh`` file to spawn jobs can be written as follows:

.. code-block:: yaml

   deployments:
     flux-example:
       type: pbs
       config:
         services:
           example:
             file: batch.sh

Alternatively, the same behaviour can be recreated by directly passing options through the YAML configuration, as follows:

.. code-block:: yaml

   deployments:
     flux-example:
       type: pbs
       config:
         services:
           example:
             nodes: 1
             queue: queue_name

Being passed directly to the ``flux batch`` command line, the YAML options have higher priority than the file-based ones.

.. warning::

    Note that the ``file`` property in the upper configuration level, i.e., outside a ``service`` definition, is still supported in Streamflow ``v0.2``, but it is deprecated and will be removed in StreamFlow ``v0.3``.

For a quick demo or tutorial, see our `example workflow <https://github.com/alpha-unito/streamflow/tree/master/examples/flux>`_.

.. jsonschema:: ../../../streamflow/deployment/connector/schemas/flux.json
    :lift_description: true
    :lift_definitions: true
    :auto_reference: true
    :auto_target: true
