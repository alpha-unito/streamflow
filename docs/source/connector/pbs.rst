=====================
PBSConnector
=====================

The `PBS <https://www.openpbs.org/>`_ connector allows offloading execution to High-Performance Computing (HPC) facilities orchestrated by the PBS queue manager. It extends the :ref:`QueueManagerConnector <QueueManagerConnector>`, which inherits from the :ref:`ConnectorWrapper <ConnectorWrapper>` interface, allowing users to offload jobs to local or remote PBS controllers using the :ref:`stacked locations <Stacked locations>` mechanism. The HPC facility is supposed to be constantly active, reducing the deployment phase to deploy the inner connector (e.g., to create an :ref:`SSHConnection <SSHConnection>` pointing to an HPC login node).

.. warning::

   Note that in StreamFlow ``v0.1``, the ``QueueManagerConnector`` directly inherited from the :ref:`SSHConnector <SSHConnector>` at the implementation level. Consequently, all the properties needed to open an SSH connection to the HPC login node (e.g., ``hostname``, ``username``, and ``sshKey``) were defined directly in the ``QueueManagerConnector``. This path is still supported by StreamFlow ``v0.2``, but it is deprecated and will be removed in StreamFlow ``v0.3``.

Interaction with the PBS scheduler happens through a Bash script with ``#PBS`` directives. Users can pass the path of a custom script to the connector using the ``file`` attribute of the :ref:`PBSService <PBSService>` configuration. This file is interpreted as a `Jinja2 <https://jinja.palletsprojects.com/>`_ template and populated at runtime by the connector. Alternatively, users can pass PBS options directly from YAML using the other options of a :ref:`PBSService <PBSService>` object.

As an example, suppose to have a PBS template script called ``qsub.sh``, with the following content:

.. code-block:: bash

    #!/bin/bash

    #PBS -l nodes=1
    #PBS -q queue_name
    #PBS -l mem=1gb

    {{streamflow_command}}

A PBS deployment configuration which uses the ``qsub.sh`` file to spawn jobs can be written as follows:

.. code-block:: yaml

   deployments:
     pbs-example:
       type: pbs
       config:
         services:
           example:
             file: qsub.sh

Alternatively, the same behaviour can be recreated by directly passing options through the YAML configuration, as follows:

.. code-block:: yaml

   deployments:
     pbs-example:
       type: pbs
       config:
         services:
           example:
             destination: queue_name
             resources:
               mem: 1gb
               nodes: 1

Being passed directly to the ``qsub`` command line, the YAML options have higher priority than the file-based ones.

.. warning::

    Note that the ``file`` property in the upper configuration level, i.e., outside a ``service`` definition, is still supported in Streamflow ``v0.2``, but it is deprecated and will be removed in StreamFlow ``v0.3``.

The unit of binding is the entire HPC facility. In contrast, the scheduling unit is a single job placement in the PBS queue. Users can limit the maximum number of concurrently placed jobs by setting the ``maxConcurrentJobs`` parameter.

.. jsonschema:: ../../../streamflow/deployment/connector/schemas/pbs.json
    :lift_description: true
    :lift_definitions: true
    :auto_reference: true
    :auto_target: true