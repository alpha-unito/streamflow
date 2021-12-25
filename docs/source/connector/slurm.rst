=====================
SlurmConnector
=====================

The `Slurm <https://slurm.schedmd.com/>`_ connector allows offloading execution to High-Performance Computing (HPC) facilities orchestrated by the Slurm queue manager. The HPC facility is supposed to be constantly active, reducing the deployment phase to creating an :ref:`SSHConnection <SSHConnection>` pointing to its login node.

Interaction with the Slurm scheduler happens through a Bash script with ``#SBATCH`` directives. Users can pass the path of a custom script to the connector using the ``file`` attribute. This file is interpreted as a `Jinja2 <https://jinja.palletsprojects.com/>`_ template and populated at runtime by the connector.

The unit of binding is the entire HPC facility. In contrast, the scheduling unit is a single job placement in the Slurm queue. Users can limit the maximum number of concurrently placed jobs by setting the ``maxConcurrentJobs`` parameter.

.. jsonschema:: ../../../streamflow/config/schemas/v1.0/queue_manager.json
    :lift_description: true
