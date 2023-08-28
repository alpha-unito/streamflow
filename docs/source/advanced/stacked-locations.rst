=================
Stacked locations
=================

StreamFlow supports the concept of stacked locations, adhering to the separation of concerns principle. This allows the user to describe complex execution environments, e.g., a :ref:`Singularity container <SingularityConnector>` launched by a :ref:`Slurm queue manager <SlurmConnector>` called through an :ref:`SSH connection <SSHConnector>`.

Users can define stacked locations using the ``wraps`` property in the :ref:`StreamFlow file <Put it all together>`. For example, consider a remote Slurm queue manager that can be contacted by connecting to the login node of an HPC facility using SSH. This is a typical configuration for HPC systems. Then a user can write:

.. code-block:: yaml

   deployments:
     ssh-hpc:
       type: ssh
       config:
         ...
     slurm-hpc:
       type: slurm
       config:
         ...
       wraps: ssh-hpc

.. warning::

   Note that in StreamFlow ``v0.1``, the queue manager connectors (:ref:`Slurm <SlurmConnector>` and :ref:`PBS <PBSConnector>`) are inherited from the :ref:`SSHConnector <SSHConnector>` at the implementation level. Consequently, all the properties needed to open an SSH connection to the HPC login node (e.g., ``hostname``, ``username``, and ``sshKey``) were defined directly in the ``config`` section of the queue manager deployment. This path is still supported by StreamFlow ``v0.2``, but it is deprecated and will be removed in StreamFlow ``v0.3``.

Note that not all deployment types can wrap other locations. Indeed, only connectors extending the :ref:`ConnectorWrapper <ConnectorWrapper>` interface support the ``wraps`` directive. Specifying the ``wraps`` directive on a container type that does not support it will result in an error during StreamFlow initialization. Conversely, if no explicit ``wraps`` directive is specified for a :ref:`ConnectorWrapper <ConnectorWrapper>`, it wraps the :ref:`LocalConnector <LocalConnector>`.

The ``wraps`` directive only supports wrapping a single inner location. However, a single location can be wrapped by multiple deployment definitions. The :ref:`DeploymentManager <DeploymentManager>` component is responsible for guaranteeing the correct order of deployment and undeployment for stacked locations.