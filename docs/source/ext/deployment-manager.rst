=================
DeploymentManager
=================

The ``DeploymentManager`` interface instantiates and manages :ref:`Connector <Connector>` objects for each ``deployment`` object described in a :ref:`StreamFlow file <Put it all together>`. It is defined in the ``streamflow.core.deployment`` module and exposes several public methods:

.. code-block:: python

    async def close(self) -> None:
        ...

    async def deploy(
        self, deployment_config: DeploymentConfig
    ) -> None:
        ...

    def get_connector(
        self, deployment_name: str
    ) -> Connector | None:
        ...

    async def undeploy(
        self, deployment_name: str
    ) -> None:
        ...

    async def undeploy_all(self) -> None:
        ...

The ``deploy`` method instantiates a ``Connector`` object starting from the given ``DeploymentConfig`` object, which derives from the ``deployments`` section in the StreamFlow file. Then, it deploys the related execution environment by calling the ``deploy`` method of the ``Connector`` object. Note that if a deployment ``wraps`` another environment (see :ref:`here <Stacked locations>`), the wrapped environment must be deployed before the wrapper one. It is in charge of each ``DeploymentManager`` implementation to correctly manage these dependencies, potentially throwing a ``WorkflowDefinitionException`` in case of misspecifications (e.g., circular dependencies). Also, it is in charge of the ``DeploymentManager`` to correctly handle concurrent calls to the ``deploy`` method with the same target deployment, e.g., to avoid spurious multiple deployments of identical infrastructures.

The ``get_connector`` method returns the ``Connector`` object related to the ``deployment_name`` input parameter or ``None`` if the environment has not been deployed yet. Note that calling ``get_connector`` before calling ``deploy`` or after calling ``undeploy`` on the related environment should always return ``None``.

The ``undeploy`` method undeploys the target execution infrastructure, identified by the ``deployment_name`` input parameter, by calling the ``undeploy`` method of the related ``Connector`` object. Plus, it marks the ``Connector`` object as invalid. It is in charge of the ``DeploymentManager`` to correctly handle concurrent calls to the ``undeploy`` method with the same target deployment.

The ``undeploy_all`` method undeploys all the active execution environments. It is equivalent to calling the ``undeploy`` method on each active deployment. StreamFlow always calls this method before terminating to clean up the execution interface.

The ``close`` method receives no input parameter and does not return anything. It frees stateful resources potentially allocated during the object’s lifetime, e.g., network or database connections.

Implementations
===============

=======     ======================================================
Type        Class
=======     ======================================================
default     streamflow.deployment.manager.DefaultDeploymentManager
=======     ======================================================
