========
Database
========

StreamFlow relies on a persistent ``Database`` to store all the metadata regarding a workflow execution. These metadata are used for fault tolerance, provenance collection, and reporting. All StreamFlow entities interacting with the ``Database`` extend the ``PersistableEntity`` interface, adhering to the `Object-Relational Mapping (ORM) <https://en.wikipedia.org/wiki/Object%E2%80%93relational_mapping>`_ programming model:

.. code-block:: python

    def __init__(self):
        self.persistent_id: int | None = None

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> PersistableEntity:
        ...

    async def save(
        self, context: StreamFlowContext
    ) -> None:
        ...

Each ``PersistableEntity`` is identified by a unique numerical ``persistent_id`` related to the corresponding ``Database`` record. Two methods, ``save`` and ``load``, allow persisting the entity in the ``Database`` and retrieving it from the persistent record. Note that ``load`` is a class method, as it must construct a new instance.

The ``load`` method receives three input parameters: the current execution ``context``, the ``persistent_id`` of the instance that should be loaded, and a ``loading_context`` (see :ref:`DatabaseLoadingContext <DatabaseLoadingContext>`). Note that the ``load`` method should not directly assign the ``persistent_id`` to the new entity, as this operation is in charge to the :ref:`DatabaseLoadingContext <DatabaseLoadingContext>` class.

Persistence
===========

The ``Database`` interface, defined in the ``streamflow.core.persistence`` module, contains all the methods to create, modify, and retrieve this metadata. Data deletion is unnecessary, as StreamFlow never removes existing records. Internally, the ``save`` and ``load`` methods call one or more of these methods to perform the desired operations.

.. code-block:: python

    async def add_dependency(
        self, step: int, port: int, type: DependencyType, name: str
    ) -> None:
        ...

    async def add_deployment(
        self,
        name: str,
        type: str,
        config: str,
        external: bool,
        lazy: bool,
        workdir: str | None,
    ) -> int:
        ...

    async def add_execution(
        self, step_id: int, tag: str, cmd: str
    ) -> int:
        ...

    async def add_port(
        self,
        name: str,
        workflow_id: int,
        type: type[Port],
        params: MutableMapping[str, Any],
    ) -> int:
        ...

    async def add_provenance(
        self, inputs: MutableSequence[int], token: int
    ) -> None:
        ...

    async def add_step(
        self,
        name: str,
        workflow_id: int,
        status: int,
        type: type[Step],
        params: MutableMapping[str, Any],
    ) -> int:
        ...

    async def add_target(
        self,
        deployment: int,
        type: type[Target],
        params: MutableMapping[str, Any],
        locations: int = 1,
        service: str | None = None,
        workdir: str | None = None,
    ) -> int:
        ...

    async def add_token(
        self, tag: str, type: type[Token], value: Any, port: int | None = None
    ) -> int:
        ...

    async def add_workflow(
        self, name: str, params: MutableMapping[str, Any], status: int, type: str
    ) -> int:
        ...

    async def close(self) -> None:
        ...

    async def get_dependees(
        self, token_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    async def get_dependers(
        self, token_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    async def get_deployment(
        self, deployment_id: int
    ) -> MutableMapping[str, Any]:
        ...

    async def get_execution(
        self, execution_id: int
    ) -> MutableMapping[str, Any]:
        ...

    async def get_executions_by_step(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    async def get_input_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    async def get_output_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    async def get_port(
        self, port_id: int
    ) -> MutableMapping[str, Any]:
        ...

    async def get_port_tokens(
        self, port_id: int
    ) -> MutableSequence[int]:
        ...

    async def get_reports(
        self, workflow: str, last_only: bool = False
    ) -> MutableSequence[MutableSequence[MutableMapping[str, Any]]]:
        ...

    async def get_step(
        self, step_id: int
    ) -> MutableMapping[str, Any]:
        ...

    async def get_target(
        self, target_id: int
    ) -> MutableMapping[str, Any]:
        ...

    async def get_token(
        self, token_id: int
    ) -> MutableMapping[str, Any]:
        ...

    async def get_workflow(
        self, workflow_id: int
    ) -> MutableMapping[str, Any]:
        ...

    async def get_workflow_ports(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    async def get_workflow_steps(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    async def get_workflows_by_name(
        self, workflow_name: str, last_only: bool = False
    ) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    async def get_workflows_list(
        self, name: str | None
    ) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    async def update_deployment(
        self, deployment_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        ...

    async def update_execution(
        self, execution_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        ...

    async def update_port(
        self, port_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        ...

    async def update_step(
        self, step_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        ...

    async def update_target(
        self, target_id: str, updates: MutableMapping[str, Any]
    ) -> int:
        ...

    async def update_workflow(
        self, workflow_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        ...

There are three families of methods in the ``Database`` interface: ``add_entity``, ``update_entity``, and ``get_data``. All these methods are generic to avoid changing the interface whenever the internals of an entity slightly change.

Each ``add_entity`` method receives in input the parameter values for each entity attribute and returns the numeric ``persistent_id`` of the created entity. Some methods also accept a ``type`` field, which identifies a particular class of entities, and a ``params`` field, a dictionary of additional entity parameters. Combined, these two features allow reusing the same method (and, optionally, the same database record structure) to store a whole hierarchy of entities inheriting from a base class.

Each ``update_entity`` method receives in input the ``persistent_id`` of the entity that should be modified and a dictionary, called ``updates``, with the names of fields to be updated as keys and their new contents as values. All of them return the numeric ``persistent_id`` of the updated entity.

Each ``get_data`` method receives in input the identifier (commonly the ``persistent_id``) of an entity and returns all the data related to that entity. Some methods also accept a boolean ``last_only`` parameter, which states if all the entities should be returned or just the most recent. All ``get_data`` methods return generic data structures, i.e., lists or dictionaries. The shape of each dictionary varies from one method to another and is documented in the source code.

The ``close`` method receives no input parameter and does not return anything. It frees stateful resources potentially allocated during the object’s lifetime, e.g., network or database connections.


Implementations
---------------

======     ============================================
Type       Class
======     ============================================
sqlite     streamflow.persistence.sqlite.SqliteDatabase
======     ============================================

By default, StreamFlow uses a local ``SqliteDatabase`` instance for metadata persistence. The ``connection`` directive can be set to ``:memory:`` to avoid disk I/O and improve performance. However, in this case, all the metadata will be erased when the workflow execution terminates.

.. jsonschema:: ../../../streamflow/persistence/schemas/sqlite.json

The database schema is structured as follows:

.. literalinclude:: ../../../streamflow/persistence/schemas/sqlite.sql
    :language: sql


DatabaseLoadingContext
======================
Workflow loading is a delicate operation. If not managed properly, it can be costly in terms of time and memory and lead to deadlocks in case of circular references.
The ``DatabaseLoadingContext`` interface allows to define classes in charge of managing these aspects. Users should always rely on these classes to load entities, instead of directly calling ``load`` methods from ``PersistableEntity`` instances.

.. code-block:: python

    def add_deployment(self, persistent_id: int, deployment: DeploymentConfig):
        ...

    def add_filter(self, persistent_id: int, filter_config: FilterConfig):
        ...

    def add_port(self, persistent_id: int, port: Port):
        ...

    def add_step(self, persistent_id: int, step: Step):
        ...

    def add_target(self, persistent_id: int, target: Target):
        ...

    def add_token(self, persistent_id: int, token: Token):
        ...

    def add_workflow(self, persistent_id: int, workflow: Workflow):
        ...

    async def load_deployment(self, context: StreamFlowContext, persistent_id: int):
        ...

    async def load_filter(self, context: StreamFlowContext, persistent_id: int):
        ...

    async def load_port(self, context: StreamFlowContext, persistent_id: int):
        ...

    async def load_step(self, context: StreamFlowContext, persistent_id: int):
        ...

    async def load_target(self, context: StreamFlowContext, persistent_id: int):
        ...

    async def load_token(self, context: StreamFlowContext, persistent_id: int):
        ...

    async def load_workflow(self, context: StreamFlowContext, persistent_id: int):
        ...


Implementations
---------------

====================================================================    =============================================================
Name                                                                    Class
====================================================================    =============================================================
:ref:`DefaultDatabaseLoadingContext <DefaultDatabaseLoadingContext>`    streamflow.persistent.loading_context.DefaultDatabaseLoadingContext
:ref:`WorkflowBuilder <WorkflowBuilder>`                                  streamflow.persistent.loading_context.WorkflowBuilder
====================================================================    =============================================================

DefaultDatabaseLoadingContext
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``DefaultDatabaseLoadingContext`` keeps track of all the objects already loaded in the current transaction, serving as a cache to efficiently load nested entities and prevent deadlocks when dealing with circular references.
Furthermore, it is in charge of assigning the ``persistent_id`` when an entity is added to the cache through an ``add_*`` method.

WorkflowBuilder
^^^^^^^^^^^^^^^
The ``WorkflowBuilder`` class loads the steps and ports of an existing workflow from a ``Database`` and inserts them into a new workflow object received as a constructor argument. It extends the ``DefaultDatabaseLoadingContext`` class and overrides only the methods involving ``step``, ``port``, and ``workflow`` entities. In particular, the ``add_*`` methods of these entities must not set the ``persistent_id``, as they are dealing with a newly-created workflow, and the ``load_*`` methods should reset the internal state of their entities to the initial value (e.g., reset the status to `Status.WAITING` and clear the `terminated` flag).

The ``load_workflow`` method must behave in two different ways, depending on whether it is called directly from a user or in the internal logic of another entity's ``load`` method. In the first case, it should load all the entities related to the original workflow, identified by the ``persistent_id`` argument, into the new one. In the latter case it should simply return the new workflow entity being built.

Other entities, such as ``deployment`` and ``target`` objects, can be safely shared between the old and the new workflows, as their internal state does not need to be modified. Therefore, they can be loaded following the common path implemented in the ``DefaultDatabaseLoadingContext`` class.

