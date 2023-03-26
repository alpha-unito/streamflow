==========
Scheduling
==========

StreamFlow lets user implement their scheduling infrastructure. There are two extension points related to scheduling: :ref:`Scheduler <Scheduler>` and :ref:`Policy <Policy>`. The ``Scheduler`` interface implements all the scheduling infrastructure, including data structures, to store the global current allocation status. The ``Policy`` interface implements a specific placement strategy to map jobs onto available locations. Both interfaces are specified in the ``streamflow.core.scheduling`` module.

In StreamFlow, the ``Job`` object is the allocation unit. Each workflow step generates zero or more ``Job`` objects sent to the scheduling infrastructure for placement.

.. code-block:: python

    class Job:
        def __init__(
            self,
            name: str,
            workflow_id: int,
            inputs: MutableMapping[str, Token],
            input_directory: str | None,
            output_directory: str | None,
            tmp_directory: str | None,
        ):
            ...

In practice, a ``Job`` is a data structure containing a unique ``name``, the ``workflow_id`` pointing to the workflow it belongs to, a dictionary of ``inputs`` containing the input data needed for execution, and three folder paths pointing to a potentially remote filesystem: ``input_directory``, ``output_directory``, ``tmp_directory``. Since the actual paths depend on the chosen execution location, these parameters are not specified before the scheduling phase.

Scheduler
=========

The ``Scheduler`` interface contains three abstract methods: ``schedule``, ``notify_status``, and ``close``.

.. code-block:: python

    async def schedule(
        self, job: Job, binding_config: BindingConfig, hardware_requirement: Hardware
    ) -> None:
        ...

    async def notify_status(
        self, job_name: str, status: Status
    ) -> None:
        ...

    async def close(
        self
    ) -> None:
        ...

The ``schedule`` method tries to allocate one or more available locations for a new ``Job`` object. It receives three input parameters:  a new ``Job`` object to be allocated, a ``BindingConfig`` object containing the list of potential allocation targets for the ``Job`` and a list of :ref:`BindingFilter <BindingFilter>` objects, and a ``HardwareRequirement`` object specifying the resource requirements of the ``Job``. Resource requirements are extracted automatically from the workflow specification, e.g., `CWL <https://www.commonwl.org/v1.2/CommandLineTool.html#ResourceRequirement>`_ files. Conversely, the ``BindingFilter`` object derives from the :ref:`StreamFlow file <Put it all together>`.

The ``notify_status`` method is called whenever a ``Job`` object changes its status, e.g., when it starts, completes, or fails. It receives two input parameters, the name of an existing ``Job`` and its new ``Status``, and returns nothing. When a ``Job`` reaches a final status (i.e., ``FAILED``, ``COMPLETED``, or ``CANCELLED``), its related locations are marked as available, and the ``Scheduler`` starts a new scheduling attempt.

The ``close`` method receives no input parameter and does not return anything. It frees stateful resources potentially allocated during the object's lifetime, e.g., network or database connections.

Implementations
---------------

=======     ================================================
Type        Class
=======     ================================================
default     streamflow.scheduling.scheduler.DefaultScheduler
=======     ================================================

In the ``DefaultScheduler`` implementation, scheduling attempts follow a simple First Come, First Served (FCFS) approach. The ``schedule`` method demands the allocation strategy to a ``Policy`` object specified in the StreamFlow file's ``bindings`` section through a ``target`` object's ``policy`` directive.  If no available allocation configuration can be found for a given ``Job``, it is queued until the next scheduling attempt.

As discussed above, a scheduling attempt occurs whenever a ``Job`` reaches a final state. Plus, to account for dynamic resource creation and deletion in remote execution environments (e.g., through the Kubernetes `HorizontalPodAutoscaler <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/>`_) the ``DefaultScheduler`` can automatically perform a scheduling attempt for each queued ``Job`` at regular intervals. The duration of such intervals can be configured through the ``retry_delay`` parameter. A value of ``0`` (the default) turns off this behaviour.

.. jsonschema:: ../../../streamflow/scheduling/schemas/scheduler.json

Policy
======

The ``Policy`` interface contains a single method ``get_location``, which returns the ``Location`` chosen for placement or ``None`` if there is no available location.

.. code-block:: python

    async def get_location(
        self,
        context: StreamFlowContext,
        job: Job,
        hardware_requirement: Hardware,
        available_locations: MutableMapping[str, AvailableLocation],
        jobs: MutableMapping[str, JobAllocation],
        locations: MutableMapping[str, MutableMapping[str, LocationAllocation]],
    ) -> Location | None:
        ...

The ``get_location`` method receives much information about the current execution context, enabling it to cover a broad class of potential scheduling strategies. In particular, the ``context`` parameter can query all the StreamFlow's relevant data structures, such as the :ref:`Database <Database>`, the :ref:`DataManager <DataManager>`, and the :ref:`DeploymentManager <DeploymentManager>`.

The ``Job`` parameter contains the ``Job`` object to be allocated, and the ``hardware_requirement`` parameter is a ``HardwareRequirement`` object specifying the ``Job``'s resource requirements. The ``available_locations`` parameter contains the list of locations available for placement in the target deployment. They are obtained by calling the ``get_available_locations`` method of the related :ref:`Connector <Connector>` object.

The ``jobs`` and ``locations`` parameters describe the current status of the workflow execution. The ``jobs`` parameter is a dictionary of ``JobAllocation`` objects, containing information about all the previously allocated ``Job`` objects, indexed by their unique name. Each ``JobAllocation`` structure contains the ``Job`` name, its target, the list of locations associated with the ``Job`` execution, the current ``Status`` of the ``Job``, and the hardware resources allocated for its execution on each selected location.

The ``locations`` parameter is the set of locations allocated to at least one ``Job`` in the past, indexed by their deployment and unique name. Each ``LocationAllocation`` object contains the location name, the name of its deployment, and the list of ``Job`` objects allocated to it, identified by their unique name.

Implementations
---------------

=============     =============================================================
Type              Class
=============     =============================================================
data_locality     streamflow.scheduling.policy.data_locality.DataLocalityPolicy
=============     =============================================================

The ``DataLocalityPolicy`` is the default scheduling policy in StreamFlow. The adopted strategy is the following:

1. File input tokens are sorted by weight in descending order;
2. All the locations containing the related files are retrieved from the :ref:`DataManager` for each token. If data are already present in one of the available locations, that location is chosen for placement;
3. If data-driven allocation is not possible, one location is randomly picked up from the remaining ones;
4. If there are no available locations, return ``None`` (and queue the ``Job``).
