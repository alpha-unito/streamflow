=========
Connector
=========

StreamFlow demands the lifecycle management of each execution environment to a specific implementation of the ``Connector`` interface. In particular, a single ``Connector`` object is created for each ``deployment`` object described in the :ref:`StreamFlow file <Put it all together>`.

The ``streamflow.core.deployment`` module defines the ``Connector`` interface, which exposes the following public methods:

.. code-block:: python

    async def copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        read_only: bool = False,
    ) -> None:
        ...

    async def copy_remote_to_local(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        read_only: bool = False,
    ) -> None:
        ...

    async def copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        source_location: Location,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ) -> None:
        ...

    async def deploy(
        self, external: bool
    ) -> None:
        ...

    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        ...

    async def get_stream_reader(
        self, location: Location, src: str
    ) -> StreamWrapperContextManager:
        ...

    async def run(
        self,
        location: Location,
        command: MutableSequence[str],
        environment: MutableMapping[str, str] = None,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        capture_output: bool = False,
        timeout: int | None = None,
        job_name: str | None = None,
    ) -> tuple[Any | None, int] | None:
        ...

    async def undeploy(
        self, external: bool
    ) -> None:
        ...

The ``deploy`` method instantiates the remote execution environment, making it ready to receive requests for data transfers and command executions. A ``deployment`` object can be marked as ``external`` in the StreamFlow file. In that case, the ``Connector`` should assume that the execution environment is already up and running, and the ``deploy`` method should only open the necessary connections to communicate with it.

The ``undeploy`` method destroys the remote execution environment, potentially cleaning up all the temporary resources instantiated during the workflow execution (e.g., intermediate results). If a ``deployment`` object is marked as ``external``, the ``undeploy`` method should not destroy it but just close all the connections opened by the ``deploy`` method.

The ``get_available_locations`` method is used in the scheduling phase to obtain the locations available for job execution, identified by their unique name (see :ref:`here <Scheduling>`). The method receives some optional input parameters to filter valid locations. The ``service`` parameter specifies a specific set of locations in a deployment, and its precise meaning differs for each deployment type (see :ref:`here <Binding steps and deployments>`). The other three parameters (``input_directory``, ``output_directory``, and ``tmp_directory``) allow the ``Connector`` to return correct disk usage values for each of the three folders in case of remote instances with multiple volumes attached.

The ``get_stream_reader`` method returns a ``StreamWrapperContextManager`` instance, which allows the ``src`` data on the ``location`` to be read using a stream (see :ref:`here <Streaming>`). The stream must be read respecting the size of the available buffer, which is defined by the ``transferBufferSize`` attribute of the ``Connector`` instance. This method improve performance of data copies between pairs of remote locations.

The ``copy`` methods perform a data transfer from a ``src`` path to a ``dst`` path in one or more destination ``locations`` in the execution environment controlled by the ``Connector``. The ``read_only`` parameter notifies the ``Connector`` if the destination files will be modified in place or not. This parameter prevents unattended side effects (e.g., symlink optimizations on the remote locations). The ``copy_remote_to_remote`` method accepts two additional parameters: a ``source_location`` and an optional ``source_connector``. The latter identifies the ``Connector`` instance that controls the ``source_location`` and defaults to ``self`` when not specified.

The ``run`` method performs a remote ``command`` execution on a remote ``location``. The ``command`` parameter is a list of arguments, mimicking the Python `subprocess <https://docs.python.org/3/library/subprocess.html>`_ abstraction. Many optional parameters can be passed to the ``run`` method. The ``environment`` parameter is a dictionary of environment variables, which should be defined in the remote execution context before executing the command. The ``workdir`` parameter identifies the remote working directory. The ``stdin``, ``stdout``, and ``stderr`` parameters are used for remote stream redirection. The ``capture_output`` parameter specifies if the command output should be retrieved or not. If ``capture_output`` is set to ``True``, the ``run`` method returns the command output and return code, while it does not return anything if ``capture_output`` is set to ``False``. The ``timeout`` parameter specifies a maximum completion time for the remote execution, after which the ``run`` method throws a ``WorkflowExecutionException``. Finally, the ``job_name`` parameter is the unique identifier of a StreamFlow job, which is used for debugging purposes.

BaseConnector
=============

Users who want to implement their own ``Connector`` class should extend from the ``BaseConnector`` whenever possible. The StreamFlow ``BaseConnector`` implementation, defined in the ``streamflow.deployment.connector.base`` module, already provides some essential support for logging and tar-based streaming data transfers. Plus, it correctly handles :ref:`FutureConnector <FutureConnector>` instances by extending the ``FutureAware`` base class. However, the ``BaseConnector`` does not allow wrapping inner connectors using the ``wraps`` directive (see :ref:`here <Stacked locations>`). Indeed, only connectors extending the :ref:`ConnectorWrapper <ConnectorWrapper>` interface support the ``wraps`` directive.

LocalConnector
==============

The ``LocalConnector`` class is a special subtype of the ``Connector`` instance that identifies the StreamFlow local node. As discussed above, data transfers that involve the local node are treated differently from remote-to-remote data movements. In general, several StreamFlow classes adopt different strategies when an action involves the local node or a remote one, and these decisions involve verifying if a ``Connector`` object extends the ``LocalConnector`` class. For this reason, users who want to provide their version of a local ``Connector`` must extend the ``LocalConnector`` class and not the ``BaseConnector`` as in other cases.

FutureConnector
===============

In the ``eager`` setting, all the ``Connector`` objects deploy their related execution environment at the beginning of a workflow execution. However, to save resources, it is sometimes desirable to adopt a ``lazy`` approach, deploying each execution environment only when it receives the first request from the StreamFlow control plane. Users can switch between these behaviours by setting the ``lazy`` attribute of each ``target`` object  to ``True`` (the default) or ``False`` in the StreamFlow file.

A ``FutureConnector`` instance wraps an actual ``Connector`` instance and implements the ``lazy`` behaviour: the ``deploy`` method does nothing, and each other method calls the ``deploy`` method on the inner ``Connector`` to initialize it and delegate the action. The main drawback of this implementation is that the type checking on a ``FutureConnector`` instance will return the wrong connector type. A ``FutureAware`` class solves this issue by transparently returning the type of the inner ``Connector``. All custom ``Connector`` instances defined by the users should extend the ``FutureAware`` class directly or indirectly by extending the :ref:`BaseConnector <BaseConnector>` or :ref:`ConnectorWrapper <ConnectorWrapper>` classes.

ConnectorWrapper
================

StreamFlow supports :ref:`stacked locations <Stacked locations>` using the ``wraps`` directive. However, not all ``Connector`` instances support inner connectors, but only those that extend the ``ConenctorWrapper`` interface. By default, a ``ConnectorWrapper`` instance receives an internal ``Connector`` object as a constructor parameter and delegates all the method calls to the wrapped ``Connector``. Plus, it already extends the ``FutureAware`` class, correctly handling :ref:`FutureConnector <FutureConnector>` instances. Users who want to create a custom ``Connector`` instance with support for the ``wraps`` directive must extend the ``ConnectorWrapper`` class and not the ``BaseConnector`` as in other cases.

Streaming
=========

StreamFlow uses ``tar`` streams as the primary way to transfer data between locations. The main reason is that the ``tar`` command is so standard nowadays that it can be found OOTB in almost all execution environments, and its API does not vary significantly across implementations.

To ensure compatibility between different ``Connector`` instances when performing data transfers, StreamFlow implements two interfaces: a ``StreamWrapper`` API to read and write data streams and a ``get_stream_reader`` method to obtain a ``StreamWrapper`` object from a ``Connector`` instance.

The ``StreamWrapper`` interface is straightforward. It is reported below:

.. code-block:: python

    def __init__(self, stream: Any):
        self.stream: Any = stream

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    async def read(self, size: int | None = None):
        ...

    @abstractmethod
    async def write(self, data: Any):
        ...

The constructor receives an internal ``stream`` object, which can be of ``Any`` type. The ``read``, ``write``, and ``close`` methods wrap the APIs of the native ``stream`` object to provide a unified API to interact with streams. In particular, the ``read`` method reads up to ``size`` bytes from the internal ``stream``. The ``write`` method writes the content of the ``data`` parameter into the internal ``stream``. The ``close`` method closes the inner ``stream``.

Each ``Connector`` instance can implement its own ``StreamWrapper`` classes by extending the ``BaseStreamWrapper`` class. In particular, it can be helpful to specialize further the ``StreamWrapper`` interface to implement unidirectional streams. This can be achieved by extending the ``StreamReaderWrapper`` and ``StreamWriterWrapper`` base classes, which raise a ``NotImplementedError`` if the stream is used in the wrong direction.

The ``StreamWrapperContextManager`` interface provides the `Asynchronous Context Manager <https://docs.python.org/3/reference/datamodel.html#async-context-managers>`_ primitives for the ``StreamWrapper`` object, allowing it to be used inside ``async with`` statements.



Implementations
===============

=======================================================     ================================================================
Name                                                        Class
=======================================================     ================================================================
:ref:`docker <DockerConnector>`                             streamflow.deployment.connector.docker.DockerConnector
:ref:`docker-compose <DockerComposeConnector>`              streamflow.deployment.connector.docker.DockerComposeConnector
:ref:`flux <FluxConnector>`                                 streamflow.deployment.connector.queue_manager.FluxConnector
:ref:`helm <Helm3Connector>`                                streamflow.deployment.connector.kubernetes.Helm3Connector
:ref:`helm3 <Helm3Connector>`                               streamflow.deployment.connector.kubernetes.Helm3Connector
:ref:`kubernetes <KubernetesConnector>`                     streamflow.deployment.connector.kubernetes.KubernetesConnector
:ref:`occam <OccamConnector>`                               streamflow.deployment.connector.occam.OccamConnector
:ref:`pbs <PBSConnector>`                                   streamflow.deployment.connector.queue_manager.PBSConnector
:ref:`singularity <SingularityConnector>`                   streamflow.deployment.connector.singularity.SingularityConnector
:ref:`slurm <SlurmConnector>`                               streamflow.deployment.connector.queue_manager.SlurmConnector
:ref:`ssh <SSHConnector>`                                   streamflow.deployment.connector.ssh.SSHConnector
=======================================================     ================================================================