===========
DataManager
===========

The ``DataManager`` interface performs data transfers to and from remote execution locations and, for each file, keeps track of its replicas across the distributed environment. It is defined in the ``streamflow.core.data`` module and exposes several public methods:

.. code-block:: python

    async def close(self) -> None:
        ...

    def get_data_locations(
        self,
        path: str,
        deployment: str | None = None,
        location: str | None = None,
        location_type: DataType | None = None,
    ) -> MutableSequence[DataLocation]:
        ...

    def get_source_location(
        self, path: str, dst_deployment: str
    ) -> DataLocation | None:
        ...

    def invalidate_location(
        self, location: Location, path: str
    ) -> None:
        ...

    def register_path(
        self,
        location: Location,
        path: str,
        relpath: str,
        data_type: DataType = DataType.PRIMARY,
    ) -> DataLocation:
        ...

    def register_relation(
        self, src_location: DataLocation, dst_location: DataLocation
    ) -> None:
        ...

    async def transfer_data(
        self,
        src_location: Location,
        src_path: str,
        dst_locations: MutableSequence[Location],
        dst_path: str,
        writable: bool = False,
    ) -> None:
        ...

The ``transfer_data`` method performs a data transfer from one source location to a set of target locations, called ``src_location`` and ``dst_locations``. The ``src_path`` parameter identifies the position of the data in the source file system, while ``dst_path`` specifies where the data must be transferred in the destination file systems. Note that the destination path is always the same in all destination locations. The ``writable`` parameter states that the data will be modified in place in the destination location. This parameter prevents unattended side effects (e.g., symlink optimizations when source and destination locations are equal).

The ``register_path`` method informs the ``DataManager`` about relevant data in a ``location`` file system at a specific ``path``. Sometimes, a file or directory is identified by a relative path, which filters out implementation-specific file system structures (e.g., the job-specific input directory). The ``relpath`` parameter contains the relevant portion of a path. The ``data_type`` parameter specifies the nature of the registered path. The available ``DataType`` identifiers are: ``PRIMARY`` for actual data; ``SYMBOLIC_LINK`` for links pointing to primary locations; ``INVALID``, which marks a ``DataLocation`` object as unavailable for future usage.

The ``register_relation`` method informs the ``DataManager`` that two distinct locations ``src_location`` and ``dst_location`` point to the same data. In other words, if the related data are needed, they can be collected interchangeably from one of the two locations.

The ``invalidate_location`` method informs the ``DataManager`` that the data registered in a ``location`` file system at a specific ``path`` are not available anymore, e.g., due to file system corruption or a failed data transfer. In practice, the ``DataType`` of the identified location is marked as ``INVALID``.

The ``get_data_locations`` method retrieves all the valid  ``DataLocation`` objects related to the ``path`` received in input. Plus, the set of locations can be further filtered by the ``deployment`` to which the location belongs, the name of the location on which the data object resides (``location_name``), or a given ``data_type``. Note that all the ``DataLocation`` objects that are marked ``INVALID`` should not be returned by this method.

The ``get_source_location`` method receives in input a ``path`` and the name of the destination deployment ``dst_deployment``, and it returns the ``DataLocation`` object that is most suitable to act as source location for performing the data transfer. The logic used to identify the best location is implementation-dependent. If no suitable location can be found, the method returns ``None``.

The ``close`` method receives no input parameter and does not return anything. It frees stateful resources potentially allocated during the object’s lifetime, e.g., network or database connections.

Implementations
===============

=======     ==========================================
Type        Class
=======     ==========================================
default     streamflow.data.manager.DefaultDataManager
=======     ==========================================

In the ``DefaultDataManager`` implementation, the distributed virtual file system is stored in memory in a dedicated data structure called ``RemotePathMapper``. The ``get_source_location`` method adopts the following strategy to choose the most suitable ``DataLocation`` object:

1. All the valid ``DataLocation`` objects related to the given ``path`` are retrieved by calling the ``get_data_locations`` method;
2. If there exists a ``DataLocation`` object marked as ``PRIMARY`` that resides on one of the locations belonging to the ``dst_deployment``, choose it;
3. Otherwise, if there exists a ``DataLocation`` object marked as ``PRIMARY`` that resides locally on the StreamFlow node, choose it;
4. Otherwise, if any of the retrieved ``DataLocation`` objects are marked as ``PRIMARY``, randomly choose one of them;
5. Otherwise, return ``None``.