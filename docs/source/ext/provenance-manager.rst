=================
ProvenanceManager
=================

The ``ProvenanceManager`` interface is in charge of generating provenance archives from the metadata of a workflow execution. The ``streamflow prov`` CLI subcommand generates such archives for executed workflows (see :ref:`Collect provenance data <Collect provenance data>`).

.. code-block:: python

    @abstractmethod
    async def create_archive(
        self,
        outdir: str,
        filename: str | None,
        config: str | None,
        additional_files: MutableSequence[MutableMapping[str, str]] | None,
        additional_properties: MutableSequence[MutableMapping[str, str]] | None,
    ) -> None:
        ...

    @classmethod
    @abstractmethod
    def supports(cls, wf_type: str) -> bool:
        ...

The ``create_archive`` method generates the provenance archive and save it on disk, receiving in input the ``outdir`` folder where the archive must be created and the archive ``filename``, this latter can be ``None`` to use the implementation default filename (typically the workflow name). The ``config`` parameter is the path of the StreamFlow file used for the execution, or ``None`` when the file does not exist. The ``additional_files`` parameter is a list of key-value mappings describing external files to be included in the archive, while ``additional_properties`` is a list of key-value mappings describing additional properties to be injected into the archive manifest, when supported.

The ``supports`` class method checks if the provenance manager supports the workflow type for the archive creation.

Implementations
===============

==============    ============================================================
Type             Class
==============    ============================================================
cwl_run_crate     streamflow.provenance.run_crate.CWLRunCrateProvenanceManager
==============    ============================================================

The ``cwl_run_crate`` implementation generates a `Workflow Run RO-Crate <https://www.researchobject.org/workflow-run-crate/>`_ archive, an `RO-Crate <https://www.researchobject.org/ro-crate/>`_ profile for capturing the provenance of a computational workflow execution.
