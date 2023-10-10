===================
CWLDockerTranslator
===================

StreamFlow relies on a ``CWLDockerTranslator`` object to convert a CWL `DockerRequirement <https://www.commonwl.org/v1.2/CommandLineTool.html#DockerRequirement>`_ specification into a step binding on a given :ref:`Connector <Connector>` instance. By default, the :ref:`DockerCWLDockerTranslator <DockerCWLDockerTranslator>` is used to spawn a :ref:`DockerConnector <DockerConnector>`. However, StreamFlow also supports translators for :ref:`Kubernetes <SingularityCWLDockerTranslator>` and :ref:`Singularity <KubernetesCWLDockerTranslator>`, and more can be implemented by the community using the :ref:`plugins <Plugins>` mechanism (see :ref:`here <CWL Docker Requirement>`).

The ``CWLDockerTranslator`` interface is defined in the ``streamflow.cwl.requirement.docker.translator`` module and exposes a single public method ``get_target``:

.. code-block:: python

    def get_target(
        self,
        image: str,
        output_directory: str | None,
        network_access: bool,
        target: Target,
    ) -> Target:
        ...

The ``get_target`` method returns a ``Target`` object that contains an auto-generated ``DeploymentConfig`` that reflects the ``CWLDockerTranslator`` configuration. The ``target`` parameter contains the original ``Target`` object of the related step. If the ``Connector`` created by the ``CWLDockerTranslator`` extends the :ref:`ConnectorWrapper <ConnectorWrapper>` class and the ``wrapper`` directive is defined as ``True`` in the StreamFlow file, the newly created ``Target`` object wraps the original one.

The other parameters derive from the CWL workflow specification. In particular, the ``image`` parameter points to the Docker image needed by the step. The ``output_directory`` parameter reflects the ``dockerOutputDirectory`` option of a CWL ``DockerRequirement``. The ``network_access`` parameter derives from the CWL `NetworkAccess <https://www.commonwl.org/v1.2/CommandLineTool.html#NetworkAccess>`_ requirement.

Implementations
===============

===================================================     ================================================================
Type                                                    Class
===================================================     ================================================================
:ref:`docker <DockerCWLDockerTranslator>`               streamflow.cwl.requirement.docker.DockerCWLDockerTranslator
:ref:`kubernetes <KubernetesCWLDockerTranslator>`       streamflow.cwl.requirement.docker.KubernetesCWLDockerTranslator
:ref:`singularity <SingularityCWLDockerTranslator>`     streamflow.cwl.requirement.docker.SingularityCWLDockerTranslator
===================================================     ================================================================
