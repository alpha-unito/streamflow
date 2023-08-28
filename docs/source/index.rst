==========
StreamFlow
==========

The StreamFlow framework is a container-native Workflow Management System written in Python 3 and based on the `Common Workflow Language <https://www.commonwl.org/>`_ (CWL) standard.

It has been designed around two main principles:

1. Allowing the execution of tasks in **multi-container environments**, in order to support concurrent execution of multiple communicating tasks in a multi-agent ecosystem.
2. Relaxing the requirement of a single shared data space, in order to allow for **hybrid workflow** executions on top of multi-cloud or hybrid cloud/HPC infrastructures.

StreamFlow source code is available on :repo:`GitHub <.>` under the LGPLv3 license. If you want to cite StreamFlow, please refer to this article:

.. code-block:: text

    I. Colonnelli, B. Cantalupo, I. Merelli and M. Aldinucci,
    "StreamFlow: cross-breeding cloud with HPC,"
    in IEEE Transactions on Emerging Topics in Computing, vol. 9, iss. 4, p. 1723-1737, 2021.
    doi: 10.1109/TETC.2020.3019202.

For LaTeX users, the following BibTeX entry can be used:

.. code-block:: bibtex

    @article{StreamFlow,
        author  = {Iacopo Colonnelli and Barbara Cantalupo and Ivan Merelli and Marco Aldinucci},
        doi     = {10.1109/TETC.2020.3019202},
        journal = {{IEEE} {T}ransactions on {E}merging {T}opics in {C}omputing},
        title   = {{StreamFlow}: cross-breeding cloud with {HPC}},
        url     = {https://doi.org/10.1109/TETC.2020.3019202},
        volume  = {9},
        number  = {4},
        pages   = {1723-1737},
        year    = {2021}
    }

.. toctree::
   :caption: Getting Started
   :hidden:

   guide/install.rst
   guide/architecture.rst
   guide/cwl.rst
   guide/deployments.rst
   guide/bind.rst
   guide/run.rst
   guide/inspect.rst

.. toctree::
   :caption: Advanced Features
   :hidden:

   advanced/multiple-targets.rst
   advanced/port-targets.rst
   advanced/stacked-locations.rst

.. toctree::
   :caption: CWL Standard
   :hidden:

   cwl/cwl-conformance.rst
   cwl/cwl-runner.rst
   cwl/docker-requirement.rst

.. toctree::
   :caption: Extension Points
   :hidden:

   ext/plugins.rst
   ext/binding-filter.rst
   ext/cwl-docker-translator.rst
   ext/connector.rst
   ext/data-manager.rst
   ext/database.rst
   ext/deployment-manager.rst
   ext/fault-tolerance.rst
   ext/scheduling.rst

.. toctree::
   :caption: Connectors
   :hidden:

   connector/docker.rst
   connector/docker-compose.rst
   connector/flux.rst
   connector/helm3.rst
   connector/kubernetes.rst
   connector/occam.rst
   connector/pbs.rst
   connector/queue-manager.rst
   connector/singularity.rst
   connector/slurm.rst
   connector/ssh.rst

.. toctree::
   :caption: CWL Docker Translators
   :hidden:

   cwl/docker/docker.rst
   cwl/docker/kubernetes.rst
   cwl/docker/singularity.rst