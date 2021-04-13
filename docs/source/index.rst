==========
StreamFlow
==========

The StreamFlow framework is a container-native Workflow Management System written in Python 3 and based on the `Common Workflow Language <https://www.commonwl.org/>`_ (CWL) standard.

It has been designed around two main principles:

1. Allowing the execution of tasks in **multi-container environments**, in order to support concurrent execution of multiple communicating tasks in a multi-agent ecosystem.
2. Relaxing the requirement of a single shared data space, in order to allow for **hybrid workflow** executions on top of multi-cloud or hybrid cloud/HPC infrastructures.

StreamFlow source code is available on `GitHub <https://github.com/alpha-unito/streamflow>`_ under the LGPLv3 license. If you want to cite StreamFlow, please refer to this article:

.. code-block:: text

    I. Colonnelli, B. Cantalupo, I. Merelli and M. Aldinucci,
    "StreamFlow: cross-breeding cloud with HPC,"
    in IEEE Transactions on Emerging Topics in Computing,
    doi: 10.1109/TETC.2020.3019202.

For LaTeX users, the following BibTeX entry can be used:

.. code-block:: bibtex

    @article{StreamFlow,
        author  = {Iacopo Colonnelli and Barbara Cantalupo and Ivan Merelli and Marco Aldinucci},
        doi     = {10.1109/TETC.2020.3019202},
        journal = {{IEEE} {T}ransactions on {E}merging {T}opics in {C}omputing},
        title   = {{StreamFlow}: cross-breeding cloud with {HPC}},
        url     = {https://doi.org/10.1109/TETC.2020.3019202},
        year    = {2020}
    }

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Getting Started

   install.rst
   architecture.rst
   operations.rst

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Connectors

   connector/docker.rst
   connector/docker-compose.rst
   connector/helm2.rst
   connector/helm3.rst
   connector/occam.rst
   connector/pbs.rst
   connector/singularity.rst
   connector/slurm.rst
   connector/ssh.rst
