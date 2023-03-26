==========
CWL Runner
==========

CWL specifies a standard ``cwl-runner`` interface to execute CWL workflows from the command line. StreamFlow adheres to this interface, installing a ``cwl-runner`` executable in the user's ``$PATH``. In particular, a

.. code-block:: bash

    cwl-runner processfile jobfile

command is equivalent to a ``streamflow run`` command with the following ``streamflow.yml`` file:

.. code-block:: yaml

   version: v1.0
   workflows:
     workflow_name:
       type: cwl
       config:
         file: processfile
         settings: jobfile

In addition to the standard parameters, it is possible to pass a ``--streamflow-file`` argument to the ``cwl-runner`` CLI with the path of a StreamFlow file containing deployments and bindings (see the :ref:`StreamFlow file <Put it all together>` section). The ``workflows`` section of this file must have a single entry containing a list of ``bindings``. If present, the ``type`` and ``config`` entries will be ignored. Files containing multiple workflow entries will throw an exception.

For example, the workflow described :ref:`here <Write your workflow>` can also be executed with the following command

.. code-block:: bash

    cwl-runner --streamflow-file /path/to/streamflow.yml main.cwl config.cwl

where the ``streamflow.yml`` fail contains these lines

.. code-block:: yaml

   version: v1.0
   workflows:
     extract-and-compile:
       bindings:
        - step: /compile
          target:
            deployment: docker-openjdk

   deployments:
     docker-openjdk:
       type: docker
       config:
         image: openjdk:9.0.1-11-slim

