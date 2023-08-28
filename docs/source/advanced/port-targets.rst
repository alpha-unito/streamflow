============
Port targets
============

In the default case, when a workflow receives files or folders as initial input objects, StreamFlow looks for them in the local file system. Along the same line, whenever a workflow step produces input files or folders, StreamFlow searches them in the location where the step was executed.

However, there are cases in which these assumptions are not valid. To correctly handle these cases, the user can specify port targets in the ``bindings`` list of a workflow. Port targets are similar to step targets described :ref:`here <Binding steps and deployments>`, but bind ports instead of steps.

In particular, a port binding contains a ``port`` directive referring to a specific input/output port in the workflow, a ``target`` directive referring to a deployment entry in the ``deployments`` section of the StreamFlow file, and a (mandatory) ``workdir`` entry identifies the base path where the data should be placed.

Similarly to steps, ports are uniquely identified using a Posix-like path, where the port is mapped to a file, and the related step is mapped to a folder. Consider the following example, which refers to :ref:`this <Write your workflow>` workflow:

.. code-block:: yaml

   version: v1.0
   workflows:
     extract-and-compile:
       type: cwl
       config:
         file: main.cwl
         settings: config.yml
       bindings:
         - step: /compile/src
           target:
             deployment: hpc-slurm
             workdir: /archive/home/myuser

   deployments:
     hpc-slurm:
       type: slurm
       config:
         ...

Here, the ``/compile/src`` path refers to the ``src`` port of the ``/compile`` step. StreamFlow will search for the file the ``src`` port requires directly on the remote ``hpc-slurm`` location in the ``/archive/home/myuser`` path.

