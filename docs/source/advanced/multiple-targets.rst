================
Multiple targets
================

StreamFlow lets users to map steps and ports to multiple targets in the :ref:`StreamFlow file <Put it all together>`. A step bound to multiple locations can be scheduled on each of them at runtime. Plus, if a step encloses multiple instances (e.g., in a CWL ``scatter`` operation), they can run on different targets.

The `filters` directive defines one or more strategies to select a target among the set of available ones (or among a subset) at runtime. By default, all available targets will be evaluated in the order of appearance in the ``target`` directive.

Users can select a given :ref:`BindingFilter <BindingFilter>` implementation by specifying its name in the ``filters`` directive of a ``binding`` object. If multiple filters are declared, they are applied to the target list in the order of appearance. For example, to evaluate targets in random order at every allocation request, users can specify the following:

.. code-block:: yaml

   workflows:
     example:
       type: cwl
       config:
         file: main.cwl
         settings: config.yml
       bindings:
        - step: /compile
          target:
            - deployment: first-deployment
            - deployment: second-deployment
          filters:
            - shuffle

Conversely, a file or directory port bound to multiple locations can be retrieved from each of them at runtime. StreamFlow will always try to minimize the overhead of data transfers, using local data whenever possible.