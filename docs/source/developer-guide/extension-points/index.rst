================
Extension Points
================

.. meta::
   :keywords: StreamFlow, plugins, extensions, connector, scheduler, database
   :description: Guide to extending StreamFlow through plugins and custom implementations

Overview
========

StreamFlow provides multiple extension points that allow you to customize and extend its functionality. This section documents all available extension points and provides guidance for creating custom plugins.

Quick Reference
===============

==============  =========================================
Audience        Plugin developers
Purpose         Create custom StreamFlow extensions
Time            4-6 hours for complete guide
Difficulty      Advanced
Prerequisites   Python 3.10+, async programming
==============  =========================================

Available Extension Points
==========================

.. toctree::
   :maxdepth: 2
   :titlesonly:

   creating-plugins
   connector
   binding-filter
   cwl-docker-translator
   scheduler
   database
   data-manager
   deployment-manager
   fault-tolerance

Extension Point Overview
========================

========================  ============================================  ==============
Extension Point           Purpose                                       Difficulty
========================  ============================================  ==============
**Connector**             Support new execution environments            Intermediate
**Scheduler**             Custom task scheduling policies               Advanced
**Database**              Alternative persistence backends              Intermediate
**DataManager**           Custom data transfer strategies               Advanced
**BindingFilter**         Custom binding selection logic                Intermediate
**CWLDockerTranslator**   Custom CWL DockerRequirement handling         Advanced
**DeploymentManager**     Alternative deployment lifecycle management   Advanced
**CheckpointManager**     Custom checkpointing strategies               Advanced
**FailureManager**        Custom failure handling policies              Advanced
========================  ============================================  ==============

Getting Started
===============

**New to Plugin Development?**

Start with the step-by-step tutorial:

:doc:`creating-plugins` - Complete guide to creating your first plugin

**Choose Your Extension Point:**

* **Want to support a new execution environment?** → :doc:`connector`
* **Need custom scheduling logic?** → :doc:`scheduler`
* **Want different data transfer behavior?** → :doc:`data-manager`
* **Need custom persistence?** → :doc:`database`
* **Want to customize binding selection?** → :doc:`binding-filter`

Plugin Development Workflow
============================

1. **Choose Extension Point:** Identify which interface to implement
2. **Study the Interface:** Read the documentation and existing implementations
3. **Implement the Plugin:** Create your plugin class implementing the interface
4. **Register the Plugin:** Use StreamFlow's plugin registration mechanism
5. **Test Locally:** Verify your plugin works as expected
6. **Package & Distribute:** Share your plugin with others (optional)

Example: Connector Plugin Structure
====================================

.. code-block:: python

   from streamflow.core.deployment import Connector
   from streamflow.core.context import StreamFlowContext
   
   class MyConnector(Connector):
       def __init__(
           self,
           deployment_name: str,
           config_dir: str,
           # ... your config parameters
       ) -> None:
           super().__init__(deployment_name, config_dir)
           # Initialize your connector
       
       async def deploy(self, external: bool) -> None:
           # Implement deployment logic
           pass
       
       async def run(
           self,
           location: ExecutionLocation,
           command: MutableSequence[str],
           # ... other parameters
       ) -> tuple[Any, int]:
           # Implement command execution
           pass
       
       # Implement other required methods...

Built-in Implementations
========================

StreamFlow includes several built-in implementations you can reference:

**Connectors:**
   - Docker, DockerCompose, Kubernetes, Helm3
   - SSH, Slurm, PBS, Flux
   - Singularity, OccAM
   
   See :doc:`/reference/index`

**Schedulers:**
   - DataLocalityPolicy (default)
   
   See :doc:`scheduler`

**Databases:**
   - SQLite (default)
   
   See :doc:`database`

**Binding Filters:**
   - ShuffleBindingFilter, MatchBindingFilter
   
   See :doc:`binding-filter`

Related Documentation
=====================

**Core Interfaces:**
   For detailed interface documentation:
   
   - :doc:`/developer-guide/core-interfaces/index` - Core abstractions

**Architecture:**
   For understanding the plugin system:
   
   - :doc:`/developer-guide/architecture/plugin-architecture` - Plugin mechanism

**Reference:**
   For configuration schemas:
   
   - :doc:`/reference/configuration/index` - Plugin configuration

Contributing Plugins
====================

If you've created a useful plugin, consider contributing it:

* **To StreamFlow Core:** Open a pull request with your connector
* **As Separate Package:** Publish to PyPI as ``streamflow-plugin-{name}``
* **Examples/Documentation:** Share in GitHub discussions

See :doc:`/developer-guide/contributing` for contribution guidelines.

Next Steps
==========

Start building your plugin:

1. :doc:`creating-plugins` - Step-by-step tutorial
2. Choose your extension point from the list above
3. Study :doc:`/developer-guide/core-interfaces/index` for interface details
4. Review :doc:`/developer-guide/code-style` for coding standards
