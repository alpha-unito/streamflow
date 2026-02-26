====================
No Container Translator
====================

.. meta::
   :keywords: StreamFlow, CWL, DockerRequirement, local, no container
   :description: No-container translator for CWL DockerRequirement in StreamFlow

Overview
========

The **No Container Translator** (``none``) bypasses CWL ``DockerRequirement`` specifications and runs workflow steps directly on the execution environment **without containers**. The local connector is used by default unless the step is explicitly bound to a different deployment.

.. warning::

   **Use with Caution!**
   
   This translator skips containerization entirely. You must manually ensure:
   
   * All required software is installed
   * Correct versions are available
   * Environment is properly configured
   * Dependencies are satisfied
   
   Step execution **will fail** if requirements are not met.

**Use Cases:**

* Testing workflows in pre-configured environments
* Debugging container issues
* Legacy systems without container support
* Environments where containers are prohibited

**When NOT to Use:**

* Production workflows (use proper containers)
* Workflows with complex dependencies
* Multi-user/multi-tenant systems
* Reproducibility-critical workflows

Configuration
=============

The no-container translator has no configurable options. Simply specify ``type: none``:

.. code-block:: yaml

   workflows:
     my-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /
             deployment:
               type: none

Examples
========

Skip All Containerization
--------------------------

Run entire workflow without containers:

.. code-block:: yaml
   :caption: streamflow.yml - No containers

   version: v1.0
   workflows:
     local-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /
             deployment:
               type: none

**Effect:**

All CWL ``DockerRequirement`` specifications are ignored. Steps run directly on the local machine.

Skip Specific Steps
-------------------

Use containers for some steps, skip for others:

.. code-block:: yaml
   :caption: Mixed containerization

   workflows:
     mixed-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
         docker:
           - step: /preprocess
             deployment:
               type: docker
               config:
                 image: python:3.10
           - step: /analyze
             deployment:
               type: none  # Run directly (no container)
           - step: /visualize
             deployment:
               type: docker
               config:
                 image: r-base:latest

Testing Environment
-------------------

Test workflow without containers:

.. code-block:: yaml
   :caption: Development/testing configuration

   version: v1.0
   workflows:
     test-workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: test-inputs.yml
         docker:
           - step: /
             deployment:
               type: none

**Prerequisites:**

You must manually install all workflow requirements:

.. code-block:: bash

   # Install Python dependencies
   pip install numpy pandas scikit-learn
   
   # Install system tools
   apt-get install samtools bcftools
   
   # Verify installations
   python --version
   samtools --version

Requirements
============

When using ``type: none``, you are responsible for:

1. **Software Installation**
   
   All tools referenced in CWL ``baseCommand`` must be installed and in ``$PATH``.

2. **Correct Versions**
   
   Installed versions must match workflow expectations.

3. **Dependencies**
   
   All runtime dependencies (libraries, system packages) must be present.

4. **Environment Configuration**
   
   Environment variables, paths, and configuration files must be set correctly.

5. **Permissions**
   
   File system permissions must allow workflow execution.

Example Checklist
-----------------

Before using ``type: none``, verify:

.. code-block:: bash

   # Check required tools are installed
   which python
   which samtools
   which bcftools
   
   # Check versions
   python --version   # Should match workflow requirements
   samtools --version
   
   # Check Python packages
   pip list | grep numpy
   pip list | grep pandas
   
   # Test execution
   python --help
   samtools --help

Advantages & Disadvantages
==========================

**Advantages:**

* No container overhead - Faster startup
* Direct execution - Simpler debugging
* Flexibility - Use system-specific optimizations
* Legacy compatibility - Works without container runtime

**Disadvantages:**

* No isolation - Execution can affect system
* Not reproducible - Environment-dependent
* Manual setup - Requires pre-configuration
* Error-prone - Missing dependencies cause failures
* Not portable - Won't work on other systems

Best Practices
==============

1. **Document Requirements**
   
   Maintain a list of required software and versions.

2. **Use for Testing Only**
   
   Prefer containers for production workflows.

3. **Automate Setup**
   
   Create scripts to install/verify requirements:
   
   .. code-block:: bash
   
      #!/bin/bash
      # setup-environment.sh
      pip install -r requirements.txt
      apt-get install -y samtools bcftools
      
      # Verify installations
      python -c "import numpy, pandas, sklearn"
      samtools --version

4. **Version Lock**
   
   Pin exact versions to match production:
   
   .. code-block:: text
      :caption: requirements.txt
   
      numpy==1.24.3
      pandas==2.0.2
      scikit-learn==1.3.0

5. **Test Before Production**
   
   Always test with ``type: none`` before deploying without containers.

Troubleshooting
===============

**Command Not Found:**

.. code-block:: text

   Error: command not found: python

**Solution:** Install the missing command:

.. code-block:: bash

   apt-get install python3
   ln -s /usr/bin/python3 /usr/bin/python

**Module Not Found:**

.. code-block:: text

   ModuleNotFoundError: No module named 'numpy'

**Solution:** Install Python packages:

.. code-block:: bash

   pip install numpy pandas

**Version Mismatch:**

Workflow expects Python 3.10 but system has Python 3.8.

**Solution:** Install correct version or use containers instead.

Related Documentation
=====================

**CWL Docker Translators:**
   - :doc:`index` - CWL Docker Translators overview
   - :doc:`docker` - Docker translator (recommended)
   - :doc:`singularity` - Singularity translator (HPC)

**User Guide:**
   - :doc:`/user-guide/writing-workflows` - Writing CWL workflows
   - :doc:`/user-guide/troubleshooting` - Troubleshooting guide

**Connectors:**
   - :doc:`/reference/connectors/docker` - Use Docker instead
   - :doc:`/reference/connectors/singularity` - Use Singularity instead
