===============
Troubleshooting
===============

.. meta::
   :keywords: StreamFlow, troubleshooting, debugging, errors, problems, solutions
   :description: Comprehensive troubleshooting guide for common StreamFlow issues

Overview
========

This guide provides solutions to common problems encountered when using StreamFlow. Issues are organized by category for quick reference.

Quick Diagnostic Steps
======================

When encountering issues:

1. **Check logs** - Look for error messages
2. **Verify configuration** - Validate YAML syntax
3. **Test connectivity** - Ensure deployments are reachable
4. **Check resources** - Verify adequate CPU/memory/disk
5. **Isolate the problem** - Test components individually

Installation Issues
===================

Python Version Error
--------------------

**Problem:** ``ERROR: Package requires a different Python version``

**Cause:** StreamFlow requires Python 3.10 or later

**Solution:**

.. code-block:: bash

   # Check Python version
   python --version
   
   # Install with specific Python version
   python3.10 -m pip install streamflow

Command Not Found
-----------------

**Problem:** ``command 'streamflow' not found``

**Cause:** Installation directory not in PATH

**Solution:**

.. code-block:: bash

   # Find installation location
   pip show streamflow | grep Location
   
   # Add to PATH (add to ~/.bashrc or ~/.zshrc)
   export PATH="$HOME/.local/bin:$PATH"
   
   # Or use full path
   ~/.local/bin/streamflow version

Dependency Conflicts
--------------------

**Problem:** pip reports dependency conflicts

**Solution:**

.. code-block:: bash

   # Use virtual environment
   python3 -m venv streamflow-env
   source streamflow-env/bin/activate
   pip install streamflow

Configuration Issues
====================

YAML Syntax Errors
------------------

**Problem:** ``YAML parse error`` or ``Unexpected token``

**Solution:**

* Check indentation (use spaces, not tabs)
* Ensure proper YAML formatting
* Validate with online YAML validator
* Use JSON Schema for validation:

.. code-block:: bash

   streamflow schema > streamflow-schema.json
   # Configure IDE to use schema

Invalid Deployment Type
-----------------------

**Problem:** ``Unknown deployment type 'xxx'``

**Solution:**

* Check spelling of connector type
* Verify connector is available:

.. code-block:: bash

   streamflow ext list

* Ensure required plugin is installed

Missing Required Fields
-----------------------

**Problem:** ``Missing required field 'xxx'``

**Solution:**

* Check connector documentation for required fields
* Refer to :doc:`/reference/index`
* Generate schema for auto-completion

Step Not Found
--------------

**Problem:** ``Step '/xxx' not found in workflow``

**Solution:**

* Verify step name matches CWL workflow
* Check for typos in step path
* Use ``/`` for entire workflow
* For nested workflows: ``/subworkflow/step``

Deployment Issues
=================

Connection Timeout
------------------

**Problem:** ``Connection timeout`` or ``Connection refused``

**Cause:** Network connectivity or service not running

**Solution:**

.. code-block:: bash

   # Test connectivity
   ping hostname
   
   # Test SSH
   ssh user@hostname
   
   # Check Docker daemon
   docker info
   
   # Check Kubernetes
   kubectl cluster-info

SSH Authentication Failed
-------------------------

**Problem:** ``Permission denied (publickey)``

**Cause:** SSH key not authorized or incorrect

**Solution:**

.. code-block:: bash

   # Check SSH key permissions
   chmod 600 ~/.ssh/id_rsa
   chmod 644 ~/.ssh/id_rsa.pub
   
   # Test SSH connection
   ssh -i ~/.ssh/id_rsa user@hostname
   
   # Add public key to remote host
   ssh-copy-id -i ~/.ssh/id_rsa.pub user@hostname
   
   # Verify key in authorized_keys
   cat ~/.ssh/authorized_keys  # on remote host

Docker Issues
-------------

**Problem:** ``Cannot connect to Docker daemon``

**Solution:**

.. code-block:: bash

   # Start Docker daemon
   sudo systemctl start docker  # Linux
   # or start Docker Desktop (macOS)
   
   # Check Docker status
   docker info
   
   # Add user to docker group (Linux)
   sudo usermod -aG docker $USER
   # Log out and back in

**Problem:** ``ImagePullBackOff`` or ``Failed to pull image``

**Solution:**

.. code-block:: bash

   # Test image locally
   docker pull image:tag
   
   # Check image name/tag
   # Verify registry credentials if private
   docker login registry.example.com
   
   # Check network connectivity

Kubernetes Issues
-----------------

**Problem:** ``Unauthorized`` or ``Error loading kubeconfig``

**Solution:**

.. code-block:: bash

   # Verify kubeconfig
   kubectl cluster-info
   
   # Check authentication
   kubectl auth can-i get pods
   
   # Verify namespace exists
   kubectl get namespaces
   
   # Check current context
   kubectl config current-context

**Problem:** Pods stuck in ``Pending``

**Solution:**

.. code-block:: bash

   # Check pod events
   kubectl describe pod <pod-name>
   
   # Check node resources
   kubectl describe nodes
   
   # Check resource quotas
   kubectl get resourcequota
   
   # Reduce resource requests or scale cluster

Slurm/PBS Issues
----------------

**Problem:** Job stays in queue indefinitely

**Solution:**

.. code-block:: bash

   # Check job status
   squeue  # Slurm
   qstat   # PBS
   
   # Check job details
   scontrol show job <jobid>  # Slurm
   qstat -f <jobid>           # PBS
   
   # Verify partition/queue exists
   sinfo  # Slurm
   qstat -Q  # PBS
   
   # Check account/QoS limits
   sacctmgr show assoc  # Slurm

* Reduce resource requests
* Check account limits
* Verify partition is active

**Problem:** ``Invalid account`` or ``Invalid QOS``

**Solution:**

.. code-block:: bash

   # Check available accounts
   sacctmgr show assoc where user=$USER  # Slurm
   
   # Use correct account in configuration
   services:
     compute:
       account: valid-account-name

Workflow Execution Issues
==========================

Workflow Won't Start
--------------------

**Problem:** Workflow fails immediately at startup

**Diagnostic Steps:**

1. Check for configuration errors
2. Verify CWL file syntax
3. Check file paths exist
4. Review initialization logs

**Solution:**

.. code-block:: bash

   # Validate CWL
   cwltool --validate workflow.cwl
   
   # Check StreamFlow config syntax
   streamflow schema > schema.json
   # Validate against schema
   
   # Enable debug logging
   streamflow run workflow.yml --debug

Step Execution Fails
--------------------

**Problem:** Specific workflow step fails

**Diagnostic Steps:**

1. Check step logs for error messages
2. Verify command/tool is available
3. Check input files are accessible
4. Verify resource requirements

**Solution:**

.. code-block:: bash

   # Check if command exists in deployment
   # For Docker:
   docker run image:tag which command
   
   # For SSH:
   ssh user@host which command
   
   # Verify input files
   # Check file paths in error logs
   
   # Test command manually
   # Run the exact command from logs

Command Not Found
-----------------

**Problem:** ``command not found: xxx``

**Cause:** Tool not installed in execution environment

**Solution:**

* Verify tool in container image
* Install missing tools
* Use correct base image
* Check PATH in execution environment

.. code-block:: yaml

   # Ensure tool in container
   deployments:
     docker-env:
       type: docker
       config:
         image: image-with-tool:latest

File Not Found
--------------

**Problem:** ``No such file or directory: /path/to/file``

**Cause:** File not accessible in execution location

**Solution:**

* Check file exists locally
* Verify file transfer occurred
* Use port binding for remote files:

.. code-block:: yaml

   bindings:
     - port: /input_file
       target:
         deployment: remote-storage
         workdir: /data

* Check file permissions

Data Transfer Issues
--------------------

**Problem:** Large files transfer slowly or fail

**Solution:**

* Use data locality - execute where data resides
* Configure port bindings to avoid transfers
* Check network bandwidth
* Use appropriate transfer protocols

.. code-block:: yaml

   # Avoid transfer by using port binding
   bindings:
     - step: /process
       target:
         deployment: hpc-cluster
     - port: /large_dataset
       target:
         deployment: hpc-cluster
         workdir: /scratch/data

Memory Issues
-------------

**Problem:** ``Out of memory`` or ``Killed``

**Solution:**

* Increase memory limits:

.. code-block:: yaml

   # CWL hint
   hints:
     ResourceRequirement:
       ramMin: 16384  # MB
   
   # Kubernetes
   services:
     workers:
       template:
         spec:
           containers:
             - resources:
                 limits:
                   memory: "16Gi"
   
   # Slurm
   services:
     compute:
       mem: 64G

* Process data in smaller chunks
* Use streaming/incremental processing
* Scale to nodes with more memory

Disk Space Issues
-----------------

**Problem:** ``No space left on device``

**Solution:**

.. code-block:: bash

   # Check disk space
   df -h
   
   # Clean temporary files
   rm -rf /tmp/streamflow/*
   
   # Increase disk quota
   # Configure larger scratch space

* Use deployment with more disk
* Clean intermediate files
* Stream data instead of storing

Container-Specific Issues
==========================

Permission Denied in Container
------------------------------

**Problem:** ``Permission denied`` inside container

**Cause:** User ID mismatch

**Solution:**

.. code-block:: yaml

   deployments:
     docker-env:
       type: docker
       config:
         image: myimage:latest
         user: "1000:1000"  # Match host UID:GID

Volume Mount Issues
-------------------

**Problem:** Files not visible in container

**Solution:**

.. code-block:: yaml

   deployments:
     docker-volumes:
       type: docker
       config:
         image: myimage:latest
         volumes:
           - /host/path:/container/path:rw

* Verify host path exists
* Check path permissions
* Use absolute paths

Performance Issues
==================

Slow Execution
--------------

**Problem:** Workflow runs slower than expected

**Diagnostic Steps:**

1. Generate HTML report
2. Check timeline for bottlenecks
3. Identify long-running steps
4. Check for sequential execution that should be parallel

**Solutions:**

* Add parallelism with scatter:

.. code-block:: yaml

   steps:
     process:
       run: tool.cwl
       scatter: input_file
       in:
         input_file: input_files

* Improve data locality
* Use faster deployment
* Adjust resource allocation
* Optimize step commands

Excessive Idle Time
-------------------

**Problem:** Resources sit idle during execution

**Solution:**

* Increase parallelism
* Adjust scatter configuration
* Use multiple deployments
* Check scheduling efficiency

CWL Issues
==========

Invalid CWL Syntax
------------------

**Problem:** ``Invalid CWL`` or ``Parse error``

**Solution:**

.. code-block:: bash

   # Validate CWL
   cwltool --validate workflow.cwl
   
   # Check version compatibility
   # StreamFlow supports CWL v1.0, v1.1, v1.2

Output Glob No Matches
----------------------

**Problem:** ``Output glob pattern matches no files``

**Cause:** Command didn't create expected output

**Solution:**

* Verify command actually creates file
* Check output filename/pattern
* Check working directory
* Test command manually:

.. code-block:: bash

   # Run command to see what files it creates
   ls -la

JavaScript Expression Errors
-----------------------------

**Problem:** ``Invalid JavaScript expression``

**Solution:**

* Add requirement:

.. code-block:: yaml

   requirements:
     InlineJavascriptRequirement: {}

* Check JavaScript syntax
* Verify variable names (``inputs.*``, ``runtime.*``)

Secondary Files Missing
-----------------------

**Problem:** ``Secondary file not found``

**Solution:**

* Ensure secondary files exist
* Check secondaryFiles specification:

.. code-block:: yaml

   inputs:
     reference:
       type: File
       secondaryFiles:
         - .fai
         - ^.dict  # ^ means replace extension

Debugging Techniques
====================

Enable Debug Logging
--------------------

.. code-block:: bash

   streamflow run workflow.yml --debug

Debug logs show:

* Detailed connector operations
* File transfers
* Command executions
* Scheduling decisions

Test Locally First
------------------

.. code-block:: yaml

   # Test with local deployment
   deployments:
     local:
       type: local
   
   bindings:
     - step: /
       target:
         deployment: local

Isolate Problems
----------------

1. **Test deployment separately**

.. code-block:: bash

   # SSH
   ssh user@hostname
   
   # Docker
   docker run -it image:tag /bin/bash
   
   # Kubernetes
   kubectl run test --image=image:tag -it -- /bin/bash

2. **Test CWL workflow separately**

.. code-block:: bash

   cwltool workflow.cwl inputs.yml

3. **Test single step**

.. code-block:: yaml

   bindings:
     - step: /problematic_step
       target:
         deployment: test-deployment

Manual Command Execution
------------------------

Run the exact command from logs manually:

.. code-block:: bash

   # Copy command from debug logs
   # Execute in same environment
   ssh user@host 'command from logs'

Check Intermediate Files
------------------------

Inspect intermediate outputs:

.. code-block:: bash

   # Check working directories for intermediate files
   ls -la /tmp/streamflow/
   
   # Or check .streamflow directory
   ls -la .streamflow/

Common Error Messages
=====================

Database Errors
---------------

**Error:** ``Database is locked``

**Solution:**

* Close other StreamFlow instances
* Remove lock file if stale
* Use separate databases (``--outdir``)

**Error:** ``Unable to open database file``

**Solution:**

* Check file permissions
* Verify directory exists
* Check disk space

Network Errors
--------------

**Error:** ``Connection reset by peer``

**Solution:**

* Check network stability
* Verify firewall rules
* Increase timeout settings

**Error:** ``Name or service not known``

**Solution:**

* Verify hostname is correct
* Check DNS resolution
* Use IP address instead

Resource Errors
---------------

**Error:** ``Insufficient resources``

**Solution:**

* Reduce resource requests
* Scale deployment
* Use different deployment with more resources

Getting Help
============

If problems persist:

1. **Check Logs**
   
   * Enable debug logging
   * Review full error messages
   * Check connector-specific logs

2. **Search Documentation**
   
   * :doc:`/reference/index` - Connector-specific guidance
   * :doc:`/reference/cli/index` - CLI reference
   * CWL issues: https://www.commonwl.org/

3. **Search GitHub Issues**
   
   https://github.com/alpha-unito/streamflow/issues

4. **Report Bug**
   
   When reporting issues, include:
   
   * StreamFlow version (``streamflow version``)
   * Python version (``python --version``)
   * Operating system
   * Complete error message
   * Minimal reproducible example
   * Debug logs

5. **Ask for Help**
   
   * GitHub Discussions: https://github.com/alpha-unito/streamflow/discussions
   * Include context and what you've tried

Related Topics
==============

* :doc:`running-workflows` - Workflow execution guide
* :doc:`inspecting-results` - Debugging with reports
* :doc:`/reference/index` - Connector documentation
* :doc:`/developer-guide/index` - Architecture and internals
