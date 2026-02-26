==================
Running Workflows
==================

.. meta::
   :keywords: StreamFlow, execute, run, workflow, CLI, monitoring, logs
   :description: Learn how to execute and monitor StreamFlow workflows

Overview
========

This guide explains how to execute workflows using the StreamFlow CLI, monitor execution progress, and understand execution options.

Basic Execution
===============

Command Syntax
--------------

.. code-block:: bash
   :caption: Execute a workflow

   streamflow run /path/to/streamflow.yml

This command:

1. Parses the StreamFlow configuration file
2. Deploys the specified execution environments
3. Executes the workflow according to bindings
4. Collects outputs and metadata
5. Tears down deployments

Example Execution
-----------------

.. code-block:: bash

   $ # Navigate to workflow directory
   $ cd my-workflow-project
   $ 
   $ # Execute workflow
   $ streamflow run streamflow.yml

   2024-02-24 12:00:00.123 INFO     StreamFlow version 0.2.0.dev14
   2024-02-24 12:00:00.456 INFO     Loading workflow from streamflow.yml
   2024-02-24 12:00:01.789 INFO     Deploying environment: docker-python
   2024-02-24 12:00:03.012 INFO     Starting workflow execution
   2024-02-24 12:00:05.345 INFO     Step /preprocess completed
   2024-02-24 12:00:08.678 INFO     Step /analyze completed
   2024-02-24 12:00:10.901 INFO     Workflow completed successfully
   2024-02-24 12:00:11.234 INFO     Undeploying environments
   2024-02-24 12:00:12.567 INFO     Results saved to ./results

Command-Line Options
====================

Output Directory
----------------

Specify where to store workflow results:

.. code-block:: bash
   :caption: Set output directory

   streamflow run streamflow.yml --outdir /path/to/outputs

**Default:** Current directory (``.``)

**Output Contents:**

::

   outputs/
   ├── result_file_1.txt      # Workflow output files
   ├── result_file_2.csv
   └── .streamflow/           # StreamFlow metadata
       └── workflow.db        # Execution database

Workflow Name
-------------

Assign a name to the workflow execution:

.. code-block:: bash
   :caption: Name workflow execution

   streamflow run streamflow.yml --name my-experiment-v1

**Purpose:**

* Track multiple executions of the same workflow
* Generate reports for specific executions
* Organize workflow history

**Default:** Auto-generated unique name

Colored Logs
------------

Enable colored log output:

.. code-block:: bash
   :caption: Enable colored logs

   streamflow run streamflow.yml --color

**Colors by Level:**

* **ERROR** - Red
* **WARNING** - Yellow
* **INFO** - Green
* **DEBUG** - Blue

**Use Cases:**

* Live demos
* Interactive terminal sessions
* Faster log inspection

Log Level
---------

Control logging verbosity:

.. code-block:: bash
   :caption: Set log level

   # Minimal output (only results, warnings, and errors)
   streamflow run streamflow.yml --quiet
   
   # Normal output (default - shows workflow progress)
   streamflow run streamflow.yml
   
   # Detailed output (debug-level diagnostics)
   streamflow run streamflow.yml --debug

**Levels:**

================  ========================================
Flag              Output
================  ========================================
``--quiet``       Only results, warnings, and errors
(default)         Normal execution information
``--debug``       Detailed debugging information
================  ========================================

Complete Example
----------------

.. code-block:: bash
   :caption: Full command with options

   streamflow run streamflow.yml \
     --outdir ./experiment-results \
     --name genome-pipeline-run-42 \
     --color \
     --log-level INFO

CWL Runner Interface
====================

StreamFlow supports the ``cwl-runner`` interface for CWL compatibility:

Basic Usage
-----------

.. code-block:: bash
   :caption: Use cwl-runner interface

   cwl-runner workflow.cwl inputs.yml

This executes the CWL workflow directly without a StreamFlow configuration file.

**Equivalent to:**

.. code-block:: yaml
   :caption: streamflow.yml equivalent

   version: v1.0
   workflows:
     workflow:
       type: cwl
       config:
         file: workflow.cwl
         settings: inputs.yml
       bindings:
         - step: /
           target:
             deployment: local
   
   deployments:
     local:
       type: local

With Deployments
----------------

To use custom deployments with ``cwl-runner``, create a minimal StreamFlow file:

.. code-block:: bash
   :caption: CWL runner with deployments

   cwl-runner \
     --streamflow-file streamflow-config.yml \
     workflow.cwl \
     inputs.yml

.. code-block:: yaml
   :caption: streamflow-config.yml

   version: v1.0
   deployments:
     docker-env:
       type: docker
       config:
         image: python:3.10
   bindings:
     - step: /
       target:
         deployment: docker-env

Running in Containers
=====================

Run StreamFlow Docker Image
----------------------------

Execute StreamFlow in a container:

.. code-block:: bash
   :caption: Run StreamFlow in Docker

   docker run --rm \
     -v "$(pwd)"/my-project:/streamflow/project \
     -v "$(pwd)"/results:/streamflow/results \
     -v "$(pwd)"/tmp:/tmp/streamflow \
     alphaunito/streamflow \
     streamflow run /streamflow/project/streamflow.yml

**Volume Mounts:**

==========================  ========================================
Mount                       Purpose
==========================  ========================================
``my-project``              Workflow files (streamflow.yml, etc.)
``results``                 Workflow outputs
``tmp``                     Temporary files
``$HOME/.streamflow``       Metadata database (optional)
==========================  ========================================

Complete Docker Example
-----------------------

.. code-block:: bash
   :caption: Complete Docker execution

   docker run -d \
     --name streamflow-execution \
     --mount type=bind,source="$(pwd)"/my-project,target=/streamflow/project \
     --mount type=bind,source="$(pwd)"/results,target=/streamflow/results \
     --mount type=bind,source="$(pwd)"/tmp,target=/tmp/streamflow \
     --mount type=bind,source="$HOME"/.streamflow,target=/root/.streamflow \
     alphaunito/streamflow \
     streamflow run /streamflow/project/streamflow.yml \
       --outdir /streamflow/results \
       --name my-workflow \
       --color

**Monitor logs:**

.. code-block:: bash

   docker logs -f streamflow-execution

**Limitations:**

Container-based connectors (Docker, Docker Compose, Singularity) are **not supported** from inside a Docker container due to nested container complexity.

Running on Kubernetes
=====================

Deploy as Kubernetes Job
------------------------

.. code-block:: yaml
   :caption: streamflow-job.yaml - Kubernetes Job

   apiVersion: batch/v1
   kind: Job
   metadata:
     name: streamflow-workflow
   spec:
     template:
       spec:
         containers:
           - name: streamflow
             image: alphaunito/streamflow:latest
             command:
               - streamflow
               - run
               - /workflow/streamflow.yml
             volumeMounts:
               - name: workflow
                 mountPath: /workflow
               - name: results
                 mountPath: /results
         volumes:
           - name: workflow
             configMap:
               name: streamflow-config
           - name: results
             persistentVolumeClaim:
               claimName: streamflow-results
         restartPolicy: Never

Apply the job:

.. code-block:: bash
   :caption: Deploy StreamFlow job

   kubectl create configmap streamflow-config \
     --from-file=streamflow.yml \
     --from-file=workflow.cwl \
     --from-file=inputs.yml
   
   kubectl apply -f streamflow-job.yaml

In-Cluster Execution
--------------------

For Helm connector to deploy on the same cluster:

.. code-block:: yaml
   :caption: streamflow.yml - In-cluster config

   deployments:
     helm-deployment:
       type: helm
       config:
         inCluster: true  # Use ServiceAccount credentials
         chart: bitnami/spark
         release: spark-cluster

**Requirements:**

* Proper RBAC configuration
* ServiceAccount with deployment permissions

.. code-block:: yaml
   :caption: rbac.yaml - Required permissions

   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: streamflow-sa
   ---
   apiVersion: rbac.authorization.k8s.io/v1
   kind: Role
   metadata:
     name: streamflow-role
   rules:
     - apiGroups: ["", "apps", "batch"]
       resources: ["pods", "deployments", "jobs", "services"]
       verbs: ["get", "list", "create", "delete", "watch"]
   ---
   apiVersion: rbac.authorization.k8s.io/v1
   kind: RoleBinding
   metadata:
     name: streamflow-rolebinding
   roleRef:
     apiGroup: rbac.authorization.k8s.io
     kind: Role
     name: streamflow-role
   subjects:
     - kind: ServiceAccount
       name: streamflow-sa

Monitoring Execution
====================

Progress Tracking
-----------------

StreamFlow logs provide real-time progress:

.. code-block:: text
   :caption: Example log output

   2024-02-24 12:00:00.123 INFO     StreamFlow version 0.2.0.dev14
   2024-02-24 12:00:00.456 INFO     Loading workflow from streamflow.yml
   2024-02-24 12:00:01.789 INFO     Deploying environment: kubernetes-cluster
   2024-02-24 12:00:05.012 INFO     Deployment kubernetes-cluster ready
   2024-02-24 12:00:05.345 INFO     Starting workflow execution
   2024-02-24 12:00:06.678 INFO     Executing step: /preprocess
   2024-02-24 12:00:15.901 INFO     Step /preprocess completed (9.2s)
   2024-02-24 12:00:16.234 INFO     Executing step: /analyze (scattered: 10 instances)
   2024-02-24 12:01:45.567 INFO     Step /analyze completed (89.3s)
   2024-02-24 12:01:45.890 INFO     Executing step: /visualize
   2024-02-24 12:01:52.123 INFO     Step /visualize completed (6.2s)
   2024-02-24 12:01:52.456 INFO     Workflow completed successfully
   2024-02-24 12:01:52.789 INFO     Total execution time: 112.4s
   2024-02-24 12:01:53.012 INFO     Undeploying environments
   2024-02-24 12:01:55.345 INFO     Results saved to ./results

Debug Logs
----------

Enable debug logging for troubleshooting:

.. code-block:: bash
   :caption: Debug logging

   streamflow run streamflow.yml --debug

Debug logs include:

* Detailed connector operations
* File transfer information
* Scheduling decisions
* Data management operations

Log Files
---------

Redirect logs to a file:

.. code-block:: bash
   :caption: Save logs to file

   streamflow run streamflow.yml 2>&1 | tee workflow-execution.log

Or:

.. code-block:: bash

   streamflow run streamflow.yml > workflow.log 2>&1

Execution Lifecycle
===================

StreamFlow execution follows these phases:

1. **Initialization**
   
   * Parse configuration
   * Validate workflow syntax
   * Initialize database

2. **Deployment**
   
   * Deploy execution environments
   * Verify connectivity
   * Create services/locations

3. **Execution**
   
   * Schedule workflow steps
   * Transfer input data
   * Execute tasks
   * Collect outputs

4. **Collection**
   
   * Gather results
   * Save metadata
   * Generate provenance

5. **Teardown**
   
   * Undeploy environments
   * Clean temporary files
   * Close connections

Execution States
----------------

================  ========================================
State             Description
================  ========================================
``PENDING``       Workflow queued for execution
``RUNNING``       Workflow currently executing
``COMPLETED``     Workflow finished successfully
``FAILED``        Workflow encountered errors
``CANCELLED``     Workflow manually stopped
================  ========================================

Handling Failures
=================

Automatic Retry
---------------

StreamFlow automatically retries failed tasks based on configuration:

.. code-block:: yaml
   :caption: Configure retry behavior (planned feature)

   workflows:
     my-workflow:
       config:
         retry:
           maxAttempts: 3
           backoff: exponential

Checkpointing
-------------

StreamFlow supports checkpointing for long-running workflows (see recovery features in reference documentation).

Manual Intervention
-------------------

If workflow fails:

1. **Check logs** for error messages
2. **Verify deployments** are accessible
3. **Fix issues** (connectivity, resources, etc.)
4. **Restart workflow** from checkpoint or beginning

Performance Optimization
========================

Parallel Execution
------------------

Use scatter for parallel processing:

.. code-block:: yaml
   :caption: CWL scatter for parallelism

   steps:
     process:
       run: process-tool.cwl
       scatter: input_file
       in:
         input_file: input_files  # Array of files
       out: [output]

StreamFlow schedules scattered tasks across available locations.

Resource Allocation
-------------------

Specify resource requirements in CWL:

.. code-block:: yaml
   :caption: Resource hints

   hints:
     ResourceRequirement:
       coresMin: 4
       ramMin: 8192  # MB

Data Locality
-------------

Bind steps to deployments where data resides:

.. code-block:: yaml
   :caption: Optimize data locality

   bindings:
     - step: /process_large_data
       target:
         deployment: hpc-storage
     - port: /large_dataset
       target:
         deployment: hpc-storage
         workdir: /data

Best Practices
==============

1. **Test Locally First**
   
   .. code-block:: bash
   
      # Test with local deployment
      streamflow run streamflow-local.yml

2. **Use Descriptive Names**
   
   .. code-block:: bash
   
      streamflow run workflow.yml --name experiment-2024-02-24-v3

3. **Save Logs**
   
   .. code-block:: bash
   
      streamflow run workflow.yml 2>&1 | tee logs/execution-$(date +%Y%m%d-%H%M%S).log

4. **Monitor Resource Usage**
   
   Use system monitoring tools alongside StreamFlow execution.

5. **Organize Outputs**
   
   .. code-block:: bash
   
      mkdir -p results/$(date +%Y-%m-%d)
      streamflow run workflow.yml --outdir results/$(date +%Y-%m-%d)

6. **Version Control Workflows**
   
   Keep ``streamflow.yml`` and CWL files in version control.

Troubleshooting
===============

Workflow Won't Start
--------------------

**Problem:** Workflow fails immediately at startup

**Solution:**

* Check YAML syntax: ``streamflow schema`` for validation
* Verify all referenced files exist
* Check deployment connectivity
* Review initialization logs

Stuck at Deployment
-------------------

**Problem:** Workflow hangs during deployment phase

**Solution:**

* Check network connectivity to deployment targets
* Verify credentials (SSH keys, kubeconfig, etc.)
* Check resource availability
* Review connector-specific logs

Step Execution Fails
--------------------

**Problem:** Specific workflow step fails

**Solution:**

* Check step-specific logs
* Verify input files are accessible
* Check command/tool is available in execution environment
* Verify resource requirements are met

Slow Execution
--------------

**Problem:** Workflow runs slower than expected

**Solution:**

* Check network bandwidth for data transfers
* Verify adequate resources allocated
* Review scheduling decisions in debug logs
* Consider data locality optimizations

For comprehensive troubleshooting, see :doc:`troubleshooting`.

Next Steps
==========

After running workflows:

* :doc:`inspecting-results` - Analyze workflow execution and results
* :doc:`troubleshooting` - Resolve common issues
* :doc:`/reference/cli/run` - Complete CLI reference
* :doc:`/reference/cli/report` - Generate execution reports

Related Topics
==============

* :doc:`binding-workflows` - Configure workflow bindings
* :doc:`/reference/cli/index` - Complete CLI documentation
* :doc:`/developer-guide/core-interfaces/workflow` - Workflow execution internals
