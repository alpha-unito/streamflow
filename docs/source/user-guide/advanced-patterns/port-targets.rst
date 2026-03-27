============
Port Targets
============

.. meta::
   :keywords: StreamFlow, port binding, data staging, remote files, input output
   :description: Learn how to bind input/output ports to specific locations for optimal data management

Overview
========

Port targets allow you to specify where StreamFlow should look for input files or place output files, overriding default behavior. This is essential for workflows with data already on remote systems or when you need to control data placement.

When to Use Port Targets
=========================

Use port targets when:

==========================  ========================================
Scenario                    Solution
==========================  ========================================
Data on Remote System       Avoid transferring large datasets
Shared Storage              Access files on shared filesystem
Output Placement            Write results to specific location
Data Locality               Execute where data resides
Archive Integration         Read from/write to archive storage
==========================  ========================================

Default Behavior
================

Without port targets:

**Input Files:**
  StreamFlow looks for inputs in the local filesystem

**Output Files:**
  StreamFlow retrieves outputs from the execution location

**Intermediate Files:**
  StreamFlow transfers files between locations as needed

Port Target Syntax
==================

Basic Structure
---------------

.. code-block:: yaml
   :caption: Port target configuration

   bindings:
     - port: /path/to/port
       target:
         deployment: deployment-name
         workdir: /remote/directory

**Required Fields:**

* ``port`` - POSIX-like path to the port
* ``target.deployment`` - Deployment where files are located
* ``target.workdir`` - Base directory for files

Port Path Format
----------------

Ports use POSIX-like paths:

==================  ========================================
Path                Description
==================  ========================================
``/input_file``     Workflow input port
``/step/in_port``   Input port of a step
``/step/out_port``  Output port of a step
==================  ========================================

**Path Structure:** ``/[step-name]/port-name``

**Root-level ports:** Omit step name (``/port-name``)

Basic Examples
==============

Remote Input File
-----------------

Data already exists on HPC system:

.. code-block:: yaml
   :caption: Access remote input file

   version: v1.0
   
   workflows:
     process-data:
       type: cwl
       config:
         file: workflow.cwl
       bindings:
         - step: /process
           target:
             deployment: hpc-cluster
         
         - port: /input_data
           target:
             deployment: hpc-cluster
             workdir: /scratch/user/datasets
   
   deployments:
     hpc-cluster:
       type: slurm
       config:
         hostname: hpc.example.edu
         username: user
         sshKey: ~/.ssh/id_rsa

**Behavior:**

* ``/input_data`` file is accessed directly on HPC
* No transfer from local machine
* Step executes on HPC where data resides

Remote Output Location
----------------------

Write results to specific remote location:

.. code-block:: yaml
   :caption: Direct output to archive

   bindings:
     - step: /analyze
       target:
         deployment: compute-cluster
     
     - port: /results
       target:
         deployment: archive-storage
         workdir: /archive/project-123/results
   
   deployments:
     compute-cluster:
       type: kubernetes
       config: { ... }
     
     archive-storage:
       type: ssh
       config:
         hostname: archive.example.com
         username: archiver
         sshKey: ~/.ssh/archive_key

**Behavior:**

* Workflow executes on compute-cluster
* Final results are placed directly on archive-storage
* No intermediate transfer through local machine

Step-Specific Port Targets
===========================

Input Port of Specific Step
----------------------------

.. code-block:: yaml
   :caption: Step input from remote

   workflows:
     compile-workflow:
       type: cwl
       config:
         file: compile.cwl
       bindings:
         - step: /compile
           target:
             deployment: build-server
         
         - port: /compile/src
           target:
             deployment: source-repository
             workdir: /repos/project/src

**CWL Workflow:**

.. code-block:: yaml
   :caption: compile.cwl

   cwlVersion: v1.2
   class: Workflow
   
   inputs:
     tarball: File
     source_file: string
   
   steps:
     untar:
       run: untar.cwl
       in:
         archive: tarball
       out: [files]
     
     compile:
       run: javac.cwl
       in:
         src: untar/files  # This input
       out: [classfile]

**Result:**

* The ``src`` input port of the ``compile`` step reads from ``source-repository``
* Other inputs use default behavior

Output Port of Specific Step
-----------------------------

.. code-block:: yaml
   :caption: Step output to specific location

   bindings:
     - step: /process
       target:
         deployment: compute-cluster
     
     - port: /process/results
       target:
         deployment: results-storage
         workdir: /results/experiment-42

Complex Data Flow
=================

Multi-Step with Different Locations
------------------------------------

.. code-block:: yaml
   :caption: Complex data flow

   workflows:
     pipeline:
       type: cwl
       config:
         file: pipeline.cwl
       bindings:
         # Steps
         - step: /preprocess
           target:
             deployment: cloud-cluster
         
         - step: /analyze
           target:
             deployment: hpc-cluster
         
         - step: /visualize
           target:
             deployment: local
         
         # Ports
         - port: /raw_data
           target:
             deployment: hpc-storage
             workdir: /data/raw
         
         - port: /analyze/preprocessed
           target:
             deployment: hpc-storage
             workdir: /data/preprocessed
         
         - port: /final_plots
           target:
             deployment: web-server
             workdir: /var/www/html/plots
   
   deployments:
     cloud-cluster:
       type: kubernetes
       config: { ... }
     
     hpc-cluster:
       type: slurm
       config: { ... }
     
     hpc-storage:
       type: ssh
       config:
         hostname: storage.hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
     
     local:
       type: local
     
     web-server:
       type: ssh
       config:
         hostname: webserver.example.com
         username: www-data
         sshKey: ~/.ssh/web_key

**Data Flow:**

1. ``/raw_data`` read from HPC storage
2. ``/preprocess`` executes on cloud, writes to HPC storage
3. ``/analyze`` reads from HPC storage, executes on HPC
4. ``/visualize`` executes locally
5. ``/final_plots`` written to web server

Avoiding Large Transfers
-------------------------

.. code-block:: yaml
   :caption: Minimize data movement

   bindings:
     # Execute where data is
     - step: /process_large_dataset
       target:
         deployment: data-center
     
     # Data already there
     - port: /large_dataset
       target:
         deployment: data-center
         workdir: /mnt/datasets/project
     
     # Results stay there
     - port: /processed_results
       target:
         deployment: data-center
         workdir: /mnt/results/project

**Benefits:**

* No transfer of multi-TB dataset to local machine
* No transfer back to storage
* Execution happens where data resides

Shared Filesystem Scenarios
============================

HPC with Shared Storage
-----------------------

Common HPC pattern with shared filesystem:

.. code-block:: yaml
   :caption: HPC shared storage

   deployments:
     hpc-login:
       type: ssh
       config:
         hostname: login.hpc.edu
         username: user
         sshKey: ~/.ssh/id_rsa
     
     hpc-compute:
       type: slurm
       config: { ... }
       wraps: hpc-login
   
   bindings:
     - step: /compute
       target:
         deployment: hpc-compute
     
     # Data on shared filesystem
     - port: /input_data
       target:
         deployment: hpc-login
         workdir: /home/user/data
     
     - port: /results
       target:
         deployment: hpc-login
         workdir: /home/user/results

**Shared Filesystem:**

* ``/home/user`` visible to both login and compute nodes
* No data transfer needed between locations
* StreamFlow recognizes shared storage

Network Filesystem (NFS)
-------------------------

.. code-block:: yaml
   :caption: NFS-mounted storage

   bindings:
     - step: /process
       target:
         deployment: worker-nodes
     
     - port: /nfs_data
       target:
         deployment: nfs-server
         workdir: /exports/shared/data

Multiple Input Ports
=====================

Different inputs from different locations:

.. code-block:: yaml
   :caption: Multiple input sources

   workflows:
     merge-data:
       type: cwl
       config:
         file: merge.cwl
       bindings:
         - step: /merge
           target:
             deployment: processor
         
         - port: /dataset_a
           target:
             deployment: source-a
             workdir: /data/a
         
         - port: /dataset_b
           target:
             deployment: source-b
             workdir: /data/b
         
         - port: /reference
           target:
             deployment: reference-db
             workdir: /db/reference

**CWL Workflow:**

.. code-block:: yaml

   inputs:
     dataset_a: File
     dataset_b: File
     reference: File
   
   steps:
     merge:
       run: merge-tool.cwl
       in:
         input_a: dataset_a
         input_b: dataset_b
         ref: reference
       out: [merged]

Secondary Files
===============

Handle index files and companions:

.. code-block:: yaml
   :caption: Port with secondary files

   bindings:
     - port: /reference_genome
       target:
         deployment: genomics-data
         workdir: /genomes

**CWL Input with Secondary Files:**

.. code-block:: yaml

   inputs:
     reference_genome:
       type: File
       secondaryFiles:
         - .fai
         - .amb
         - .ann

**Behavior:**

* Main file: ``/genomes/hg38.fa``
* Secondary files automatically found:
  
  * ``/genomes/hg38.fa.fai``
  * ``/genomes/hg38.fa.amb``
  * ``/genomes/hg38.fa.ann``

Array Ports
===========

Port binding with array inputs:

.. code-block:: yaml
   :caption: Array port target

   bindings:
     - port: /input_files
       target:
         deployment: file-server
         workdir: /data/inputs

**CWL Array Input:**

.. code-block:: yaml

   inputs:
     input_files:
       type: File[]

**Behavior:**

All files in the array are accessed from the specified location.

Best Practices
==============

1. **Bind Step and Port Together**
   
   Execute where data resides:
   
   .. code-block:: yaml
   
      bindings:
        - step: /process
          target:
            deployment: hpc-cluster
        - port: /input
          target:
            deployment: hpc-cluster
            workdir: /data

2. **Use Absolute Paths**
   
   Specify full paths in ``workdir``:
   
   .. code-block:: yaml
   
      # Good
      workdir: /home/user/data
      
      # Avoid
      workdir: data  # Relative path

3. **Document Data Locations**
   
   Comment your port bindings:
   
   .. code-block:: yaml
   
      bindings:
        # Large genomics dataset stored on HPC shared filesystem
        - port: /genome_data
          target:
            deployment: hpc-storage
            workdir: /gpfs/genomics/references

4. **Consider Data Lifecycle**
   
   Plan where data is at each workflow stage.

5. **Test File Accessibility**
   
   Verify files exist at specified locations:
   
   .. code-block:: bash
   
      ssh user@host ls -la /data/input.txt

Troubleshooting
===============

File Not Found
--------------

**Problem:** ``No such file or directory``

**Solutions:**

1. **Verify file exists at remote location:**
   
   .. code-block:: bash
   
      ssh user@hostname ls -la /workdir/file.txt

2. **Check path is absolute:**
   
   .. code-block:: yaml
   
      workdir: /absolute/path  # Not relative

3. **Verify permissions:**
   
   .. code-block:: bash
   
      ssh user@hostname test -r /workdir/file.txt && echo "readable"

4. **Check CWL input name matches:**
   
   Port name must match CWL workflow input name.

Unexpected Transfer
-------------------

**Problem:** StreamFlow still transfers files

**Cause:** Port binding not recognized

**Solutions:**

* Verify port path matches CWL definition
* Check spelling of port name
* Ensure ``workdir`` is correct

Permission Denied
-----------------

**Problem:** ``Permission denied`` accessing remote file

**Solutions:**

* Verify SSH key has access
* Check file permissions on remote system
* Ensure user has read/write permissions
* Test manually: ``ssh user@host cat /path/to/file``

Wrong Deployment
----------------

**Problem:** Port bound to wrong deployment

**Solution:**

Carefully check deployment names:

.. code-block:: yaml

   # Deployment definition
   deployments:
     hpc-storage:  # Name here
       type: ssh
       config: { ... }
   
   # Port binding - must match
   bindings:
     - port: /data
       target:
         deployment: hpc-storage  # Must match exactly

Examples
========

Genomics Pipeline
-----------------

.. code-block:: yaml
   :caption: Genomics workflow with port targets

   bindings:
     - step: /align
       target:
         deployment: hpc-cluster
     
     - step: /call_variants
       target:
         deployment: hpc-cluster
     
     # Reference genome on shared storage
     - port: /reference
       target:
         deployment: hpc-storage
         workdir: /genomes/hg38
     
     # Raw reads on shared storage
     - port: /reads
       target:
         deployment: hpc-storage
         workdir: /sequencing/project-123/raw
     
     # Results to project directory
     - port: /variants
       target:
         deployment: hpc-storage
         workdir: /sequencing/project-123/results

Machine Learning Training
--------------------------

.. code-block:: yaml
   :caption: ML training with remote data

   bindings:
     - step: /train_model
       target:
         deployment: gpu-cluster
     
     # Training data on fast storage
     - port: /training_data
       target:
         deployment: nvme-storage
         workdir: /fast-storage/datasets/imagenet
     
     # Checkpoints to persistent storage
     - port: /checkpoints
       target:
         deployment: persistent-storage
         workdir: /models/experiment-42/checkpoints

Next Steps
==========

After mastering port targets:

* :doc:`stacked-locations` - Complex deployment hierarchies
* :doc:`multiple-targets` - Multiple target strategies
* :doc:`/user-guide/configuring-deployments` - Deployment details
* :doc:`/developer-guide/core-interfaces/data` - Data management internals

Related Topics
==============

* :doc:`/user-guide/binding-workflows` - Basic binding concepts
* :doc:`/user-guide/writing-workflows` - CWL workflow syntax
* :doc:`/reference/configuration/binding-config` - Complete binding reference
