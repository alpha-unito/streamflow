=================
Writing Workflows
=================

.. meta::
   :keywords: StreamFlow, CWL, Common Workflow Language, workflow, CommandLineTool, steps
   :description: Learn how to write CWL workflows for StreamFlow execution

Overview
========

StreamFlow uses the `Common Workflow Language <https://www.commonwl.org/>`_ (CWL) standard to describe workflows. This guide introduces CWL concepts and shows how to write workflows for StreamFlow execution.

CWL Support in StreamFlow
==========================

StreamFlow implements CWL conformance for multiple versions:

===========  ===================  ===============================
Version      Conformance Status   Notes
===========  ===================  ===============================
v1.0         Full conformance     All required and optional features
v1.1         Full conformance     Including scatter/gather
v1.2         Full conformance     Including conditional execution
v1.3         Partial support      Under development
===========  ===================  ===============================

For complete conformance details, see :doc:`/reference/cwl-support/index`.

.. note::
   StreamFlow does not modify the CWL specification. All standard CWL workflows are compatible with StreamFlow.

CWL Basics
==========

A CWL workflow consists of:

**Workflow**
   A directed acyclic graph (DAG) of computational steps.

**Steps**
   Individual computational tasks, each described by a CommandLineTool or sub-workflow.

**Inputs**
   Data and parameters provided to the workflow.

**Outputs**
   Results produced by the workflow.

**Connections**
   Data flow between steps via input/output bindings.

Minimal Workflow Example
========================

Here's the simplest possible CWL workflow:

.. code-block:: yaml
   :caption: hello-world.cwl - Minimal workflow

   cwlVersion: v1.2
   class: Workflow
   
   inputs:
     message: string
   
   outputs:
     output_file:
       type: File
       outputSource: echo_step/outfile
   
   steps:
     echo_step:
       run:
         class: CommandLineTool
         baseCommand: echo
         inputs:
           msg:
             type: string
             inputBinding:
               position: 1
         outputs:
           outfile:
             type: stdout
         stdout: output.txt
       in:
         msg: message
       out: [outfile]

**Usage:**

.. code-block:: yaml
   :caption: hello-inputs.yml

   message: "Hello from StreamFlow!"

.. code-block:: bash
   :caption: Execute the workflow

   streamflow run workflow.yml

Where ``workflow.yml`` is the StreamFlow configuration file that references ``hello-world.cwl``.

CommandLineTool Structure
==========================

A CommandLineTool describes how to execute a single command:

.. code-block:: yaml
   :caption: CommandLineTool components

   class: CommandLineTool
   baseCommand: [command, arg1]     # Base command to execute
   arguments: [--flag, value]       # Additional arguments
   inputs:                          # Input parameters
     input_name:
       type: File | string | int | boolean | ...
       inputBinding:
         position: 1                # Argument position
         prefix: --input            # Command-line flag
   outputs:                         # Output specifications
     output_name:
       type: File | Directory | ...
       outputBinding:
         glob: "*.txt"              # Output file pattern
   requirements:                    # Tool requirements
     - class: DockerRequirement
       dockerPull: ubuntu:22.04
   hints:                           # Optional hints
     - class: ResourceRequirement
       coresMin: 4
       ramMin: 8192

Data Types
----------

CWL supports various data types:

==================  ========================================
Type                Description
==================  ========================================
``string``          Text string
``int``             Integer number
``long``            Long integer
``float``           Floating-point number
``double``          Double-precision float
``boolean``         True or false
``File``            File reference
``Directory``       Directory reference
``null``            Null value (for optional inputs)
``array``           Array of any type (e.g., ``File[]``)
``record``          Structured data (custom schema)
``enum``            Enumerated values
==================  ========================================

Optional inputs use a union type: ``[null, string]`` or ``string?``

Multi-Step Workflow Example
============================

Here's a more realistic workflow with multiple steps:

.. code-block:: yaml
   :caption: compile-workflow.cwl - Extract and compile Java source

   cwlVersion: v1.2
   class: Workflow
   
   inputs:
     tarball: File
     name_of_file_to_extract: string
   
   outputs:
     compiled_class:
       type: File
       outputSource: compile/classfile
   
   steps:
     untar:
       run:
         class: CommandLineTool
         baseCommand: [tar, --extract]
         inputs:
           tarfile:
             type: File
             inputBinding:
               prefix: --file
           extractfile: string
         outputs:
           extracted_file:
             type: File
             outputBinding:
               glob: $(inputs.extractfile)
       in:
         tarfile: tarball
         extractfile: name_of_file_to_extract
       out: [extracted_file]
     
     compile:
       run:
         class: CommandLineTool
         baseCommand: javac
         arguments: ["-d", $(runtime.outdir)]
         inputs:
           src:
             type: File
             inputBinding:
               position: 1
         outputs:
           classfile:
             type: File
             outputBinding:
               glob: "*.class"
       in:
         src: untar/extracted_file
       out: [classfile]

**Key Points:**

* Steps execute in dependency order (``compile`` waits for ``untar``)
* Data flows from ``untar/extracted_file`` to ``compile/src``
* Each step has its own CommandLineTool definition

Workflow Requirements
=====================

Requirements specify runtime conditions:

Common Requirements
-------------------

**DockerRequirement**
   Run in a Docker container:

   .. code-block:: yaml

      requirements:
        DockerRequirement:
          dockerPull: python:3.10

**InitialWorkDirRequirement**
   Stage files in the working directory:

   .. code-block:: yaml

      requirements:
        InitialWorkDirRequirement:
          listing:
            - $(inputs.input_file)
            - entry: $(inputs.config_data)
              entryname: config.json

**ResourceRequirement**
   Specify resource needs:

   .. code-block:: yaml

      requirements:
        ResourceRequirement:
          coresMin: 4
          coresMax: 8
          ramMin: 8192       # MB
          ramMax: 16384
          tmpdirMin: 10000   # MB
          outdirMin: 10000

**ScatterFeatureRequirement**
   Enable parallel scatter execution:

   .. code-block:: yaml

      requirements:
        ScatterFeatureRequirement: {}

**SubworkflowFeatureRequirement**
   Use sub-workflows:

   .. code-block:: yaml

      requirements:
        SubworkflowFeatureRequirement: {}

For complete list, see the `CWL specification <https://www.commonwl.org/v1.2/CommandLineTool.html#Requirements_and_hints>`_.

Scatter/Gather Pattern
======================

Process arrays in parallel using scatter:

.. code-block:: yaml
   :caption: Scatter example - Process multiple files

   cwlVersion: v1.2
   class: Workflow
   
   requirements:
     ScatterFeatureRequirement: {}
   
   inputs:
     files: File[]
   
   outputs:
     processed:
       type: File[]
       outputSource: process/output
   
   steps:
     process:
       run: process-tool.cwl
       scatter: input_file
       in:
         input_file: files
       out: [output]

StreamFlow will schedule scattered tasks across available locations for parallel execution.

Conditional Execution (CWL v1.2+)
=================================

Execute steps conditionally:

.. code-block:: yaml
   :caption: Conditional workflow

   cwlVersion: v1.2
   class: Workflow
   
   requirements:
     InlineJavascriptRequirement: {}
   
   inputs:
     run_optional: boolean
     data: File
   
   outputs:
     result:
       type: File
       outputSource: process/output
   
   steps:
     optional_step:
       when: $(inputs.run_optional)
       run: preprocessing.cwl
       in:
         run_optional: run_optional
         input: data
       out: [preprocessed]
     
     process:
       run: main-processing.cwl
       in:
         input:
           source: [optional_step/preprocessed, data]
           pickValue: first_non_null
       out: [output]

JavaScript Expressions
======================

CWL supports JavaScript for dynamic values:

.. code-block:: yaml
   :caption: Using JavaScript expressions

   requirements:
     InlineJavascriptRequirement: {}
   
   inputs:
     input_file:
       type: File
       inputBinding:
         # Remove .txt extension and add .processed.txt
         valueFrom: |
           $(inputs.input_file.nameroot + '.processed' + inputs.input_file.nameext)

**Available variables:**

* ``inputs.*`` - Input values
* ``self`` - Current value
* ``runtime.*`` - Runtime environment (``cores``, ``ram``, ``outdir``, ``tmpdir``)

External Tool Definitions
==========================

Keep CommandLineTool definitions in separate files:

.. code-block:: yaml
   :caption: workflow.cwl - Reference external tools

   cwlVersion: v1.2
   class: Workflow
   
   steps:
     align:
       run: tools/bwa-mem.cwl      # External tool definition
       in:
         reference: ref_genome
         reads: input_reads
       out: [aligned]
     
     sort:
       run: tools/samtools-sort.cwl
       in:
         input: align/aligned
       out: [sorted]

.. code-block:: yaml
   :caption: tools/bwa-mem.cwl - External tool definition

   class: CommandLineTool
   baseCommand: [bwa, mem]
   inputs:
     reference:
       type: File
       inputBinding:
         position: 1
     reads:
       type: File
       inputBinding:
         position: 2
   outputs:
     aligned:
       type: File
       outputBinding:
         glob: "aligned.sam"
   stdout: aligned.sam

StreamFlow-Specific Considerations
===================================

While StreamFlow follows the CWL standard, consider these points:

File Transfer
-------------

StreamFlow automatically transfers files between execution locations. Use appropriate data transfer strategies in your workflow design. See :doc:`/user-guide/advanced-patterns/index` for details.

Container Translation
---------------------

DockerRequirement is automatically translated to the appropriate container runtime (Docker, Singularity, Kubernetes) based on deployment configuration. See :doc:`/reference/cwl-docker-translators/index`.

Binding Filters
---------------

StreamFlow extends CWL with binding filters to control which steps run where. This is configured in the StreamFlow YAML file, not the CWL file. See :doc:`binding-workflows`.

Resource Hints
--------------

ResourceRequirement hints guide StreamFlow's scheduler but don't enforce hard limits unless configured in the deployment.

Best Practices
==============

1. **Keep Tools Separate**
   
   Store CommandLineTool definitions in separate files for reusability:
   
   ::
   
      workflows/
        my-workflow.cwl
        tools/
          tool1.cwl
          tool2.cwl

2. **Use Descriptive Names**
   
   Use clear, descriptive names for steps, inputs, and outputs:
   
   .. code-block:: yaml
   
      # Good
      align_reads:
        run: bwa-mem.cwl
      
      # Avoid
      step1:
        run: tool.cwl

3. **Document Inputs**
   
   Add documentation to inputs:
   
   .. code-block:: yaml
   
      inputs:
        reference_genome:
          type: File
          label: "Reference genome FASTA"
          doc: "Reference genome in FASTA format for alignment"

4. **Validate Workflows**
   
   Use ``cwltool --validate`` to check syntax:
   
   .. code-block:: bash
   
      cwltool --validate my-workflow.cwl

5. **Test Locally First**
   
   Test workflows with local execution before deploying to remote infrastructure.

6. **Specify CWL Version**
   
   Always include ``cwlVersion`` to ensure compatibility.

7. **Use Type Hints**
   
   Explicitly type all inputs and outputs for better validation.

Common Patterns
===============

File Pairs (R1/R2 Reads)
------------------------

Handle paired-end sequencing reads:

.. code-block:: yaml

   inputs:
     reads:
       type:
         type: record
         fields:
           forward: File
           reverse: File

Multiple Output Files
---------------------

Capture multiple outputs:

.. code-block:: yaml

   outputs:
     results:
       type: File[]
       outputBinding:
         glob: "result_*.txt"

Secondary Files
---------------

Handle index files and other companions:

.. code-block:: yaml

   inputs:
     reference:
       type: File
       secondaryFiles:
         - .fai
         - ^.dict

Troubleshooting
===============

Common CWL Issues
-----------------

**Problem:** ``ValueError: Missing required input parameter``

**Solution:** Ensure all required inputs are provided in the inputs file.

**Problem:** ``Output glob pattern matches no files``

**Solution:** 

* Check that the command actually creates the expected output file
* Verify the glob pattern matches the actual filename
* Check working directory and output directory paths

**Problem:** ``Invalid JavaScript expression``

**Solution:**

* Add ``InlineJavascriptRequirement`` to requirements
* Check JavaScript syntax
* Verify variable names (``inputs.*``, ``runtime.*``)

Validation Errors
-----------------

Use ``cwltool`` for validation:

.. code-block:: bash

   # Validate workflow syntax
   cwltool --validate workflow.cwl
   
   # Validate with inputs
   cwltool --validate workflow.cwl inputs.yml
   
   # Print detailed validation info
   cwltool --print-pre workflow.cwl

StreamFlow-Specific Issues
---------------------------

**Problem:** Workflow works with ``cwltool`` but fails in StreamFlow

**Solution:**

* Check that all files are accessible from the execution location
* Verify deployment bindings are correct
* Check container availability on target deployment
* Review StreamFlow logs for detailed error messages

Next Steps
==========

After writing your workflow:

* :doc:`configuring-deployments` - Set up execution environments
* :doc:`binding-workflows` - Bind workflow steps to deployments
* :doc:`running-workflows` - Execute and monitor workflows
* :doc:`/user-guide/advanced-patterns/index` - Learn advanced binding patterns

Learning Resources
==================

**Official CWL Documentation:**
   * `CWL User Guide <https://www.commonwl.org/user_guide/>`_ - Comprehensive tutorial
   * `CWL Specification <https://www.commonwl.org/v1.2/>`_ - Complete reference
   * `CWL Command Line Tool <https://github.com/common-workflow-language/cwltool>`_ - Reference implementation

**StreamFlow Resources:**
   * :doc:`/reference/cwl-support/index` - CWL conformance details
   * :doc:`/reference/cwl-docker-translators/index` - Container translation
   * `GitHub Examples <https://github.com/alpha-unito/streamflow/tree/main/examples>`_ - Sample workflows

Related Topics
==============

* :doc:`quickstart` - Simple workflow example
* :doc:`/reference/configuration/workflow-config` - Workflow configuration schema
* :doc:`/developer-guide/core-interfaces/workflow` - Workflow interface internals
* `CWL Conformance Tests <https://github.com/common-workflow-language/cwl-v1.2>`_ - Test suite
