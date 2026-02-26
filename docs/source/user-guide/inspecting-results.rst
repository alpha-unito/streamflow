==================
Inspecting Results
==================

.. meta::
   :keywords: StreamFlow, results, provenance, report, metadata, inspection
   :description: Learn how to inspect workflow execution results and generate reports in StreamFlow

Overview
========

After workflow execution, StreamFlow provides tools to inspect results, analyze execution metadata, and generate reports. This guide covers the inspection capabilities of the StreamFlow CLI.

StreamFlow Metadata
===================

StreamFlow automatically collects execution metadata:

========================  ========================================
Metadata Type             Information Collected
========================  ========================================
**Execution**             Start/end times, status, duration
**Workflow**              Workflow name, type, configuration
**Steps**                 Step execution times, status, locations
**Data**                  Input/output files, data transfers
**Deployments**           Deployed environments, resources
**Provenance**            Complete execution trace
========================  ========================================

**Storage Location:**

By default: ``${HOME}/.streamflow/workflow.db``

Or: ``<outdir>/.streamflow/workflow.db`` if ``--outdir`` was specified

Listing Workflows
=================

List All Workflows
------------------

View all executed workflows:

.. code-block:: bash

   $ streamflow list

   ====================  ======  ===========
   NAME                  TYPE    EXECUTIONS
   ====================  ======  ===========
   data-pipeline         cwl     5
   analysis-workflow     cwl     3
   test-workflow         cwl     12
   ====================  ======  ===========

**Columns:**

* **NAME** - Workflow name (from ``--name`` or auto-generated)
* **TYPE** - Workflow type (``cwl``)
* **EXECUTIONS** - Number of times workflow was executed

List Workflow Executions
-------------------------

View execution history for a specific workflow:

.. code-block:: bash

   $ streamflow list data-pipeline

   ==================================  ==================================  ===========
   START_TIME                          END_TIME                            STATUS
   ==================================  ==================================  ===========
   2024-02-20T09:15:23.456789+00:00    2024-02-20T09:45:12.345678+00:00    COMPLETED
   2024-02-21T14:30:45.678901+00:00    2024-02-21T15:10:23.456789+00:00    COMPLETED
   2024-02-22T08:20:15.234567+00:00    2024-02-22T08:22:30.123456+00:00    FAILED
   2024-02-23T10:45:30.345678+00:00    2024-02-23T11:20:15.234567+00:00    COMPLETED
   2024-02-24T13:10:45.456789+00:00    2024-02-24T13:55:30.345678+00:00    COMPLETED
   ==================================  ==================================  ===========

**Columns:**

* **START_TIME** - When execution started (ISO 8601 format)
* **END_TIME** - When execution finished
* **STATUS** - Final status (COMPLETED, FAILED, CANCELLED)

**Use Cases:**

* Track workflow execution history
* Identify failed runs
* Compare execution times
* Debugging and auditing

Execution Status
----------------

================  ========================================
Status            Meaning
================  ========================================
``PENDING``       Workflow queued for execution
``RUNNING``       Currently executing
``COMPLETED``     Finished successfully
``FAILED``        Encountered fatal error
``CANCELLED``     Manually stopped
================  ========================================

Generating Reports
==================

Interactive HTML Report
-----------------------

Generate an interactive timeline report:

.. code-block:: bash
   :caption: Generate HTML report

   streamflow report data-pipeline

**Output:** ``data-pipeline.html`` (interactive visualization)

**Report Contents:**

* Timeline of step executions
* Resource utilization
* Data transfer operations
* Execution statistics
* Interactive exploration

**Open in Browser:**

.. code-block:: bash

   # macOS
   open data-pipeline.html
   
   # Linux
   xdg-open data-pipeline.html
   
   # macOS
   open data-pipeline.html

Report Formats
--------------

StreamFlow supports multiple report formats:

.. code-block:: bash
   :caption: Generate different format reports

   # HTML (default, interactive)
   streamflow report workflow-name --format html
   
   # JSON (machine-readable)
   streamflow report workflow-name --format json
   
   # Text (console-friendly)
   streamflow report workflow-name --format text

**Available Formats:**

================  ========================================
Format            Use Case
================  ========================================
``html``          Interactive visualization (default)
``json``          Programmatic analysis
``text``          Console inspection
================  ========================================

Report Multiple Executions
--------------------------

By default, reports include only the most recent execution. To include all executions:

.. code-block:: bash
   :caption: Report for all executions

   streamflow report workflow-name --all

**Use Cases:**

* Track workflow evolution over time
* Compare different runs of the same workflow
* Analyze performance trends

Multi-Workflow Reports
----------------------

Generate combined report from multiple workflows:

.. code-block:: bash
   :caption: Combined report

   streamflow report workflow1,workflow2,workflow3

**Output:** Single report comparing all three workflows

**Use Cases:**

* Compare different workflow versions
* Analyze performance across experiments
* Aggregate results for reporting

Report Output Location
----------------------

Specify where to save the report:

.. code-block:: bash
   :caption: Save report to specific location

   streamflow report workflow-name --outdir ./reports

**Default:** Current directory

Custom Report Name
------------------

.. code-block:: bash
   :caption: Custom report filename

   streamflow report workflow-name \
     --outdir ./reports \
     --name experiment-2024-02-24

**Output:** ``./reports/experiment-2024-02-24.html``

Report Analysis
===============

Understanding HTML Reports
--------------------------

The interactive HTML report includes:

**1. Summary Section**

* Workflow name and execution time
* Total duration
* Number of steps
* Success/failure status
* Resource summary

**2. Timeline View**

* Visual representation of step execution
* Parallel execution visualization
* Data transfer operations
* Idle time identification

**3. Step Details**

* Individual step execution times
* Input/output files
* Execution location
* Resource usage
* Error messages (if failed)

**4. Resource Utilization**

* CPU usage over time
* Memory consumption
* Network I/O
* Disk I/O

**5. Performance Metrics**

* Critical path analysis
* Parallelization efficiency
* Resource utilization percentage
* Bottleneck identification

JSON Report Structure
---------------------

.. code-block:: json
   :caption: JSON report structure (example)

   {
     "workflow": {
       "name": "data-pipeline",
       "type": "cwl",
       "status": "COMPLETED",
       "start_time": "2024-02-24T13:10:45.456789+00:00",
       "end_time": "2024-02-24T13:55:30.345678+00:00",
       "duration": 2684.888889
     },
     "steps": [
       {
         "name": "/preprocess",
         "status": "COMPLETED",
         "start_time": "2024-02-24T13:11:00.123456+00:00",
         "end_time": "2024-02-24T13:15:30.234567+00:00",
         "duration": 270.111111,
         "location": "kubernetes-cluster/worker-pod-1"
       }
     ],
     "statistics": {
       "total_steps": 5,
       "completed_steps": 5,
       "failed_steps": 0,
       "total_duration": 2684.888889
     }
   }

Provenance Archives
===================

StreamFlow supports the `Workflow Run RO-Crate <https://www.researchobject.org/workflow-run-crate/>`_ provenance format for capturing complete workflow execution provenance.

Generate Provenance Archive
----------------------------

.. code-block:: bash
   :caption: Generate provenance archive

   streamflow prov workflow-name

**Output:** ``workflow-name.crate.zip``

**Archive Contents:**

* Workflow definition files
* Input/output data
* Execution metadata
* Provenance graph
* RO-Crate metadata (JSON-LD)

Archive All Executions
----------------------

Include entire execution history:

.. code-block:: bash
   :caption: Archive all executions

   streamflow prov workflow-name --all

Custom Archive Name
-------------------

.. code-block:: bash
   :caption: Custom archive name

   streamflow prov workflow-name --name experiment-2024

**Output:** ``experiment-2024.crate.zip``

Specify Output Directory
-------------------------

.. code-block:: bash
   :caption: Save archive to specific location

   streamflow prov workflow-name --outdir /path/to/archives

**Default:** Current directory

Provenance Use Cases
--------------------

1. **Reproducibility**
   
   * Share complete workflow execution
   * Enable exact reproduction
   * Document computational experiments

2. **Publication**
   
   * Supplement research papers
   * Meet FAIR data principles
   * Provide computational evidence

3. **Compliance**
   
   * Audit trail for regulated workflows
   * Record-keeping requirements
   * Quality assurance

4. **Debugging**
   
   * Comprehensive execution trace
   * Input/output inspection
   * Performance analysis

Workflow Outputs
================

Accessing Results
-----------------

Workflow outputs are stored in the specified output directory:

.. code-block:: bash
   :caption: Check output location

   # Default output directory (current)
   ls -la ./
   
   # Custom output directory
   ls -la /path/to/outdir

**Typical Output Structure:**

::

   outdir/
   ├── result_file_1.txt       # Workflow outputs
   ├── result_file_2.csv
   ├── results/                # Subdirectory outputs
   │   ├── analysis_1.png
   │   └── analysis_2.png
   └── .streamflow/            # StreamFlow metadata
       └── workflow.db

Output Files
------------

CWL workflows declare outputs explicitly:

.. code-block:: yaml
   :caption: CWL outputs

   outputs:
     results:
       type: File
       outputSource: analyze/result_file
     
     plots:
       type: Directory
       outputSource: visualize/plot_directory

StreamFlow ensures these outputs are transferred to the output directory.

Intermediate Files
------------------

By default, StreamFlow cleans up intermediate files. To preserve them:

.. code-block:: bash
   :caption: Keep intermediate files (planned feature)

   streamflow run workflow.yml --keep-intermediates

Database Inspection
===================

Direct Database Access
----------------------

The StreamFlow database is SQLite format:

.. code-block:: bash
   :caption: Inspect database directly

   sqlite3 ~/.streamflow/workflow.db

.. code-block:: sql
   :caption: Example queries

   -- List all workflows
   SELECT name, type FROM workflows;
   
   -- Get execution times
   SELECT name, start_time, end_time, status 
   FROM executions 
   WHERE workflow_name = 'my-workflow';
   
   -- Step execution details
   SELECT step_name, duration, status 
   FROM steps 
   WHERE execution_id = 'exec-123';

**Database Schema:**

* ``workflows`` - Workflow definitions
* ``executions`` - Execution instances
* ``steps`` - Individual step executions
* ``transfers`` - Data transfer operations
* ``deployments`` - Deployed environments

Programmatic Access
-------------------

For custom analysis, use Python with SQLite:

.. code-block:: python
   :caption: Analyze execution data

   import sqlite3
   import pandas as pd
   
   # Connect to database
   conn = sqlite3.connect(os.path.expanduser('~/.streamflow/workflow.db'))
   
   # Load execution data
   df = pd.read_sql_query("""
       SELECT 
           e.workflow_name,
           s.step_name,
           s.duration,
           s.location
       FROM executions e
       JOIN steps s ON e.id = s.execution_id
       WHERE e.status = 'COMPLETED'
   """, conn)
   
   # Analyze
   print(df.groupby('step_name')['duration'].describe())
   
   conn.close()

Performance Analysis
====================

Execution Time Analysis
-----------------------

Compare execution times across runs:

.. code-block:: bash
   :caption: Get execution times

   streamflow list my-workflow | awk '{print $2, $4}'

Identify slow steps:

1. Generate JSON report
2. Parse step durations
3. Identify bottlenecks

.. code-block:: bash
   :caption: Extract step durations

   streamflow report my-workflow --format json | \
     jq '.steps[] | {name: .name, duration: .duration}' | \
     jq -s 'sort_by(.duration) | reverse'

Resource Utilization
--------------------

HTML reports show:

* CPU utilization over time
* Memory consumption patterns
* I/O bandwidth usage
* Resource allocation efficiency

Parallelization Efficiency
--------------------------

Analyze how well workflow parallelizes:

**Ideal Parallelization:** Steps execute simultaneously on all available resources

**Poor Parallelization:** Sequential execution with idle resources

HTML reports visualize this through timeline view.

Troubleshooting with Reports
=============================

Failed Executions
-----------------

Inspect failed runs:

.. code-block:: bash
   :caption: List failed executions

   streamflow list workflow-name | grep FAILED

Generate report for the most recent execution (which may include failed runs):

.. code-block:: bash
   :caption: Report on recent run

   streamflow report workflow-name

**Report Shows:**

* Which step failed
* Error messages
* Input files at failure point
* Execution location
* Resource state

Performance Issues
------------------

If workflows run slowly:

1. **Generate HTML report**
2. **Check timeline** for:
   
   * Long-running steps
   * Sequential execution (should be parallel)
   * Excessive idle time
   * Large data transfers

3. **Optimize** based on findings:
   
   * Add parallelism (scatter)
   * Improve data locality
   * Adjust resource allocation
   * Use faster deployments

Best Practices
==============

1. **Regular Archiving**
   
   Archive important workflow executions:
   
   .. code-block:: bash
   
      # After successful production run
      streamflow prov production-workflow \
        --name production-2024-02-24 \
        --outdir /archive/provenance

2. **Comparative Analysis**
   
   Compare workflow versions:
   
   .. code-block:: bash
   
      streamflow report workflow-v1,workflow-v2,workflow-v3 \
        --format html \
        --outdir ./comparisons

3. **Automated Reporting**
   
   Generate reports automatically:
   
   .. code-block:: bash
   
      #!/bin/bash
      # post-workflow-script.sh
      WORKFLOW_NAME=$1
      streamflow report $WORKFLOW_NAME \
        --outdir ./reports/$(date +%Y-%m-%d)

4. **Database Backups**
   
   Backup metadata regularly:
   
   .. code-block:: bash
   
      # Backup StreamFlow database
      cp ~/.streamflow/workflow.db \
         ~/backups/streamflow-$(date +%Y%m%d).db

5. **Clean Old Data**
   
   Periodically remove old executions:
   
   .. code-block:: bash
   
      # Remove database (careful!)
      rm ~/.streamflow/workflow.db
      
      # Or use database tools to prune old records

Exporting Results
=================

Share with Collaborators
------------------------

.. code-block:: bash
   :caption: Package results for sharing

   # Create archive
   tar -czf results-package.tar.gz \
     results/ \
     reports/workflow-report.html \
     workflow-name.crate.zip

Integration with Analysis Tools
--------------------------------

Export data for external tools:

.. code-block:: bash
   :caption: Export for analysis

   # JSON for programmatic access
   streamflow report workflow-name --format json > results.json
   
   # Load in Python/R/etc. for analysis
   python analyze-results.py results.json

Next Steps
==========

After inspecting results:

* :doc:`troubleshooting` - Resolve issues found in reports
* :doc:`/reference/cli/report` - Complete report options
* :doc:`/reference/cli/prov` - Provenance command reference
* :doc:`advanced-patterns/index` - Optimize workflow performance

Related Topics
==============

* :doc:`running-workflows` - Workflow execution
* :doc:`/reference/cli/list` - List command reference
* :doc:`/developer-guide/core-interfaces/persistence` - Database internals
* `RO-Crate Specification <https://www.researchobject.org/ro-crate/>`_ - Provenance format
