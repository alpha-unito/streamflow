========
Glossary
========

.. meta::
   :keywords: StreamFlow, glossary, terms, definitions, terminology
   :description: Complete glossary of StreamFlow terminology and concepts

Overview
========

This glossary provides definitions for key terms and concepts used throughout StreamFlow documentation.

Terms
=====

.. glossary::

   Binding
      Associates a workflow step with a deployment target, specifying where the step should execute.

   Connector
      A StreamFlow component that interfaces with an execution environment (Docker, Kubernetes, HPC, etc.).

   CWL
      Common Workflow Language - a standard for describing computational workflows.

   Deployment
      An execution environment configured in StreamFlow where workflow tasks can run.

   Location
      The lowest level in StreamFlow's three-tier execution model (Deployment → Service → Location).

   Port
      An input or output interface of a workflow step that handles data flow.

   Scheduler
      Component responsible for assigning workflow tasks to available resources.

   Service
      The middle level in StreamFlow's execution model, representing a logical grouping of locations.

   Step
      An individual task or operation in a workflow.

   StreamFlowContext
      The central coordinator managing all StreamFlow components and their lifecycle.

   Target
      A deployment destination specified in a binding.

   Token
      A data unit flowing through workflow ports during execution.

   Workflow
      A computational process defined as a directed acyclic graph of steps.

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/quickstart` - Get started with StreamFlow
   - :doc:`/user-guide/writing-workflows` - Workflow concepts

**Reference:**
   - :doc:`/reference/index` - Complete reference documentation
