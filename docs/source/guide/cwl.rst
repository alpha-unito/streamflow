===================
Write your workflow
===================

StreamFlow relies on the `Common Workflow Language <https://www.commonwl.org/>`_ (CWL) standard to describe workflows. In particular, it supports version ``v1.2`` of the standard, which introduces conditional execution of workflow steps.

The reader is referred to the `official CWL documentation <https://www.commonwl.org/v1.2/>`_ to learn how the workflow description language works, as StreamFlow does not introduce any modification to the original specification.

.. note::
  StreamFlow supports all the features required by the CWL standard conformance, and nearly all optional features, for versions ``v1.0``, ``v1.1``, and ``v1.2``. For a complete overview of CWL conformance status, look :ref:`here <CWL Conformance>`.

The following snippet contain a simple example of CWL workflow, which extracts a Java source file from a tar archive and compiles it.

.. code-block:: yaml

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