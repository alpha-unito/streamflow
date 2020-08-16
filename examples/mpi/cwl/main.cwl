#!/usr/bin/env cwl-runner
cwlVersion: v1.1
class: Workflow
$namespaces:
  sf: "https://streamflow.org/cwl#"
doc: |
  This is a workflow for processing and composition of astronomical images through Munipack software
  (http://munipack.physics.muni.cz/). It illustrates a simple stacking process expressed as a workflow and
  takes, as a model, the explanations given by the reported resource (http://munipack.physics.muni.cz/guide.html).
  It uses Blazar S5 0716+714 data example from: ftp://munipack.physics.muni.cz/pub/munipack/munipack-data-blazar.tar.gz
  The workflow takes as input Blazar S5 0716+714 frames and correction frames for dark and flat-field and
  produce as output a single frame that is the result of the correction and stacking processes. You can use the download_data.sh
  script to download the required data.
  A short explanation of each step follows.

inputs:
  num_processes: int
  source_file: File

outputs: []

steps:
  compile:
    run: clt/compile.cwl
    doc: |
      This step takes as input a C source file and compiles it with the MPI compiler. Its output is an executable
      linked with a proper MPI implementation.
    in:
      source_file: source_file
    out: [executable_file]

##############################################################

  execute:
    run: clt/execute.cwl
    doc: |
      This step takes as input the MPI executable file and some mpirun configurations. It runs the MPI executable on
      multiple nodes with the aid of StreamFlow's `replicas` setting in the streamflow.yml file.
    in:
      executable_file: compile/executable_file
      num_processes: num_processes
    out: []
