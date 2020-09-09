#!/usr/bin/env cwl-runner
cwlVersion: v1.1
class: Workflow
$namespaces:
  sf: "https://streamflow.org/cwl#"

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
