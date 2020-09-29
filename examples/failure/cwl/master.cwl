#!/usr/bin/env cwl-runner
cwlVersion: v1.1
class: Workflow
$namespaces:
  sf: "https://streamflow.org/cwl#"

inputs:
  main_dir: Directory
  worker_number: int

outputs:
  partitioned_dirs:
    type: Directory[]
    outputSource: scattering/directories

steps:
  find_files:
    run: clt/find.cwl
    in:
      main_directory: main_dir
    out: [ files_array ]

  #######################################################

  scattering:
    run: clt/scatter.cwl
    in:
      worker: worker_number
      files_array: find_files/files_array
    out: [ directories ]
