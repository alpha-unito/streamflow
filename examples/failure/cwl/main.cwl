#!/usr/bin/env cwl-runner
cwlVersion: v1.1
class: Workflow
$namespaces:
  sf: "https://streamflow.org/cwl#"

requirements:
  ScatterFeatureRequirement: { }
  SubworkflowFeatureRequirement: { }

inputs:
  num_file: File
  main_d: Directory
  worker_n: int

outputs:
  result:
    type: File
    outputSource: comb_step/comb_file

steps:
  distribute_files:
    run: master.cwl
    doc: |
      This step takes as inputs the directory which contains the text files with numbers.
      It distributes equally among directories (whose desidered number is worker_n).
      Its output is an array of worker_n Directories, each of them containing a certain
      number of the text files and each of them becoming an input directory for the worker node.
    in:
      main_dir: main_d
      worker_number: worker_n
    out: [ partitioned_dirs ]

  ##############################################################

  open_step:
    run: clt/openit.cwl
    doc: |
      This step takes as input a file that inside there are integers.
      Its output is an array of files, every files contains the same number of integers take
      from original file, the last file may have padding
    in:
      num_file: num_file
    out:
      [ nums ]

  ##############################################################

  sum_step:
    run: clt/sumit.cwl
    doc: |
      This step takes as input all files from step open_step and that given to it from step distribute_files.
      Its output is a file with the sum of all numbers of his input files
    scatter: extra_file
    in:
      num_files: open_step/nums
      extra_file: distribute_files/partitioned_dirs
    out:
      [ sum_file ]

  ##############################################################

  comb_step:
    run: clt/combit.cwl
    doc: |
      This step takes as input all files from step sum_step.
      Its output is a only file that contains in every row the number inside the input files
    in:
      files: sum_step/sum_file
    out:
      [ comb_file ]
