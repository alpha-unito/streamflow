cwlVersion: v1.1
class: CommandLineTool
requirements:
  ShellCommandRequirement: {}
baseCommand: ["flux", "run"]
stdout: mpi_output.log
inputs:
  num_processes:
    type: int
    inputBinding:
      position: 1
      prefix: '-n'
  executable_file:
    type: File
    inputBinding:
      position: 4

outputs:
  mpi_output:
    type: stdout
