cwlVersion: v1.1
class: CommandLineTool
baseCommand: ["mpirun"]
arguments:
  - position: 2
    valueFrom: '$STREAMFLOW_HOSTS'
    prefix: '--host'
  - position: 3
    valueFrom: '--allow-run-as-root'
stdout: mpi_output.log
inputs:
  num_processes:
    type: int
    inputBinding:
      position: 1
      prefix: '-np'
  executable_file:
    type: File
    inputBinding:
      position: 4

outputs:
  mpi_output:
    type: stdout