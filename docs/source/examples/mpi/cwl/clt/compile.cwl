cwlVersion: v1.1
class: CommandLineTool
baseCommand: ["mpicxx"]
arguments:
  - position: 1
    valueFrom: '-O3'
  - position: 2
    valueFrom: '$(inputs.source_file.nameroot)'
    prefix: '-o'

inputs:
  source_file:
    type: File
    inputBinding:
      position: 3

outputs:
  executable_file:
    type: File
    outputBinding:
      glob: '$(inputs.source_file.nameroot)'