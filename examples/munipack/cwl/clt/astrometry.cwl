cwlVersion: v1.1
class: CommandLineTool
baseCommand: ["munipack", "astrometry", "-O", "--mask"]
arguments:
  - position: 1
    valueFrom: 'astro_\\0'

requirements:
  InitialWorkDirRequirement:
    listing:          
    - '$(inputs.conefile)'

inputs:
  conefile: File

  aphot_frame: 
    type: File
    inputBinding:
      position: 2

outputs:
  astrometry_frame:
    type: File
    outputBinding:
      glob: "astro_$(inputs.aphot_frame.nameroot).fits"