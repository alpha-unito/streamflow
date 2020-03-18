cwlVersion: v1.1
class: CommandLineTool
baseCommand: ["munipack", "find", "-O", "--mask"]
arguments:
  - position: 1
    valueFrom: 'find_\\0'

inputs:
  frames: 
    type: File[]
    inputBinding:
      position: 2
  fwmh: int

outputs:
  find_frames:
    type: File[]
    outputBinding:
      glob: "find_*.fits"