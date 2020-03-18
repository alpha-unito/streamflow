cwlVersion: v1.1
class: CommandLineTool
baseCommand: ["munipack", "aphot", "-O", "--mask"]
arguments:
  - position: 1
    valueFrom: 'aphot_\\0'

inputs:
  find_frames: 
    type: File[]
    inputBinding:
      position: 2

outputs: 
  aphot_frames:
    type: File[]
    outputBinding:
      glob: "aphot_*.fits"
