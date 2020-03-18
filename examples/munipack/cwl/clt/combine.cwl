cwlVersion: v1.1
class: CommandLineTool
baseCommand: ["munipack", "kombine"]
arguments:
  - position: 1
    valueFrom: "./processed.fits"
    prefix: -o 

inputs:
  stack_frames: 
    type: File[]
    inputBinding:
      position: 2

outputs:
  processed_frame:
    type: File
    outputBinding:
      glob: "processed.fits"