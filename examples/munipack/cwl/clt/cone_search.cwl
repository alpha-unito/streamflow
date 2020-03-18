cwlVersion: v1.1
class: CommandLineTool
baseCommand: ["munipack", "cone"]
arguments:
  - position: 1
    prefix: -r 
    valueFrom: '$(inputs.deg)'
  - position: 2 
    valueFrom: '$(inputs.ra)'
  - position: 3
    valueFrom: '$(inputs.dec)'

inputs: 
  ra: float
  dec: float
  deg: float

outputs:
  conefile:
    type: File
    outputBinding:
     glob: "cone.fits"
