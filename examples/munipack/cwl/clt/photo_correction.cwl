cwlVersion: v1.1
class: CommandLineTool
baseCommand: ["munipack", "phcorr", "-O", "--mask"]
arguments:
  - position: 1
    valueFrom: 'proc_\\0'

inputs:
  dark_frame:
    type: File
    inputBinding:
      position: 2
      prefix: -dark
      separate: true

  master_flat_frame:
    type: File
    inputBinding:
      position: 3
      prefix: -flat
      separate: true

  to_correct:
    type: Directory
    loadListing: shallow_listing
    inputBinding:
      position: 4
      valueFrom: "$(inputs.to_correct.listing)"

outputs:
  corrected:
    type: File[]
    outputBinding:
      glob: "proc*.fits"