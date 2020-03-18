cwlVersion: v1.1
id: flat
label: master flat frame
class: CommandLineTool
baseCommand: ["munipack", "flat"]

requirements:
  InitialWorkDirRequirement:
    listing:          
    - $(inputs.dark_frame)

inputs:
  flat_name: 
    type: string
    inputBinding:
      position: 1
      prefix: -o
  
  dark_frame:
    type: File
    inputBinding:
      position: 2
      prefix: -dark

  flat_dir:
    type: Directory
    inputBinding:
      position: 3
      valueFrom: '$(inputs.flat_dir.listing)'
    loadListing: shallow_listing
    label: collection of flat frames

outputs: 
  master_flat:
    type: File
    label: master flat frame
    outputBinding:
      glob: '$(inputs.flat_name)'