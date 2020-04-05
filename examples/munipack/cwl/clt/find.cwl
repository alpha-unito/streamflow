cwlVersion: v1.1
class: CommandLineTool
baseCommand: ["ls", "-la"]

inputs:
  main_directory:
    type: Directory
    loadListing: shallow_listing
    inputBinding:
      position: 1

outputs:
  files_array:
    type: File[]
    outputBinding:
      outputEval: '$(inputs.main_directory.listing)'

