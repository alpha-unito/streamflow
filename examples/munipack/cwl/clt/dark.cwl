cwlVersion: v1.1
id: dark
label: average dark frame
class: CommandLineTool
baseCommand: ["munipack", "dark"]
arguments:
  - position: 1
    valueFrom: './$(inputs.dark_name)'
    prefix: -o 
  - position: 2
    valueFrom: '$(inputs.dark_dir.listing)'

inputs:
  dark_name: string
  dark_dir:
    type: Directory
    label: collection of dark frames
    loadListing: shallow_listing

outputs: 
  average_dark:
    type: File
    label: average dark frame
    outputBinding:
      glob: '$(inputs.dark_name)'