cwlVersion: v1.1
class: CommandLineTool

# run script: sum-exe
# pars:
#   fst must be a operation: {open, sum, comb}
#   snd must be a error: {zero, loop}
#     with zero will throw a divion by zero exception
#     with loop will enter in a infinity loop
#   trd must be: a integer [0, 100]
#     indicate the probability that will throw the error
#   following: all file paths necessary
baseCommand: [ "sum-exe", "sum", "zero", "0" ]

requirements:
  ToolTimeLimit:
    timelimit: 5

inputs:
  num_files:
    type: File[]
    inputBinding:
      position: 1

  extra_file:
    type: Directory
    loadListing: shallow_listing
    inputBinding:
      position: 2
      valueFrom: "$(inputs.extra_file.listing)"

outputs:
  sum_file:
    type: File
    outputBinding:
      glob: "sum*.txt"
