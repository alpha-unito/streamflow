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
baseCommand: [ "sum-exe", "open", "zero", "0" ]
arguments:
  - position: 1
    valueFrom: '$(inputs.num_file.path)'

requirements:
  ToolTimeLimit:
    timelimit: 5

inputs:
  num_file: File

outputs:
  nums:
    type: File[]
    outputBinding:
      glob: "nums*.txt"
