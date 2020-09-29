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
baseCommand: [ "sum-exe", "comb", "zero", "80" ]

requirements:
  ToolTimeLimit:
    timelimit: 5

inputs:
  files:
    type: File[]
    inputBinding:
      position: 1

outputs:
  comb_file:
    type: File
    outputBinding:
      glob: "comb.txt"
