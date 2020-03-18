cwlVersion: v1.1
label: Correction frames, dark and flat-field
class: Workflow

inputs:
  dark_file_name: string
  dark_flat_name: string
  flat_file_name: string
  dark_frames_dir: Directory
  dark_flat_frames_dir: Directory
  flat_frames_dir: Directory

outputs:
  average_dark_frame:
    type: File
    outputSource: dark/average_dark
  master_flat_frame:
    type: File
    outputSource: flat/master_flat

steps:
  dark:
    run: clt/dark.cwl
    in:
      dark_name: dark_file_name
      dark_dir: dark_frames_dir
    out: [average_dark]

#######################################################

  flat_dark:
    run: clt/dark.cwl
    in:
      dark_name: dark_flat_name
      dark_dir: dark_flat_frames_dir
    out: [average_dark]

#######################################################

  flat:
    run: clt/flat.cwl
    in:
      flat_name: flat_file_name
      dark_frame: flat_dark/average_dark
      flat_dir: flat_frames_dir
    out: [master_flat]
  