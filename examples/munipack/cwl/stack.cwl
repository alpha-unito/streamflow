#!/usr/bin/env cwl-runner
cwlVersion: v1.1
class: Workflow
$namespaces:
  sf: "https://streamflow.org/cwl#"

requirements:
  ScatterFeatureRequirement: {}

inputs:
  corrected_frames: File[]
  fwmh: int
  ra: float
  dec: float
  deg: float

outputs: 
  final_frame:
    type: File
    outputSource: combine/processed_frame

steps:
  cone_search:
    run: clt/cone_search.cwl
    in: 
      ra: ra
      dec: dec
      deg: deg
    out: [conefile]

#######################################################
  
  find_star:
    run: clt/find_star.cwl
    in:
      frames: corrected_frames
      fwmh: fwmh
    out: [find_frames]

#######################################################

  aperture_photometry:
    run: clt/aperture_photometry.cwl
    in: 
      find_frames: find_star/find_frames
    out: [aphot_frames]


 #######################################################

  astrometry:
    run: clt/astrometry.cwl
    scatter: aphot_frame
    in: 
      aphot_frame: aperture_photometry/aphot_frames
      conefile: cone_search/conefile
    out: [astrometry_frame]

 #######################################################

  combine:
    run: clt/combine.cwl
    in: 
      stack_frames: astrometry/astrometry_frame
    out: [processed_frame]