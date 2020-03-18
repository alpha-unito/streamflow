#!/usr/bin/env cwl-runner
cwlVersion: v1.1
class: Workflow
$namespaces:
  sf: "https://streamflow.org/cwl#"
doc: |
  This is a workflow for processing and composition of astronomical images through Munipack software 
  (http://munipack.physics.muni.cz/). It illustrates a simple stacking process expressed as a workflow and 
  takes, as a model, the explanations given by the reported resource (http://munipack.physics.muni.cz/guide.html).
  It uses Blazar S5 0716+714 data example from: ftp://munipack.physics.muni.cz/pub/munipack/munipack-data-blazar.tar.gz
  The workflow takes as input Blazar S5 0716+714 frames and correction frames for dark and flat-field and 
  produce as output a single frame that is the result of the correction and stacking processes. You can use the download_data.sh
  script to download the required data.
  A short explanation of each step follows.

requirements:
  ScatterFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}
  MultipleInputFeatureRequirement: {}
  InlineJavascriptRequirement: {}

inputs:
  main_d: Directory
  worker_n: int

  dark_file_n: string
  dark_flat_n: string
  flat_file_n: string
  dark_frames_d: Directory
  dark_flat_frames_d: Directory
  flat_frames_d: Directory
  
  fwmh_par: int
  right_ascension: float
  declination: float
  degree: float

 
outputs:
  result:
    type: File
    outputSource: stacking/final_frame

steps:
  distribute_files:
    run: master.cwl
    doc: |
      This step takes as inputs the directory which contains the subject frames (0716_*R.fits files). It
      distributes equally among directories (whose desidered number is worker_n). Its output is an array of 
      worker_n Directories, each of them containing a certain number of the subject frames and each of them 
      becoming an input directory for the worker node. 
    in:
      main_dir: main_d
      worker_number: worker_n
    out: [partitioned_dirs]

##############################################################

  pre_correction:
    run: pre_correction.cwl
    doc: |
      This step takes as input the desidered files name for the correction frames (dark frame 
      and flat-field frame). For the creation of the average dark frame is invoked the dark module 
      that takes the average of images specified as input: dark frames named d120_*.fits in data/dark120
      directory. For the creation of the flat-field frame: first it does the same operation of dark but on dark 
      frames named d10_*.fits, in data/dark10 directory (this is the average dark frame of the flat-field),
      then the flat module is invoked; this module takes the average of flat images specified as input (the data
      in data/flat directory).

    in:
      dark_file_name: dark_file_n
      dark_flat_name: dark_flat_n
      flat_file_name: flat_file_n
      dark_frames_dir: dark_frames_d
      dark_flat_frames_dir: dark_flat_frames_d
      flat_frames_dir: flat_frames_d
    out: [average_dark_frame, master_flat_frame]

##############################################################

  photo_correction:
    run: clt/photo_correction.cwl
    doc: |
      This step corrects the original scientific images for dark-frames and for flats by running phcorr action, 
      that subtracts the previously created dark frame and flat-field frame from every scientific exposures of
      the subject.
    scatter: to_correct
    in:
      to_correct: distribute_files/partitioned_dirs
      dark_frame: pre_correction/average_dark_frame
      master_flat_frame: pre_correction/master_flat_frame
    out: [corrected]

##############################################################

  flattening:
    run: clt/flattening.cwl
    doc: |
      This step is an Expression Tool for flattening the results of the previous step (photo correction).
      It takes as input an array of array of File and produce an array of File (the flattened version of the input 
      array of array)
    in:
      inputArray: photo_correction/corrected 
    out: [flattenedArray]

##############################################################
  
  stacking:
    run: stack.cwl
    doc: |
      This step combines the corrected frames to get one unique frame after a stacking process.
      The first step of this subworkflow is a cone search of astronomical catalogue provided by 
      Virtual Observatory. It takes as input the right ascension and the declination of an object
      and the search radius in degree. It gives in output the cone.fits file, that contains such information. 
      Then a new Header Data Units is added to the fits files by running the find operation to find stars. 
      It takes as input the previously corrected (for dark and flat) frames. Follow the aperture_photometry 
      step, which run the aphot action to add the aperture photometry table to the fits files (that already 
      have the FIND HDU added). The subject frames (now with FIND and APHOT HDU added) and the cone.fits file
      go to the input of next step, that derives astrometry calibration of FITS frames.  
      After all these steps are terminated, the corrected original frames of blazar with FIND and APHOT extension and
      with astrometry calibration are stacked all togheter with the kombine action to produce a unique summing frame.
    in:
      corrected_frames: flattening/flattenedArray
      fwmh: fwmh_par 
      ra: right_ascension
      dec: declination
      deg: degree
    out: [final_frame]

###############################################################