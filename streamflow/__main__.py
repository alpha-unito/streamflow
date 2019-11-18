"""Default entrypoint for the streamflow module."""
import sys

from streamflow import main

main.main(sys.argv[1:])
