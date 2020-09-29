import argparse
import os

parser = argparse.ArgumentParser(description='StreamFlow Command Line')
parser.add_argument('streamflow_file', nargs='?', metavar='STREAMFLOW_FILE',
                    help='Path to the StreamFlow file describing the workflow execution')
parser.add_argument('--outdir', default=os.getcwd(), type=str,
                    help='Output directory in which to store final results of the workflow')
parser.add_argument('--quiet', action='store_true',
                    help='Only prints results, warnings and errors')
parser.add_argument('--version', action='store_true',
                    help='If present, only print StreamFlow version and exit')
