import argparse
import os

parser = argparse.ArgumentParser(description='StreamFlow Command Line')
subparsers = parser.add_subparsers(dest='context')

# streamflow run
run_parser = subparsers.add_parser('run')
run_parser.add_argument('streamflow_file', nargs='?', metavar='STREAMFLOW_FILE', type=str,
                        help='Path to the StreamFlow file describing the workflow execution.')
run_parser.add_argument('--outdir', default=os.getcwd(), type=str,
                        help='Output directory in which to store final results of the workflow.')
run_parser.add_argument('--quiet', action='store_true',
                        help='Only prints results, warnings and errors.')

# streamflow report
report_parser = subparsers.add_parser('report')
report_parser.add_argument('--group-by-step', action='store_true',
                           help='Groups execution of the same steps on a single line.')
report_parser.add_argument('--format', default='html', type=str,
                           help='Report format: html (default), pdf, eps, png, jpg, webp, svg, csv, or json.')
report_parser.add_argument('--name', type=str,
                           help='Name of the report file (default report.format).')
report_parser.add_argument('--outdir', default=os.getcwd(), type=str,
                           help='Output directory in which the final results of the workflow are stored.')

# streamflow version
version_parser = subparsers.add_parser('version', help='Only print StreamFlow version and exit.')
