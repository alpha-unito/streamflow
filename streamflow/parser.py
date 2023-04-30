import argparse
import os

parser = argparse.ArgumentParser(description="StreamFlow Command Line")
subparsers = parser.add_subparsers(dest="context")

# streamflow list
list_parser = subparsers.add_parser("list", help="List the executed workflows")
list_parser.add_argument(
    "--file",
    "-f",
    default="streamflow.yml",
    type=str,
    help="Path to the StreamFlow file describing the workflow execution",
)
list_parser.add_argument(
    "name",
    metavar="NAME",
    nargs="?",
    type=str,
    help="List all executions for the given workflow",
)

# streamflow plugin
plugin_parser = subparsers.add_parser(
    "plugin", help="Retrieve information on the installed StreamFlow plugins"
)
plugin_subparsers = plugin_parser.add_subparsers(dest="plugin_context")

# streamflow plugin list
plugin_list_parser = plugin_subparsers.add_parser(
    "list", help="List the installed StreamFlow plugins"
)

# streamflow plugin show
plugin_show_parser = plugin_subparsers.add_parser(
    "show", help="Show the details of a StreamFlow plugin"
)
plugin_show_parser.add_argument(
    "plugin", metavar="PLUGIN", type=str, help="Name of the plugin to show"
)
plugin_show_parser.add_argument(
    "--name", "-n", type=str, help="Filter extensions by name"
)
plugin_show_parser.add_argument(
    "--type",
    "-t",
    type=str,
    choices=[
        "binding_filter",
        "checkpoint_manager",
        "cwl_docker_translator",
        "connector",
        "data_manager",
        "database",
        "deployment_manager",
        "failure_manager",
        "policy",
        "scheduler",
    ],
    help="Filter extensions by type",
)
plugin_show_parser.add_argument(
    "--show-schema",
    action="store_true",
    help="Print property schemas for selected extension points",
)

# streamflow prov
prov_parser = subparsers.add_parser(
    "prov", help="Generate a provenance archive for an executed workflow"
)
prov_parser.add_argument(
    "workflow", metavar="WORKFLOW", type=str, help="Name of the workflow to process"
)
prov_parser.add_argument(
    "--all",
    "-a",
    action="store_true",
    help="If true, include all executions of the selected worwflow. "
    "If false, include just the last one. (default: false)",
)
prov_parser.add_argument(
    "--file",
    "-f",
    default="streamflow.yml",
    type=str,
    help="Path to the StreamFlow file describing the workflow execution",
)
prov_parser.add_argument(
    "--name",
    type=str,
    help="The name of the generated archive (default: workflow_name.crate.zip)",
)
prov_parser.add_argument(
    "--outdir",
    default=os.getcwd(),
    type=str,
    help="Where the archive should be created (default: current directory)",
)
prov_parser.add_argument(
    "--type",
    "-t",
    default="run_crate",
    type=str,
    choices=["run_crate"],
    help="The type of provenance archive to generate (default: run_crate)",
)

# streamflow report
report_parser = subparsers.add_parser(
    "report", help="Generate a report for an executed workflow"
)
report_parser.add_argument(
    "workflow", metavar="WORKFLOW", type=str, help="Name of the workflow to process"
)
report_parser.add_argument(
    "--all",
    "-a",
    action="store_true",
    help="If true, include all executions of the selected worwflow. "
    "If false, include just the last one. (default: false)",
)
report_parser.add_argument(
    "--file",
    "-f",
    default="streamflow.yml",
    type=str,
    help="Path to the StreamFlow file describing the workflow execution",
)
report_parser.add_argument(
    "--format",
    default=["html"],
    nargs="*",
    type=str,
    choices=["html", "pdf", "eps", "png", "jpg", "webp", "svg", "csv", "json"],
    help="Report format: (default: html)",
)
report_parser.add_argument(
    "--group-by-step",
    action="store_true",
    help="Groups execution of multiple instances of the same step on a single line",
)
report_parser.add_argument(
    "--name", type=str, help="Name of the report folder (default '${WORKFLOW}-report')"
)

# streamflow run
run_parser = subparsers.add_parser("run", help="Execute a workflow")
run_parser.add_argument(
    "streamflow_file",
    metavar="STREAMFLOW_FILE",
    default="streamflow.yml",
    type=str,
    help="Path to the StreamFlow file describing the workflow execution",
)
run_parser.add_argument(
    "--color",
    action="store_true",
    help="Prints log preamble with colors related to the logging level",
)
run_parser.add_argument(
    "--debug", action="store_true", help="Prints debug-level diagnostic output"
)
run_parser.add_argument(
    "--name",
    nargs="?",
    type=str,
    help="Name of the current workflow. Will be used for search and indexing",
)
run_parser.add_argument(
    "--outdir",
    default=os.getcwd(),
    type=str,
    help="Output directory in which to store final results of the workflow (default: current directory)",
)
run_parser.add_argument(
    "--quiet", action="store_true", help="Only prints results, warnings and errors"
)


# streamflow version
version_parser = subparsers.add_parser(
    "version", help="Only print StreamFlow version and exit"
)
