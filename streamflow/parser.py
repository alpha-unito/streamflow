import argparse
import os
import re

import streamflow.ext.plugin

UNESCAPED_COMMA = re.compile(r"(?<!\\),")
UNESCAPED_EQUAL = re.compile(r"(?<!\\)=")


class _KeyValueAction(argparse.Action):
    def __call__(self, _parser, namespace, values, option_string=None):
        items = getattr(namespace, self.dest) or []
        items.append(
            {
                k: v
                for k, v in (
                    UNESCAPED_EQUAL.split(val, maxsplit=1)
                    for val in UNESCAPED_COMMA.split(values)
                )
            }
        )
        setattr(namespace, self.dest, items)


parser = argparse.ArgumentParser(description="StreamFlow Command Line")
subparsers = parser.add_subparsers(dest="context")


# streamflow ext
ext_parser = subparsers.add_parser(
    "ext", help="Retrieve information on the available StreamFlow extensions"
)
ext_subparsers = ext_parser.add_subparsers(dest="ext_context")

# streamflow ext list
ext_list_parser = ext_subparsers.add_parser(
    "list", help="List the available StreamFlow extensions"
)
ext_list_parser.add_argument("--name", "-n", type=str, help="Filter extensions by name")
ext_list_parser.add_argument(
    "--type",
    "-t",
    type=str,
    choices=streamflow.ext.plugin.extension_points,
    help="Filter extensions by type",
)

# streamflow ext show
ext_show_parser = ext_subparsers.add_parser(
    "show", help="Show the details of a StreamFlow extension"
)
ext_show_parser.add_argument(
    "type",
    metavar="TYPE",
    type=str,
    choices=streamflow.ext.plugin.extension_points,
    help="Type of the extension to show",
)
ext_show_parser.add_argument(
    "name", metavar="NAME", type=str, help="Name of the extension to show"
)

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

# streamflow prov
prov_parser = subparsers.add_parser(
    "prov", help="Generate a provenance archive for an executed workflow"
)
prov_parser.register("action", "key_value", _KeyValueAction)
prov_parser.add_argument(
    "workflow", metavar="WORKFLOW", type=str, help="Name of the workflow to process"
)
prov_parser.add_argument(
    "--add-file",
    action="key_value",
    help="Add an external file to the provenance archive. File properties are specified as comma-separated "
    "key-value pairs (key1=value1,key2=value2). A `src` property (mandatory) specifies where the file is "
    "located. A `dst` property (default: /) contains a POSIX path specifying where the file should be placed "
    "in the archive file system (the root folder '/' corresponds to the root of the archive). Additional "
    'properties can be specified as strings or JSON objects (e.g., about={\\"@id\\":\\"./\\"}) and their meaning '
    "depends on the selected provenance type.",
)
prov_parser.add_argument(
    "--add-property",
    action="key_value",
    help="Add a property to the archive manifest (if present). Properties are specified as comma-separated "
    "key-value pairs (key1=value1,key2=value2) and can be specified as strings or JSON objects (e.g., "
    '\\./.license={\\"@id\\":\\"LICENSE\\"}). The way in which property keys map to manifest objects '
    "depends on the selected provenance type.",
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
