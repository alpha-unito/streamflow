import sys

from streamflow.config.config import WorkflowConfig
from streamflow.config.validator import SfValidator
from streamflow.cwl.main import main as cwl_main


def main(args):
    streamflow_file = args[0]
    streamflow_config = SfValidator().validate(streamflow_file)
    for workflow in streamflow_config.get('workflows', {}):
        workflow_config = WorkflowConfig(workflow, streamflow_config)
        if workflow_config.type == 'cwl':
            cwl_main(workflow_config)


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
