import cwl_utils.parser.cwl_v1_0
import cwl_utils.parser.cwl_v1_1
import cwl_utils.parser.cwl_v1_2


def get_cwl_parser(version: str):
    if version == "v1.0":
        return cwl_utils.parser.cwl_v1_0
    elif version == "v1.1":
        return cwl_utils.parser.cwl_v1_1
    elif version == "v1.2":
        return cwl_utils.parser.cwl_v1_1
    else:
        raise ValueError(f"Unsupported CWL version {version}")
