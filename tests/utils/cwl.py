from __future__ import annotations

from types import ModuleType

import cwl_utils.parser.cwl_v1_0
import cwl_utils.parser.cwl_v1_1
import cwl_utils.parser.cwl_v1_2


def get_cwl_parser(version: str) -> ModuleType:
    match version:
        case "v1.0":
            return cwl_utils.parser.cwl_v1_0
        case "v1.1":
            return cwl_utils.parser.cwl_v1_1
        case "v1.2":
            return cwl_utils.parser.cwl_v1_1
        case _:
            raise ValueError(f"Unsupported CWL version {version}")
