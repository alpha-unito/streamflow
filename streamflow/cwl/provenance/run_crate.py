from __future__ import annotations

import asyncio
import os
import posixpath
import uuid
from collections.abc import MutableMapping, MutableSequence
from typing import Any, cast, get_args

import cwl_utils.parser

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowProvenanceException,
)
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.utils import flatten_list
from streamflow.core.workflow import Job, Token, Workflow
from streamflow.cwl.command import CWLCommand
from streamflow.cwl.utils import (
    build_context,
    eval_expression,
    get_name,
    process_embedded_tool,
)
from streamflow.provenance.run_crate import (
    RunCrateProvenanceManager,
    checksum,
    file_checksum,
)
from streamflow.workflow.step import ExecuteStep
from streamflow.workflow.token import FileToken, ListToken, ObjectToken
from streamflow.workflow.utils import get_token_value


def _get_cwl_entity_id(entity_id: str) -> str:
    tokens = entity_id.split("#")
    if len(tokens) > 1:
        return "#".join([tokens[0].split("/")[-1], tokens[1]])
    else:
        return tokens[0].split("/")[-1]


def _get_cwl_param(
    cwl_param: (
        cwl_utils.parser.CommandInputParameter
        | cwl_utils.parser.WorkflowInputParameter
        | cwl_utils.parser.CommandOutputParameter
        | cwl_utils.parser.ExpressionToolOutputParameter
        | cwl_utils.parser.WorkflowOutputParameter
    ),
) -> MutableMapping[str, Any]:
    name = "#".join(cwl_param.id.split("#")[1:])
    jsonld_param = {
        "@id": _get_cwl_entity_id(cwl_param.id),
        "@type": "FormalParameter",
        "additionalType": [],
        "name": name,
        "conformsTo": "https://bioschemas.org/profiles/FormalParameter/1.0-RELEASE/",
    }
    if cwl_param.doc is not None:
        jsonld_param["description"] = cwl_param.doc
    if cwl_param.format is not None:
        jsonld_param["encodingFormat"] = cwl_param.format
    _process_cwl_type(cwl_param.type_, jsonld_param, cwl_param)
    if len(jsonld_param["additionalType"]) == 1:
        jsonld_param["additionalType"] = jsonld_param["additionalType"][0]
    return jsonld_param


def _get_cwl_programming_language(
    version: str,
) -> MutableMapping[str, Any]:
    return {
        "@id": "https://w3id.org/workflowhub/workflow-ro-crate#cwl",
        "@type": "ComputerLanguage",
        "alternateName": "CWL",
        "identifier": {"@id": f"https://w3id.org/cwl/{version}/"},
        "name": "Common Workflow Language",
        "url": {"@id": "https://www.commonwl.org/"},
        "version": version,
    }


def _get_workflow_template(entity_id: str, name: str) -> MutableMapping[str, Any]:
    return {
        "@id": entity_id,
        "@type": ["SoftwareSourceCode", "ComputationalWorkflow", "HowTo"],
        "name": name,
        "input": [],
        "output": [],
        "conformsTo": "https://bioschemas.org/profiles/ComputationalWorkflow/1.0-RELEASE/",
    }


def _process_cwl_type(
    cwl_type: (
        str
        | MutableSequence[Any]
        | cwl_utils.parser.ArraySchema
        | cwl_utils.parser.EnumSchema
        | cwl_utils.parser.RecordSchema
    ),
    jsonld_param: MutableMapping[str, Any],
    cwl_param: (
        cwl_utils.parser.CommandInputParameter
        | cwl_utils.parser.WorkflowInputParameter
        | cwl_utils.parser.CommandOutputParameter
        | cwl_utils.parser.ExpressionToolOutputParameter
        | cwl_utils.parser.WorkflowOutputParameter
    ),
) -> None:
    if isinstance(cwl_type, str):
        if cwl_type == "boolean":
            jsonld_param["additionalType"].append("Boolean")
        elif cwl_type in ["int", "long"]:
            jsonld_param["additionalType"].append("Integer")
        elif cwl_type in ["double", "float"]:
            jsonld_param["additionalType"].append("Float")
        elif cwl_type == "string":
            jsonld_param["additionalType"].append("Text")
        elif cwl_type == "File":
            if cast(cwl_utils.parser.File, cwl_param).secondaryFiles:
                jsonld_param["additionalType"].append("Collection")
            else:
                jsonld_param["additionalType"].append("File")
        elif cwl_type == "Directory":
            jsonld_param["additionalType"].append("Dataset")
        elif cwl_type == "Any":
            jsonld_param["additionalType"].append("DataType")
    elif isinstance(cwl_type, MutableSequence):
        for t in cwl_type:
            if t == "null":
                jsonld_param["valueRequired"] = "False"
            else:
                _process_cwl_type(t, jsonld_param, cwl_param)
    elif isinstance(cwl_type, get_args(cwl_utils.parser.ArraySchema)):
        jsonld_param["multipleValues"] = "True"
        _process_cwl_type(cwl_type.items, jsonld_param, cwl_param)
    elif isinstance(cwl_type, get_args(cwl_utils.parser.EnumSchema)):
        jsonld_param["additionalType"] = "Text"
        jsonld_param["valuePattern"] = "|".join(cwl_type.symbols)
    elif isinstance(cwl_type, get_args(cwl_utils.parser.RecordSchema)):
        jsonld_param["additionalType"] = "PropertyValue"
        jsonld_param["multipleValues"] = "True"


class CWLRunCrateProvenanceManager(RunCrateProvenanceManager):
    def __init__(
        self,
        context: StreamFlowContext,
        db_context: DatabaseLoadingContext,
        workflows: MutableSequence[Workflow],
    ):
        super().__init__(context, db_context, workflows)
        paths = set()
        for workflow in self.workflows:
            path = workflow.config["file"]
            if not os.path.isabs(path):
                path = os.path.join(os.path.dirname(context.config["path"]), path)
            paths.add(path)
        if len(paths) != 1:
            raise WorkflowProvenanceException(
                "Cannot build a single workflow provenance for multiple workflow definitions."
            )
        self.cwl_definition = cwl_utils.parser.load_document_by_uri(next(iter(paths)))
        self.scatter_map: MutableMapping[str, MutableSequence[str]] = {}

    def _add_metadata(
        self,
        cwl_object: cwl_utils.parser.Process,
        jsonld_object: MutableMapping[str, Any],
    ) -> None:
        for key, value in cwl_object.loadingOptions.addl_metadata.items():
            if key.startswith("http://schema.org/"):
                jsonld_object[key.removeprefix("http://schema.org/")] = (
                    self._add_metadata_entry(value)
                )
            elif key.startswith("https://schema.org/"):
                jsonld_object[key.removeprefix("https://schema.org/")] = (
                    self._add_metadata_entry(value)
                )

    def _add_metadata_entry(self, metadata: Any) -> Any:
        if isinstance(metadata, MutableMapping):
            jsonld_metadata = {}
            for key in metadata:
                value = self._add_metadata_entry(metadata[key])
                if key == "class":
                    jsonld_metadata["@type"] = value
                elif key == "identifier":
                    jsonld_metadata["@id"] = value
                else:
                    jsonld_metadata[
                        key.removeprefix("http://schema.org/").removeprefix(
                            "https://schema.org/"
                        )
                    ] = value
            if "@id" not in jsonld_metadata:
                jsonld_metadata["@id"] = f"#{uuid.uuid4()}"
            self.graph[jsonld_metadata["@id"]] = jsonld_metadata
            return {"@id": jsonld_metadata["@id"]}
        elif isinstance(metadata, MutableSequence):
            return [self._add_metadata_entry(v) for v in metadata]
        elif isinstance(metadata, str):
            return metadata.removeprefix("http://schema.org/").removeprefix(
                "https://schema.org/"
            )
        else:
            return metadata

    def _add_params(
        self,
        cwl_prefix: str,
        prefix: str,
        jsonld_element: MutableMapping[str, Any],
        cwl_element: cwl_utils.parser.Process,
    ) -> None:
        # Add inputs
        for cwl_input in cwl_element.inputs or []:
            jsonld_param = _get_cwl_param(cwl_input)
            jsonld_param["defaultValue"] = str(cwl_input.default)
            self.graph[jsonld_param["@id"]] = jsonld_param
            jsonld_element.setdefault("input", []).append({"@id": jsonld_param["@id"]})
            self.param_parent_map[jsonld_param["@id"]] = jsonld_element
            port_name = posixpath.join(
                prefix,
                get_name(prefix, cwl_prefix, cwl_input.id),
            )
            self.register_input_port(port_name, jsonld_param)
        # Add outputs
        for cwl_output in cwl_element.outputs or []:
            jsonld_param = _get_cwl_param(cwl_output)
            self.graph[jsonld_param["@id"]] = jsonld_param
            jsonld_element.setdefault("output", []).append({"@id": jsonld_param["@id"]})
            self.param_parent_map[jsonld_param["@id"]] = jsonld_element
            port_name = posixpath.join(
                prefix,
                get_name(prefix, cwl_prefix, cwl_output.id),
            )
            self.register_output_port(port_name, jsonld_param)

    def _get_additional_dependencies(
        self, port_name: str, jsonld_port: MutableMapping[str, Any], step_name: str
    ) -> set[str]:
        global_name = posixpath.join(step_name, port_name)
        if global_name not in self.scatter_map[step_name]:
            return super()._get_additional_dependencies(
                port_name, jsonld_port, step_name
            )
        else:
            return set()

    def _get_connection(
        self,
        cwl_prefix: str,
        prefix: str,
        source: str | MutableSequence[str],
        target_parameter: str,
        workflow_inputs: MutableSequence[str],
    ) -> MutableMapping[str, Any]:
        if isinstance(source, MutableSequence) and len(source) > 1:
            source_parameter = []
            for src in source:
                source_parameter.append(
                    self._get_source(cwl_prefix, prefix, src, workflow_inputs)
                )
        else:
            if isinstance(source, MutableSequence):
                source = source[0]
            source_parameter = self._get_source(
                cwl_prefix, prefix, source, workflow_inputs
            )
        connection = {
            "@id": f"#{uuid.uuid4()}",
            "@type": "ParameterConnection",
            "sourceParameter": source_parameter,
            "targetParameter": {"@id": target_parameter},
        }
        return connection

    def _get_source(
        self,
        cwl_prefix: str,
        prefix: str,
        src: str,
        workflow_inputs: MutableSequence[str],
    ) -> MutableMapping[str, str]:
        source_id = _get_cwl_entity_id(src)
        if source_id in workflow_inputs:
            return {"@id": source_id}
        else:
            source_name = get_name(prefix, cwl_prefix, src)
            return {"@id": self.output_port_map[source_name]["@id"]}

    def _get_step(
        self,
        cwl_prefix: str,
        prefix: str,
        cwl_step: cwl_utils.parser.WorkflowStep,
        version: str,
    ) -> MutableMapping[str, Any]:
        jsonld_step = {
            "@id": _get_cwl_entity_id(cwl_step.id),
            "@type": "HowToStep",
        }
        step_name = get_name(prefix, cwl_prefix, cwl_step.id)
        embedded_tool, cwl_prefix, _ = process_embedded_tool(
            cwl_name_prefix=cwl_prefix,
            name_prefix=prefix,
            cwl_element=cwl_step,
            step_name=step_name,
            context={"version": version},
        )
        if isinstance(embedded_tool, get_args(cwl_utils.parser.Workflow)):
            work_example = self._get_workflow(
                cwl_prefix=cwl_prefix,
                prefix=step_name,
                cwl_workflow=embedded_tool,
            )
        else:
            work_example = self._get_tool(
                cwl_prefix=cwl_prefix, prefix=step_name, cwl_tool=embedded_tool
            )
        self.register_step(step_name, jsonld_step)
        jsonld_step["workExample"] = {"@id": work_example["@id"]}
        self.graph[work_example["@id"]] = work_example
        return jsonld_step

    def _get_tool(
        self,
        cwl_prefix: str,
        prefix: str,
        cwl_tool: cwl_utils.parser.CommandLineTool | cwl_utils.parser.ExpressionTool,
    ) -> MutableMapping[str, Any]:
        # Create entity
        entity_id = _get_cwl_entity_id(cwl_tool.id)
        jsonld_tool = {
            "@id": entity_id,
            "name": entity_id.split("#")[-1],
            "input": [],
            "output": [],
        }
        if entity_id.startswith("_:"):
            jsonld_tool["@type"] = "SoftwareApplication"
        else:
            if (path := cwl_tool.id.split("#")[0][7:]) not in self.files_map:
                self.graph["./"]["hasPart"].append({"@id": entity_id})
                self.files_map[path] = os.path.basename(path)
            jsonld_tool["sha1"] = file_checksum(path, "sha1")
            jsonld_tool["@type"] = ["SoftwareApplication", "File"]
        # Add description
        if cwl_tool.doc:
            jsonld_tool["description"] = cwl_tool.doc
        # Add metadata
        self._add_metadata(cwl_tool, jsonld_tool)
        # Add inputs and outputs
        self._add_params(cwl_prefix, prefix, jsonld_tool, cwl_tool)
        return jsonld_tool

    def _get_workflow(
        self, cwl_prefix: str, prefix: str, cwl_workflow: cwl_utils.parser.Workflow
    ) -> MutableMapping[str, Any]:
        # Create entity
        entity_id = _get_cwl_entity_id(cwl_workflow.id)
        jsonld_workflow = _get_workflow_template(entity_id, entity_id.split("#")[-1])
        if not entity_id.startswith("_:"):
            if (path := cwl_workflow.id.split("#")[0][7:]) not in self.files_map:
                self.graph["./"]["hasPart"].append({"@id": entity_id})
                self.files_map[path] = os.path.basename(path)
            jsonld_workflow["sha1"] = file_checksum(path, "sha1")
            cast(MutableSequence, jsonld_workflow["@type"]).append("File")
        # Add description
        if cwl_workflow.doc:
            jsonld_workflow["description"] = cwl_workflow.doc
        # Add metadata
        self._add_metadata(cwl_workflow, jsonld_workflow)
        # Add inputs and outputs
        self._add_params(cwl_prefix, prefix, jsonld_workflow, cwl_workflow)
        # Add steps
        if len(cwl_workflow.steps) > 0:
            jsonld_workflow["step"] = []
            jsonld_steps = [
                self._get_step(
                    cwl_prefix=cwl_prefix,
                    prefix=prefix,
                    cwl_step=s,
                    version=cast(str, cwl_workflow.cwlVersion),
                )
                for s in cwl_workflow.steps
            ]
            self._register_steps(
                cwl_prefix=cwl_prefix,
                prefix=prefix,
                jsonld_entity=jsonld_workflow,
                jsonld_steps=jsonld_steps,
                cwl_steps=cwl_workflow.steps,
                version=cast(str, cwl_workflow.cwlVersion),
            )
        # Connect output sources
        workflow_inputs = [inp["@id"] for inp in jsonld_workflow.get("output", [])]
        for cwl_output in cwl_workflow.outputs or []:
            if source := cwl_output.outputSource:
                connection = self._get_connection(
                    cwl_prefix=cwl_prefix,
                    prefix=prefix,
                    source=source,
                    target_parameter=_get_cwl_entity_id(cwl_output.id),
                    workflow_inputs=workflow_inputs,
                )
                self.graph[connection["@id"]] = connection
                jsonld_workflow.setdefault("connection", []).append(
                    {"@id": connection["@id"]}
                )
        return jsonld_workflow

    async def _process_file_token(self, token_value: MutableMapping[str, Any]):
        if token_value["class"] == "File":
            if "secondaryFiles" in token_value:
                self.files_map[token_value["path"]] = token_value["checksum"][5:]
                self.graph[token_value["checksum"][5:]] = {
                    "@id": token_value["checksum"][5:],
                    "@type": "File",
                    "alternateName": token_value["basename"],
                    "sha1": token_value["checksum"][5:],
                }
                parts = cast(
                    MutableSequence[MutableMapping[str, Any]],
                    await asyncio.gather(
                        *(
                            asyncio.create_task(self._process_file_token(f))
                            for f in token_value["secondaryFiles"]
                        )
                    ),
                )
                for part in parts:
                    self.graph[part["@id"]] = part
                parts = [{"@id": token_value["checksum"][5:]}] + [
                    {"@id": p["@id"]} for p in parts
                ]
                collection = {
                    "@id": f"#{checksum(''.join(sorted([p['@id'] for p in parts])))}",
                    "@type": "Collection",
                    "mainEntity": {"@id": token_value["checksum"][5:]},
                    "hasPart": parts,
                }
                if collection["@id"] not in self.graph:
                    self.graph[collection["@id"]] = collection
                    self.graph["./"]["hasPart"].append(
                        {"@id": collection["mainEntity"]["@id"]}
                    )
                    self.graph["./"].setdefault("mentions", []).append(
                        {"@id": collection["@id"]}
                    )
                return collection
            else:
                self.files_map[token_value["path"]] = token_value["checksum"][5:]
                return {
                    "@id": token_value["checksum"][5:],
                    "@type": "File",
                    "alternateName": token_value["basename"],
                    "sha1": token_value["checksum"][5:],
                }
        elif token_value["class"] == "Directory":
            dataset = {"@type": "Dataset"}
            jsonld_map = {}
            if "path" in token_value and (
                has_part := await self._list_dir(token_value["path"], jsonld_map)
            ):
                dataset["@id"] = checksum(
                    "".join(sorted([part["@id"] for part in has_part]))
                )
                dataset["alternateName"] = token_value["basename"]
                dataset["hasPart"] = self._rename_parts(
                    has_part, jsonld_map, dataset["@id"], dataset["alternateName"]
                )
                if dataset["@id"] not in self.graph:
                    self.graph["./"]["hasPart"].append({"@id": dataset["@id"]})
                    self.graph[dataset["@id"]] = dataset
                    for entry in cast(
                        MutableSequence[MutableMapping[str, Any]], dataset["hasPart"]
                    ):
                        self.graph["./"]["hasPart"].append({"@id": entry["@id"]})
                self.files_map[token_value["path"]] = dataset["@id"]
            else:
                dataset["@id"] = token_value["basename"]
            return dataset
        else:
            raise WorkflowDefinitionException(
                f"Invalid class {token_value['class']} for file token."
            )

    def _register_steps(
        self,
        cwl_prefix: str,
        prefix: str,
        jsonld_entity: MutableMapping[str, Any],
        jsonld_steps: MutableSequence[MutableMapping[str, Any]],
        cwl_steps: MutableSequence[cwl_utils.parser.WorkflowStep],
        version: str,
    ):
        has_part = set()
        for cwl_step, jsonld_step in zip(cwl_steps, jsonld_steps):
            step_name = get_name(prefix, cwl_prefix, cwl_step.id)
            cwl_step_name = get_name(
                prefix, cwl_prefix, cwl_step.id, preserve_cwl_prefix=True
            )
            embedded_tool, inner_cwl_prefix, _ = process_embedded_tool(
                cwl_name_prefix=cwl_prefix,
                name_prefix=prefix,
                cwl_element=cwl_step,
                step_name=step_name,
                context={"version": version},
            )
            # Register step
            jsonld_entity["step"].append({"@id": jsonld_step["@id"]})
            self.graph[jsonld_step["@id"]] = jsonld_step
            # Register workExample
            has_part.add(jsonld_step["workExample"]["@id"])
            # Find scatter elements
            if isinstance(cwl_step.scatter, str):
                self.scatter_map[step_name] = [
                    get_name(step_name, cwl_step_name, cwl_step.scatter)
                ]
            else:
                self.scatter_map[step_name] = [
                    get_name(step_name, cwl_step_name, n)
                    for n in (cwl_step.scatter or [])
                ]
            # Connect sources
            workflow_inputs = [inp["@id"] for inp in jsonld_entity.get("input", [])]
            for cwl_input in cwl_step.in_ or []:
                if source := cwl_input.source:
                    global_name = get_name(prefix, cwl_prefix, cwl_input.id)
                    port_name = posixpath.relpath(global_name, step_name)
                    for inner_input in embedded_tool.inputs or []:
                        inner_global_name = get_name(
                            step_name, inner_cwl_prefix, inner_input.id
                        )
                        inner_port_name = posixpath.relpath(
                            inner_global_name, step_name
                        )
                        if port_name == inner_port_name:
                            connection = self._get_connection(
                                cwl_prefix=cwl_prefix,
                                prefix=prefix,
                                source=source,
                                target_parameter=_get_cwl_entity_id(inner_input.id),
                                workflow_inputs=workflow_inputs,
                            )
                            self.graph[connection["@id"]] = connection
                            jsonld_step.setdefault("connection", []).append(
                                {"@id": connection["@id"]}
                            )
                            break
        jsonld_entity["hasPart"] = [{"@id": p} for p in has_part]

    async def add_initial_inputs(self, wf_id: int, workflow: Workflow) -> None:
        if "settings" in workflow.config:
            cwl_prefix = (
                get_name(posixpath.sep, posixpath.sep, self.cwl_definition.id)
                if "#" in self.cwl_definition.id
                else posixpath.sep
            )
            for cwl_input in self.cwl_definition.inputs or []:
                input_name = get_name(posixpath.sep, cwl_prefix, cwl_input.id)
                port_name = posixpath.relpath(input_name, posixpath.sep)
                port = workflow.steps[
                    input_name + "-token-transformer"
                ].get_output_port()
                jsonld_port = self.input_port_map[input_name]
                if property_values := await self._get_property_values(
                    port_name, port, jsonld_port
                ):
                    self._update_actions(
                        wf_id=wf_id,
                        property_values=property_values,
                        step_name=posixpath.sep,
                        is_input=True,
                    )

    async def get_main_entity(self) -> MutableMapping[str, Any]:
        # Create entity
        cwl_prefix = (
            get_name(posixpath.sep, posixpath.sep, self.cwl_definition.id)
            if "#" in self.cwl_definition.id
            else posixpath.sep
        )
        path = self.cwl_definition.id.split("#")[0][7:]
        self.files_map[path] = os.path.basename(path)
        entity_id = _get_cwl_entity_id(self.cwl_definition.id).split("#")[0]
        main_entity = _get_workflow_template(entity_id, entity_id)
        main_entity["@type"].append("File")
        main_entity["sha1"] = file_checksum(path, "sha1")
        # Add programming language
        programming_language = _get_cwl_programming_language(
            self.cwl_definition.cwlVersion
        )
        self.graph[programming_language["@id"]] = programming_language
        main_entity["programmingLanguage"] = {"@id": programming_language["@id"]}
        # Add metadata
        self._add_metadata(self.cwl_definition, main_entity)
        # Add inputs and outputs
        self._add_params(cwl_prefix, posixpath.sep, main_entity, self.cwl_definition)
        # Add steps if present
        if (
            isinstance(self.cwl_definition, get_args(cwl_utils.parser.Workflow))
            and len(self.cwl_definition.steps) > 0
        ):
            main_entity["step"] = []
            jsonld_steps = [
                self._get_step(
                    cwl_prefix, posixpath.sep, s, self.cwl_definition.cwlVersion
                )
                for s in self.cwl_definition.steps
            ]
            self._register_steps(
                cwl_prefix=cwl_prefix,
                prefix=posixpath.sep,
                jsonld_entity=main_entity,
                jsonld_steps=jsonld_steps,
                cwl_steps=self.cwl_definition.steps,
                version=self.cwl_definition.cwlVersion,
            )
            # Connect output sources
            workflow_inputs = [inp["@id"] for inp in main_entity.get("output", [])]
            for cwl_output in self.cwl_definition.outputs or []:
                if source := cwl_output.outputSource:
                    connection = self._get_connection(
                        cwl_prefix=cwl_prefix,
                        prefix=posixpath.sep,
                        source=source,
                        target_parameter=_get_cwl_entity_id(cwl_output.id),
                        workflow_inputs=workflow_inputs,
                    )
                    self.graph[connection["@id"]] = connection
                    main_entity.setdefault("connection", []).append(
                        {"@id": connection["@id"]}
                    )
        return main_entity

    async def get_property_value(
        self, name: str, token: Token
    ) -> MutableMapping[str, Any] | None:
        if isinstance(token, ListToken):
            property_values = await asyncio.gather(
                *(
                    asyncio.create_task(self.get_property_value(name, t))
                    for t in token.value
                )
            )
            value = []
            for property_value in (k for k in property_values if k is not None):
                if property_value["@type"] in ["Dataset", "File"]:
                    # Check for duplicate checksums
                    if property_value["@id"] not in self.graph:
                        self.graph["./"]["hasPart"].append(
                            {"@id": property_value["@id"]}
                        )
                        self.graph[property_value["@id"]] = property_value
                    value.append({"@id": property_value["@id"]})
                else:
                    value.append(property_value["value"])
            value = flatten_list(value)
            return {
                "@id": f"#{uuid.uuid4()}",
                "@type": "PropertyValue",
                "name": name,
                "value": value[0] if len(value) == 1 else value,
            }
        elif isinstance(token, FileToken):
            return await self._process_file_token(token.value)
        elif isinstance(token, ObjectToken):
            value = await asyncio.gather(
                *(
                    asyncio.create_task(
                        self.get_property_value(posixpath.join(name, k), v)
                    )
                    for k, v in token.value.items()
                )
            )
            for v in value:
                self.graph[v["@id"]] = v
            return {
                "@id": f"#{uuid.uuid4()}",
                "@type": "PropertyValue",
                "name": name,
                "value": value,
            }
        elif (value := get_token_value(token)) is not None:
            return {
                "@id": f"#{uuid.uuid4()}",
                "@type": "PropertyValue",
                "name": name,
                "value": str(value),
            }
        else:
            return None

    async def process_job(
        self, create_action: MutableMapping[str, Any], step: ExecuteStep, job: Job
    ) -> None:
        if isinstance(step.command, CWLCommand):
            context = build_context(
                inputs=job.inputs,
                output_directory=job.output_directory,
                tmp_directory=job.tmp_directory,
                hardware=step.workflow.context.scheduler.get_hardware(job.name),
            )
            for name, value in step.command.environment.items():
                self.add_environment(
                    create_action=create_action,
                    name=name,
                    value=eval_expression(
                        expression=value,
                        context=context,
                        expression_lib=step.command.expression_lib,
                        full_js=step.command.full_js,
                    ),
                )
