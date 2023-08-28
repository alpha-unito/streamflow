from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import logging
import os.path
import posixpath
import re
import urllib.parse
import uuid
from abc import ABC, abstractmethod
from typing import Any, MutableMapping, MutableSequence, cast
from zipfile import ZipFile

import cwltool.command_line_tool
import cwltool.context
import cwltool.process
import cwltool.workflow
from yattag import Doc

import streamflow.core.utils
import streamflow.cwl.utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowProvenanceException,
)
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.provenance import ProvenanceManager
from streamflow.core.workflow import Port, Status, Token, Workflow
from streamflow.log_handler import logger
from streamflow.version import VERSION
from streamflow.workflow.token import FileToken, ListToken, ObjectToken
from streamflow.workflow.utils import get_token_value

ESCAPED_COMMA = re.compile(r"\\,")
ESCAPED_DOT = re.compile(r"\\.")
ESCAPED_EQUAL = re.compile(r"\\=")


def _checksum(data: str) -> str:
    sha1_checksum = hashlib.new("sha1", usedforsecurity=False)
    sha1_checksum.update(data.encode("utf-8"))
    return sha1_checksum.hexdigest()


def _file_checksum(path: str, hash_function) -> str:
    with open(path, "rb") as f:
        while data := f.read(2**16):
            hash_function.update(data)
    return hash_function.hexdigest()


def _has_type(obj: MutableMapping[str, Any], _type: str) -> bool:
    if "@type" not in obj:
        return False
    elif isinstance(obj["@type"], str):
        return obj["@type"] == _type
    elif isinstance(obj["@type"], MutableSequence):
        return _type in obj["@type"]
    else:
        raise WorkflowProvenanceException(
            f"Invalid type {obj['@type']} for object {obj['@id']}"
        )


def _get_action_status(status: Status) -> str:
    if status == Status.COMPLETED:
        return "CompletedActionStatus"
    elif status == Status.FAILED:
        return "FailedActionStatus"
    elif status in [Status.CANCELLED, Status.SKIPPED]:
        return "PotentialActionStatus"
    else:
        raise WorkflowProvenanceException(f"Action status {status.name} not supported.")


def _get_cwl_entity_id(entity_id: str) -> str:
    tokens = entity_id.split("#")
    if len(tokens) > 1:
        return "#".join([tokens[0].split("/")[-1], tokens[1]])
    else:
        return tokens[0].split("/")[-1]


def _get_cwl_param(cwl_param: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    name = "#".join(cwl_param["id"].split("#")[1:])
    jsonld_param = {
        "@id": _get_cwl_entity_id(cwl_param["id"]),
        "@type": "FormalParameter",
        "additionalType": [],
        "name": name,
        "conformsTo": "https://bioschemas.org/profiles/FormalParameter/1.0-RELEASE/",
    }
    if "default" in cwl_param:
        jsonld_param["defaultValue"] = str(cwl_param["default"])
    if "doc" in cwl_param:
        jsonld_param["description"] = cwl_param["doc"]
    if "format" in cwl_param:
        jsonld_param["encodingFormat"] = cwl_param["format"]
    _process_cwl_type(cwl_param["type"], jsonld_param, cwl_param)
    if len(jsonld_param["additionalType"]) == 1:
        jsonld_param["additionalType"] = jsonld_param["additionalType"][0]
    return jsonld_param


def _get_cwl_programming_language(
    loading_context: cwltool.context.LoadingContext,
) -> MutableMapping[str, Any]:
    version = loading_context.metadata[
        "http://commonwl.org/cwltool#original_cwlVersion"
    ]
    return {
        "@id": "https://w3id.org/workflowhub/workflow-ro-crate#cwl",
        "@type": "ComputerLanguage",
        "alternateName": "CWL",
        "identifier": {"@id": f"https://w3id.org/cwl/{version}/"},
        "name": "Common Workflow Language",
        "url": {"@id": "https://www.commonwl.org/"},
        "version": version,
    }


def _get_item_fqdn(item: str) -> str:
    if item in ["connection", "sourceParameter", "targetParameter"]:
        return "https://w3id.org/ro/terms/workflow-run#" + item
    elif item in ["input", "output"]:
        return "https://bioschemas.org/ComputationalWorkflow#" + item
    else:
        return "https://schema.org/" + item


def _get_item_preview(tag: Any, text: Any, value: Any) -> None:
    if isinstance(value, str):
        with tag("span"):
            text(value)
    elif isinstance(value, MutableMapping):
        with tag("a", href=f"#{urllib.parse.quote(value['@id'])}"):
            text(value["@id"])
    elif isinstance(value, MutableSequence):
        if len(value) > 1:
            with tag("ul"):
                for item in value:
                    with tag("li"):
                        _get_item_preview(tag, text, item)
        elif len(value) >= 1:
            _get_item_preview(tag, text, value[0])
    else:
        raise WorkflowProvenanceException(
            f"Unsupported item type {type(value)} for value {value}."
        )


def _get_workflow_template(entity_id: str, name: str) -> MutableMapping[str, Any]:
    return {
        "@id": entity_id,
        "@type": ["SoftwareSourceCode", "ComputationalWorkflow", "HowTo"],
        "name": name,
        "input": [],
        "output": [],
        "conformsTo": "https://bioschemas.org/profiles/ComputationalWorkflow/1.0-RELEASE/",
    }


def _is_existing_directory(path: str, graph: MutableMapping[str, Any]) -> bool:
    if path == posixpath.sep:
        return True
    else:
        path = posixpath.relpath(posixpath.normpath(path), posixpath.sep)
        return graph.get(path, {}).get("@type") == "Directory"


def _process_cwl_type(
    cwl_type: str | MutableSequence[Any] | MutableMapping[str, Any],
    jsonld_param: MutableMapping[str, Any],
    cwl_param: MutableMapping[str, Any],
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
            if "secondaryFiles" in cwl_param:
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
    elif isinstance(cwl_type, MutableMapping):
        if cwl_type["type"] == "array":
            jsonld_param["multipleValues"] = "True"
            _process_cwl_type(cwl_type["items"], jsonld_param, cwl_param)
        elif cwl_type["type"] == "enum":
            jsonld_param["additionalType"] = "Text"
            jsonld_param["valuePattern"] = "|".join(cwl_type["symbols"])
        elif cwl_type["type"] == "record":
            jsonld_param["additionalType"] = "PropertyValue"
            jsonld_param["multipleValues"] = "True"


class RunCrateProvenanceManager(ProvenanceManager, ABC):
    def __init__(
        self,
        context: StreamFlowContext,
        db_context: DatabaseLoadingContext,
        workflows: MutableSequence[Workflow],
    ):
        super().__init__(context, db_context, workflows)
        self.graph: MutableMapping[str, Any] = {
            "./": {
                "@id": "./",
                "@type": "Dataset",
                "conformsTo": [
                    {"@id": "https://w3id.org/ro/wfrun/process/0.1"},
                    {"@id": "https://w3id.org/ro/wfrun/workflow/0.1"},
                    {"@id": "https://w3id.org/ro/wfrun/provenance/0.1"},
                    {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"},
                ],
                "datePublished": (
                    datetime.datetime.utcnow()
                    .replace(tzinfo=datetime.timezone.utc)
                    .replace(microsecond=0)
                    .isoformat()
                ),
                "hasPart": [],
            },
            "ro-crate-metadata.json": {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "about": {"@id": "./"},
                "conformsTo": [
                    {"@id": "https://w3id.org/ro/crate/1.1"},
                    {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"},
                ],
            },
            "https://w3id.org/ro/wfrun/process/0.1": {
                "@id": "https://w3id.org/ro/wfrun/process/0.1",
                "@type": "CreativeWork",
                "name": "Process Run Crate",
                "version": "0.1",
            },
            "https://w3id.org/ro/wfrun/workflow/0.1": {
                "@id": "https://w3id.org/ro/wfrun/workflow/0.1",
                "@type": "CreativeWork",
                "name": "Workflow Run Crate",
                "version": "0.1",
            },
            "https://w3id.org/ro/wfrun/provenance/0.1": {
                "@id": "https://w3id.org/ro/wfrun/provenance/0.1",
                "@type": "CreativeWork",
                "name": "Provenance Run Crate",
                "version": "0.1",
            },
            "https://w3id.org/workflowhub/workflow-ro-crate/1.0": {
                "@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0",
                "@type": "CreativeWork",
                "name": "Workflow RO-Crate",
                "version": "1.0",
            },
        }
        self.create_action_map: MutableMapping[
            int,
            MutableMapping[
                str, MutableMapping[str, MutableMapping[str, MutableMapping[str, Any]]]
            ],
        ] = {}
        self.files_map: MutableMapping[str, str] = {}
        self.input_port_map: MutableMapping[str, MutableMapping[str, Any]] = {}
        self.output_port_map: MutableMapping[str, MutableMapping[str, Any]] = {}
        self.param_parent_map: MutableMapping[str, MutableMapping[str, Any]] = {}
        self.step_map: MutableMapping[str, MutableMapping[str, Any]] = {}

    def _create_preview(self, metadata: MutableMapping[str, Any]) -> str:
        doc, tag, text = Doc().tagtext()
        with tag("html"):
            with tag("head"):
                with tag("script", type="application/ld+json"):
                    text(json.dumps(metadata, indent=2, sort_keys=True))
                with tag(
                    "script",
                    src="https://unpkg.com/ro-crate-html-js/dist/ro-crate-dynamic.js",
                ):
                    pass
                doc.stag(
                    "link",
                    rel="stylesheet",
                    href="https://stackpath.bootstrapcdn.com/bootstrap/4.2.1/css/bootstrap.min.css",
                    integrity="sha384-GJzZqFGwb1QTTN6wy59ffF1BuGJpLSa9DkKMp0DgiMDm4iYMj70gZWKYbI706tWS",
                    crossorigin="anonymous",
                )
                doc.stag(
                    "link",
                    rel="stylesheet",
                    href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css",
                )
                doc.stag("meta", charset="utf-8")
                with tag("style"):
                    text("table.table { padding-bottom: 300px; }")
            with tag("body"):
                with tag("nav", klass="navbar"):
                    with tag("ul", klass="nav navbar-nav"):
                        with tag("li"):
                            with tag("a", href="#"):
                                doc.stag(
                                    "span",
                                    klass="glyphicon glyphicon-home dataset_name",
                                )
                                text(self.graph["./"].get("name", ""))
                with tag("div", klass="container"):
                    with tag("div", klass="jumbotron"):
                        doc.stag("h4", klass="citation")
                        doc.stag("h3", klass="item_name")
                        with tag("a", href="./ro-crate-metadata.json"):
                            doc.asis(
                                'Download all the metadata for <span class="name">'
                                f'{self.graph["./"].get("name", "")}'
                                "</span> in JSON-LD format"
                            )
                        doc.stag("div", id="check")
                    with tag("div", id="summary"):
                        with tag("div", klass="all-meta"):
                            for obj in [
                                v
                                for k, v in self.graph.items()
                                if k != "ro-crate-metadata.json"
                            ]:
                                with tag("div", id=urllib.parse.quote(obj["@id"])):
                                    with tag(
                                        "table", klass="table metadata table-striped"
                                    ):
                                        with tag("tbody"):
                                            for k, v in obj.items():
                                                with tag("tr"):
                                                    with tag(
                                                        "th",
                                                        klass="prop",
                                                        style="text-align: left;",
                                                    ):
                                                        text(k)
                                                        if not k.startswith("@"):
                                                            doc.asis(
                                                                "<span>&nbsp;</span>"
                                                            )
                                                            with tag(
                                                                "a",
                                                                href=_get_item_fqdn(k),
                                                            ):
                                                                text("[?]")
                                                    with tag(
                                                        "td", style="text-align: left;"
                                                    ):
                                                        _get_item_preview(tag, text, v)
                                doc.stag("hr")
                                doc.stag("br")
                                doc.stag("br")
                                if (
                                    isinstance(obj["@type"], str)
                                    and obj["@type"] in ["Dataset", "File"]
                                    or isinstance(obj["@type"], MutableSequence)
                                    and (
                                        "Dataset" in obj["@type"]
                                        or "File" in obj["@type"]
                                    )
                                ):
                                    with tag("div"):
                                        with tag("h3"):
                                            with tag("a", href=obj["@id"]):
                                                text("Download: ")
                                            text(obj["@id"])
        return doc.getvalue()

    def _get_additional_dependencies(
        self, port_name: str, jsonld_port: MutableMapping[str, Any], step_name: str
    ) -> set[str]:
        dependencies = set()
        for connection in (
            self.graph[c["@id"]] for c in self.step_map[step_name].get("connection", [])
        ):
            if isinstance(connection["targetParameter"], MutableSequence):
                targets = [t["@id"] for t in connection["targetParameter"]]
            else:
                targets = [connection["targetParameter"]["@id"]]
            if jsonld_port["@id"] in targets:
                if isinstance(connection["sourceParameter"], MutableSequence):
                    dependencies.update(
                        {s["@id"] for s in connection["sourceParameter"]}
                    )
                else:
                    dependencies.add(connection["sourceParameter"]["@id"])
        return dependencies

    async def _get_property_values(
        self,
        port_name: str,
        port: Port,
        jsonld_port: MutableMapping[str, Any],
        step_name: str | None = None,
    ) -> MutableMapping[str, MutableMapping[str, Any]]:
        tokens = await asyncio.gather(
            *(
                asyncio.create_task(Token.load(self.context, t, self.db_context))
                for t in await self.context.database.get_port_tokens(port.persistent_id)
            )
        )
        property_values = cast(
            MutableMapping[str, MutableMapping[str, Any]],
            dict(
                zip(
                    (t.tag for t in tokens),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(
                                self.get_property_value(jsonld_port["name"], t)
                            )
                            for t in tokens
                        )
                    ),
                )
            ),
        )
        property_values = {k: pv for k, pv in property_values.items() if pv is not None}
        # Check for duplicate checksums
        for property_value in property_values.values():
            if property_value["@id"] not in self.graph:
                if property_value["@type"] in ["Dataset", "File"]:
                    self.graph["./"]["hasPart"].append({"@id": property_value["@id"]})
                self.graph[property_value["@id"]] = property_value
        dependencies = {jsonld_port["@id"]}
        if step_name in self.step_map:
            dependencies.update(
                self._get_additional_dependencies(port_name, jsonld_port, step_name)
            )
        property_values = {
            k: self.graph[pv["@id"]] for k, pv in property_values.items()
        }
        for property_value in property_values.values():
            if "exampleOfWork" in property_value:
                if isinstance(property_value["exampleOfWork"], MutableMapping):
                    example_of_work = {property_value["exampleOfWork"]["@id"]}
                else:
                    example_of_work = {
                        eow["@id"] for eow in property_value["exampleOfWork"]
                    }
            else:
                example_of_work = set()
            example_of_work.update(dependencies)
            if len(example_of_work) > 1:
                property_value["exampleOfWork"] = [
                    {"@id": eow} for eow in example_of_work
                ]
            else:
                property_value["exampleOfWork"] = {"@id": next(iter(example_of_work))}
        return property_values

    async def _list_dir(
        self, path: str, jsonld_map: [MutableMapping[str, Any]]
    ) -> MutableSequence[MutableMapping[str, Any]]:
        has_part = []
        if os.path.exists(path):
            dir_content = os.listdir(path)
            for element in dir_content:
                element_path = os.path.join(path, element)
                if os.path.isdir(element_path):
                    jsonld_object = {"@type": "Dataset"}
                    if inner_has_part := await self._list_dir(element_path, jsonld_map):
                        jsonld_object["hasPart"] = inner_has_part
                        jsonld_object["@id"] = _checksum(
                            "".join(sorted([part["@id"] for part in inner_has_part]))
                        )
                        jsonld_object["alternateName"] = os.path.basename(element_path)
                    else:
                        jsonld_object["@id"] = os.path.basename(element_path)
                else:
                    checksum = _file_checksum(
                        element_path, hashlib.new("sha1", usedforsecurity=False)
                    )
                    jsonld_object = {
                        "@id": checksum,
                        "@type": "File",
                        "alternateName": os.path.basename(element_path),
                        "sha1": checksum,
                    }
                jsonld_map[jsonld_object["@id"]] = element_path
                has_part.append(jsonld_object)
        return has_part

    def _rename_parts(
        self,
        parts: MutableSequence[MutableMapping[str, Any]],
        jsonld_map: MutableMapping[str, str],
        prefix: str,
        alternatePrefix: str,
    ) -> MutableSequence[MutableMapping[str, Any]]:
        for part in parts:
            path = jsonld_map.pop(part["@id"])
            part["@id"] = os.path.join(prefix, part["@id"])
            if "alternateName" in part:
                part["alternateName"] = os.path.join(
                    alternatePrefix, part["alternateName"]
                )
            self.files_map[path] = part["@id"]
            if part["@id"] not in self.graph:
                self.graph[part["@id"]] = part
            if part["@type"] == "Dataset" and "hasPart" in part:
                part["hasPart"] = self._rename_parts(
                    part["hasPart"],
                    jsonld_map,
                    part["@id"],
                    part.get("alternateName", part["@id"]),
                )
        return [{"@id": part["@id"]} for part in parts]

    def _update_actions(
        self,
        wf_id: int,
        property_values: MutableMapping[str, MutableMapping[str, Any]],
        step_name: str,
        is_input: bool,
    ) -> None:
        for tag, property_value in property_values.items():
            if isinstance(property_value["exampleOfWork"], MutableSequence):
                params = [p for p in property_value["exampleOfWork"]]
            else:
                params = [property_value["exampleOfWork"]]
            for param in params:
                parent = self.param_parent_map[param["@id"]]
                for action_name, create_actions in self.create_action_map[wf_id][
                    parent["@id"]
                ].items():
                    if step_name.startswith(action_name):
                        create_action = create_actions[tag]
                        if is_input and param["@id"] in [
                            inp["@id"] for inp in parent.get("input", [])
                        ]:
                            jsonld_object = create_action.setdefault("object", [])
                            if property_value["@id"] not in [
                                obj["@id"] for obj in jsonld_object
                            ]:
                                jsonld_object.append({"@id": property_value["@id"]})
                        elif not is_input and param["@id"] in [
                            out["@id"] for out in parent.get("output", [])
                        ]:
                            jsonld_result = create_action.setdefault("result", [])
                            if property_value["@id"] not in [
                                res["@id"] for res in jsonld_result
                            ]:
                                jsonld_result.append({"@id": property_value["@id"]})

    async def add_file(self, file: MutableMapping[str, str]) -> None:
        if "src" not in file:
            raise WorkflowProvenanceException(
                "Property `src` is mandatory when specifying the `--add-file` option."
            )
        src = file["src"]
        dst = file.get("dst", posixpath.sep)
        dst_parent = posixpath.dirname(posixpath.normpath(dst))
        if src in self.files_map:
            if logger.isEnabledFor(logging.WARN):
                logger.warn(f"File {src} is already present in the archive.")
        else:
            if os.path.isfile(os.path.realpath(src)):
                checksum = _file_checksum(
                    src, hashlib.new("sha1", usedforsecurity=False)
                )
                jsonld_file = {
                    "@type": "File",
                    "sha1": checksum,
                }

                if _is_existing_directory(dst, self.graph):
                    dst = posixpath.relpath(
                        posixpath.join(dst, checksum), posixpath.sep
                    )
                    jsonld_file["alternateName"] = os.path.basename(src)
                    if dst_parent != posixpath.dirname:
                        self.graph[dst_parent].setdefault("hasPart", []).append(
                            {"@id": dst}
                        )
                elif _is_existing_directory(dst_parent, self.graph):
                    dst = posixpath.relpath(dst, posixpath.sep)
                    if dst_parent != posixpath.sep:
                        self.graph[dst_parent].setdefault("hasPart", []).append(
                            {"@id": dst}
                        )
                else:
                    raise WorkflowProvenanceException(
                        f"Property `dst` points to the non-existing location `{dst}`."
                    )
                jsonld_file["@id"] = dst
                self.graph[dst] = jsonld_file
            else:
                jsonld_dataset = {"@type": "Dataset"}
                jsonld_map = {}
                has_part = await self._list_dir(src, jsonld_map)
                if _is_existing_directory(dst, self.graph):
                    if has_part:
                        dst = posixpath.relpath(
                            posixpath.join(
                                dst,
                                _checksum(
                                    "".join(sorted([part["@id"] for part in has_part]))
                                ),
                            ),
                            posixpath.sep,
                        )
                        jsonld_dataset["alternateName"] = os.path.basename(src)
                    else:
                        dst = posixpath.relpath(os.path.basename(src), posixpath.sep)
                    if dst_parent != posixpath.sep:
                        self.graph[dst_parent].setdefault("hasPart", []).append(
                            {"@id": dst}
                        )
                elif _is_existing_directory(dst_parent, self.graph):
                    dst = posixpath.relpath(dst, posixpath.sep)
                    if dst_parent != posixpath.sep:
                        self.graph[dst_parent].setdefault("hasPart", []).append(
                            {"@id": dst}
                        )
                else:
                    raise WorkflowProvenanceException(
                        f"Property `dst` points to the non-existing location `{dst}`."
                    )
                jsonld_dataset["@id"] = dst
                if has_part:
                    jsonld_dataset["hasPart"] = self._rename_parts(
                        has_part,
                        jsonld_map,
                        dst,
                        os.path.basename(src),
                    )
                    for entry in cast(
                        MutableSequence[MutableMapping[str, Any]],
                        jsonld_dataset["hasPart"],
                    ):
                        self.graph["./"]["hasPart"].append({"@id": entry["@id"]})
                self.graph[dst] = jsonld_dataset
            self.graph["./"]["hasPart"].append({"@id": self.graph[dst]["@id"]})
            self.files_map[src] = dst
            for k, v in (
                (k, v)
                for k, v in file.items()
                if k not in ["src", "dst", "@id", "@type"]
            ):
                v = ESCAPED_EQUAL.sub("=", v)
                v = ESCAPED_COMMA.sub(",", v)
                try:
                    self.graph[dst][k] = json.loads(v)
                except json.JSONDecodeError:
                    self.graph[dst][k] = v

    @abstractmethod
    async def add_initial_inputs(self, wf_id: int, workflow: Workflow) -> None:
        ...

    async def add_property(self, key: str, value: str):
        current_obj = self.graph
        keys = re.split(r"(?<!\\)\.", key)
        for k in keys[:-1]:
            k = ESCAPED_EQUAL.sub("=", k)
            k = ESCAPED_COMMA.sub(",", k)
            k = ESCAPED_DOT.sub(".", k)
            if k not in current_obj:
                raise WorkflowProvenanceException(
                    f"Token `{k}` of key `{key}` does not exist in archive manifest."
                )
            current_obj = current_obj[k]
        if logger.isEnabledFor(logging.WARN):
            if keys[-1] in current_obj:
                logger.warn(
                    f"Key {key} already exists in archive manifest and will be overridden."
                )
        value = ESCAPED_EQUAL.sub("=", value)
        value = ESCAPED_COMMA.sub(",", value)
        try:
            current_obj[keys[-1]] = json.loads(value)
        except json.JSONDecodeError:
            current_obj[keys[-1]] = value

    async def create_archive(
        self,
        outdir: str,
        filename: str | None,
        config: str | None,
        additional_files: MutableSequence[MutableMapping[str, str]] | None,
        additional_properties: MutableSequence[MutableMapping[str, str]] | None,
    ):
        # Add main entity
        main_entity = await self.get_main_entity()
        self.step_map[posixpath.sep] = main_entity
        self.graph[main_entity["@id"]] = main_entity
        self.graph["./"]["hasPart"].append({"@id": main_entity["@id"]})
        self.graph["./"]["mainEntity"] = {"@id": main_entity["@id"]}
        # Add StreamFlow engine
        engine = {
            "@id": "#" + str(uuid.uuid4()),
            "@type": "SoftwareApplication",
            "name": f"StreamFlow {VERSION}",
            "softwareVersion": VERSION,
        }
        self.graph[engine["@id"]] = engine
        # Add StreamFlow configuration file
        config_file = None
        if config:
            config_checksum = _file_checksum(
                config, hashlib.new("sha1", usedforsecurity=False)
            )
            config_file = {
                "@id": config_checksum,
                "@type": "File",
                "alternateName": os.path.basename(config),
                "encodingFormat": "application/yaml",
                "sha1": config_checksum,
            }
            self.files_map[config] = config_checksum
            self.graph["./"]["hasPart"].append({"@id": config_file["@id"]})
            self.graph[config_file["@id"]] = config_file
        # Add executons
        for wf_id, workflow in enumerate(self.workflows):
            wf_obj = await self.context.database.get_workflow(workflow.persistent_id)
            execution = {
                "@id": "#" + str(uuid.uuid4()),
                "@type": "CreateAction",
                "actionStatus": _get_action_status(Status(wf_obj["status"])),
                "endTime": streamflow.core.utils.get_date_from_ns(wf_obj["end_time"]),
                "instrument": {"@id": main_entity["@id"]},
                "name": f"Run of workflow/{main_entity['@id']}",
                "startTime": streamflow.core.utils.get_date_from_ns(
                    wf_obj["start_time"]
                ),
            }
            self.create_action_map.setdefault(wf_id, {}).setdefault(
                main_entity["@id"], {}
            ).setdefault(posixpath.sep, {})["0"] = execution
            self.graph["./"].setdefault("mentions", []).append(
                {"@id": execution["@id"]}
            )
            # Process the main entity if it is executable
            for port_name, port in workflow.get_output_ports().items():
                jsonld_port = self.output_port_map[
                    posixpath.join(posixpath.sep, port_name)
                ]
                if property_values := await self._get_property_values(
                    port_name, port, jsonld_port
                ):
                    self._update_actions(
                        wf_id=wf_id,
                        property_values=property_values,
                        step_name=posixpath.sep,
                        is_input=False,
                    )
            self.graph[execution["@id"]] = execution
            organize_action: MutableMapping[str, Any] = {
                "@id": "#" + str(uuid.uuid4()),
                "@type": "OrganizeAction",
                "instrument": {"@id": engine["@id"]},
                "name": f"Run of StreamFlow {VERSION}",
                "result": {"@id": execution["@id"]},
            }
            self.graph[organize_action["@id"]] = organize_action
            # Add StreamFlow configuration file
            if config:
                organize_action.setdefault("object", []).append(
                    {"@id": config_file["@id"]}
                )
            # Process steps
            for step_name, jsonld_step in self.step_map.items():
                # Add CreateActions
                create_actions = []
                if step := workflow.steps.get(step_name):
                    for command in await self.context.database.get_commands_by_step(
                        step.persistent_id
                    ):
                        create_action = {
                            "@id": "#" + str(uuid.uuid4()),
                            "@type": "CreateAction",
                            "actionStatus": _get_action_status(
                                Status(command["status"])
                            ),
                            "endTime": streamflow.core.utils.get_date_from_ns(
                                command["end_time"]
                            ),
                            "instrument": {"@id": jsonld_step["workExample"]["@id"]},
                            "name": f"Run of workflow/{jsonld_step['@id']}",
                            "startTime": streamflow.core.utils.get_date_from_ns(
                                command["start_time"]
                            ),
                        }
                        self.graph[create_action["@id"]] = create_action
                        self.create_action_map.setdefault(wf_id, {}).setdefault(
                            jsonld_step["workExample"]["@id"], {}
                        ).setdefault(step_name, {})[command["tag"]] = create_action
                        create_actions.append(create_action)
                # Add ControlAction
                if create_actions:
                    control_action = {
                        "@id": "#" + str(uuid.uuid4()),
                        "@type": "ControlAction",
                        "instrument": {"@id": jsonld_step["@id"]},
                        "name": f"orchestrate {jsonld_step['workExample']['@id']}",
                        "object": [{"@id": ca["@id"]} for ca in create_actions],
                    }
                    organize_action.setdefault("object", []).append(
                        {"@id": control_action["@id"]}
                    )
                    self.graph[control_action["@id"]] = control_action
            # Add initial inputs
            await self.add_initial_inputs(wf_id, workflow)
            # Add command inputs and outputs
            for step_name in self.step_map:
                if step := workflow.steps.get(step_name):
                    for port_name, port in step.get_input_ports().items():
                        if jsonld_port := self.input_port_map.get(
                            posixpath.join(step_name, port_name)
                        ):
                            if property_values := await self._get_property_values(
                                port_name, port, jsonld_port, step_name
                            ):
                                self._update_actions(
                                    wf_id=wf_id,
                                    property_values=property_values,
                                    step_name=step_name,
                                    is_input=True,
                                )
                    for port_name, port in step.get_output_ports().items():
                        if jsonld_port := self.output_port_map.get(
                            posixpath.join(step_name, port_name)
                        ):
                            if property_values := await self._get_property_values(
                                port_name, port, jsonld_port, step_name
                            ):
                                self._update_actions(
                                    wf_id=wf_id,
                                    property_values=property_values,
                                    step_name=step_name,
                                    is_input=False,
                                )
        # Add additional files
        for file in additional_files or []:
            await self.add_file(file)
        # Add additional properties
        for prop in additional_properties or []:
            await asyncio.gather(
                *(asyncio.create_task(self.add_property(k, v)) for k, v in prop.items())
            )
        # Create metadata
        metadata = {
            "@context": [
                "https://w3id.org/ro/crate/1.1/context",
                {
                    "ParameterConnection": "https://w3id.org/ro/terms/workflow-run#ParameterConnection",
                    "connection": "https://w3id.org/ro/terms/workflow-run#connection",
                    "sourceParameter": "https://w3id.org/ro/terms/workflow-run#sourceParameter",
                    "targetParameter": "https://w3id.org/ro/terms/workflow-run#targetParameter",
                    "sha1": "https://w3id.org/ro/terms/workflow-run#sha1",
                },
            ],
            "@graph": list(self.graph.values()),
        }
        # Populate empty fields in Ro-Crate objects
        for obj in cast(MutableSequence[MutableMapping[str, Any]], metadata["@graph"]):
            if _has_type(obj, "CreateAction"):
                obj.setdefault("object", [])
                obj.setdefault("result", [])
        # Create directory structure
        os.makedirs(outdir, exist_ok=True)
        path = os.path.join(outdir, filename or (self.workflows[0].name + ".crate.zip"))
        with ZipFile(path, "w") as archive:
            with archive.open("ro-crate-metadata.json", "w") as f:
                f.write(json.dumps(metadata, indent=4, sort_keys=True).encode("utf-8"))
            with archive.open("ro-crate-preview.html", "w") as f:
                f.write(self._create_preview(metadata).encode("utf-8"))
            for src, dst in self.files_map.items():
                if os.path.exists(src):
                    if dst not in archive.namelist():
                        archive.write(src, dst)
                else:
                    logger.warn(f"File {src} does not exist.")
        print(f"Successfully created run_crate archive at {path}")

    @abstractmethod
    async def get_main_entity(self) -> MutableMapping[str, Any]:
        ...

    @abstractmethod
    async def get_property_value(
        self, name: str, token: Token
    ) -> MutableMapping[str, Any] | None:
        ...

    def register_input_port(
        self, streamflow_name: str, run_crate_param: MutableMapping[str, Any]
    ) -> None:
        self.input_port_map[streamflow_name] = run_crate_param

    def register_output_port(
        self, streamflow_name: str, run_crate_param: MutableMapping[str, Any]
    ) -> None:
        self.output_port_map[streamflow_name] = run_crate_param

    def register_step(
        self, streamflow_name: str, run_crate_step: MutableMapping[str, Any]
    ) -> None:
        self.step_map[streamflow_name] = run_crate_step


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
        (
            self.cwl_definition,
            self.loading_context,
        ) = streamflow.cwl.utils.load_cwl_workflow(list(paths)[0])
        self.scatter_map: MutableMapping[str, MutableSequence[str]] = {}

    def _add_metadata(
        self,
        cwl_object: cwltool.process.Process,
        jsonld_object: MutableMapping[str, Any],
    ) -> None:
        for key, value in cwl_object.metadata.items():
            if key.startswith("http://schema.org/"):
                jsonld_object[key[18:]] = self._add_metadata_entry(value)
            elif key.startswith("https://schema.org/"):
                jsonld_object[key[19:]] = self._add_metadata_entry(value)

    def _add_metadata_entry(self, metadata: Any) -> Any:
        if isinstance(metadata, MutableMapping):
            jsonld_metadata = {}
            for key in metadata:
                value = self._add_metadata_entry(metadata[key])
                if key.startswith("http://schema.org/"):
                    key = key[18:]
                elif key.startswith("https://schema.org/"):
                    key = key[19:]
                if key == "class":
                    jsonld_metadata["@type"] = value
                elif key == "identifier":
                    jsonld_metadata["@id"] = value
                else:
                    jsonld_metadata[key] = value
            if "@id" not in jsonld_metadata:
                jsonld_metadata["@id"] = "#" + str(uuid.uuid4())
            self.graph[jsonld_metadata["@id"]] = jsonld_metadata
            return {"@id": jsonld_metadata["@id"]}
        elif isinstance(metadata, MutableSequence):
            return [self._add_metadata_entry(v) for v in metadata]
        elif isinstance(metadata, str):
            if metadata.startswith("http://schema.org/"):
                return metadata[18:]
            elif metadata.startswith("https://schema.org/"):
                return metadata[19:]
            else:
                return metadata
        else:
            return metadata

    def _add_params(
        self,
        cwl_prefix: str,
        prefix: str,
        jsonld_element: MutableMapping[str, Any],
        cwl_element: cwltool.process.Process,
    ) -> None:
        # Add inputs
        for cwl_input in cwl_element.tool["inputs"]:
            jsonld_param = _get_cwl_param(cwl_input)
            self.graph[jsonld_param["@id"]] = jsonld_param
            jsonld_element.setdefault("input", []).append({"@id": jsonld_param["@id"]})
            self.param_parent_map[jsonld_param["@id"]] = jsonld_element
            port_name = posixpath.join(
                prefix,
                streamflow.cwl.utils.get_name(prefix, cwl_prefix, cwl_input["id"]),
            )
            self.register_input_port(port_name, jsonld_param)
        # Add outputs
        for cwl_output in cwl_element.tool["outputs"]:
            jsonld_param = _get_cwl_param(cwl_output)
            self.graph[jsonld_param["@id"]] = jsonld_param
            jsonld_element.setdefault("output", []).append({"@id": jsonld_param["@id"]})
            self.param_parent_map[jsonld_param["@id"]] = jsonld_element
            port_name = posixpath.join(
                prefix,
                streamflow.cwl.utils.get_name(prefix, cwl_prefix, cwl_output["id"]),
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
            "@id": "#" + str(uuid.uuid4()),
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
            source_name = streamflow.cwl.utils.get_name(prefix, cwl_prefix, src)
            return {"@id": self.output_port_map[source_name]["@id"]}

    def _get_step(
        self, cwl_prefix: str, prefix: str, cwl_step: cwltool.workflow.WorkflowStep
    ) -> MutableMapping[str, Any]:
        jsonld_step = {
            "@id": _get_cwl_entity_id(cwl_step.id),
            "@type": "HowToStep",
        }
        step_name = streamflow.cwl.utils.get_name(prefix, cwl_prefix, cwl_step.id)
        cwl_prefix = streamflow.cwl.utils.get_inner_cwl_prefix(
            prefix, cwl_prefix, cwl_step
        )
        if isinstance(cwl_step.embedded_tool, cwltool.workflow.Workflow):
            work_example = self._get_workflow(
                cwl_prefix=cwl_prefix,
                prefix=step_name,
                cwl_workflow=cwl_step.embedded_tool,
            )
        else:
            work_example = self._get_tool(
                cwl_prefix=cwl_prefix, prefix=step_name, cwl_tool=cwl_step.embedded_tool
            )
        self.register_step(step_name, jsonld_step)
        jsonld_step["workExample"] = {"@id": work_example["@id"]}
        self.graph[work_example["@id"]] = work_example
        return jsonld_step

    def _get_tool(
        self,
        cwl_prefix: str,
        prefix: str,
        cwl_tool: (
            cwltool.command_line_tool.CommandLineTool
            | cwltool.command_line_tool.ExpressionTool
        ),
    ) -> MutableMapping[str, Any]:
        # Create entity
        entity_id = _get_cwl_entity_id(cwl_tool.tool["id"])
        path = cwl_tool.tool["id"].split("#")[0][7:]
        if path not in self.files_map:
            self.graph["./"]["hasPart"].append({"@id": entity_id})
            self.files_map[path] = os.path.basename(path)
        jsonld_tool = {
            "@id": entity_id,
            "name": entity_id.split("#")[-1],
            "input": [],
            "output": [],
            "sha1": _file_checksum(path, hashlib.new("sha1", usedforsecurity=False)),
            "@type": ["SoftwareApplication", "File"],
        }
        # Add description
        if "doc" in cwl_tool.tool:
            jsonld_tool["description"] = cwl_tool.tool["doc"]
        # Add metadata
        self._add_metadata(cwl_tool, jsonld_tool)
        # Add inputs and outputs
        self._add_params(cwl_prefix, prefix, jsonld_tool, cwl_tool)
        return jsonld_tool

    def _get_workflow(
        self, cwl_prefix: str, prefix: str, cwl_workflow: cwltool.workflow.Workflow
    ) -> MutableMapping[str, Any]:
        # Create entity
        entity_id = _get_cwl_entity_id(cwl_workflow.tool["id"])
        jsonld_workflow = _get_workflow_template(entity_id, entity_id.split("#")[-1])
        path = cwl_workflow.tool["id"].split("#")[0][7:]
        jsonld_workflow.update(
            {"sha1": _file_checksum(path, hashlib.new("sha1", usedforsecurity=False))}
        )
        if (path := cwl_workflow.tool["id"].split("#")[0][7:]) not in self.files_map:
            self.graph["./"]["hasPart"].append({"@id": entity_id})
            self.files_map[path] = os.path.basename(path)
        jsonld_workflow["@type"].append("File")
        # Add description
        if "doc" in cwl_workflow.tool:
            jsonld_workflow["description"] = cwl_workflow.tool["doc"]
        # Add metadata
        self._add_metadata(cwl_workflow, jsonld_workflow)
        # Add inputs and outputs
        self._add_params(cwl_prefix, prefix, jsonld_workflow, cwl_workflow)
        # Add steps
        if len(cwl_workflow.steps) > 0:
            jsonld_workflow["step"] = []
            jsonld_steps = [
                self._get_step(cwl_prefix, prefix, s) for s in cwl_workflow.steps
            ]
            self._register_steps(
                cwl_prefix=cwl_prefix,
                prefix=prefix,
                jsonld_entity=jsonld_workflow,
                jsonld_steps=jsonld_steps,
                cwl_steps=cwl_workflow.steps,
            )
        # Connect output sources
        workflow_inputs = [inp["@id"] for inp in jsonld_workflow.get("output", [])]
        for cwl_output in cwl_workflow.tool.get("outputs", []):
            if source := cwl_output.get("outputSource"):
                connection = self._get_connection(
                    cwl_prefix=cwl_prefix,
                    prefix=prefix,
                    source=source,
                    target_parameter=_get_cwl_entity_id(cwl_output["id"]),
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
                    "@id": "#" + _checksum("".join(sorted([p["@id"] for p in parts]))),
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
                dataset["@id"] = _checksum(
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
        cwl_steps: MutableSequence[cwltool.workflow.WorkflowStep],
    ):
        has_part = set()
        for cwl_step, jsonld_step in zip(cwl_steps, jsonld_steps):
            step_name = streamflow.cwl.utils.get_name(prefix, cwl_prefix, cwl_step.id)
            cwl_step_name = streamflow.cwl.utils.get_name(
                prefix, cwl_prefix, cwl_step.id, preserve_cwl_prefix=True
            )
            inner_cwl_prefix = streamflow.cwl.utils.get_inner_cwl_prefix(
                prefix, cwl_prefix, cwl_step
            )
            # Register step
            jsonld_entity["step"].append({"@id": jsonld_step["@id"]})
            self.graph[jsonld_step["@id"]] = jsonld_step
            # Register workExample
            has_part.add(jsonld_step["workExample"]["@id"])
            # Find scatter elements
            if isinstance(cwl_step.tool.get("scatter"), str):
                self.scatter_map[step_name] = [
                    streamflow.cwl.utils.get_name(
                        step_name, cwl_step_name, cwl_step.tool["scatter"]
                    )
                ]
            else:
                self.scatter_map[step_name] = [
                    streamflow.cwl.utils.get_name(step_name, cwl_step_name, n)
                    for n in cwl_step.tool.get("scatter", [])
                ]
            # Connect sources
            workflow_inputs = [inp["@id"] for inp in jsonld_entity.get("input", [])]
            for cwl_input in cwl_step.tool.get("inputs", []):
                if source := cwl_input.get("source"):
                    global_name = streamflow.cwl.utils.get_name(
                        prefix, cwl_prefix, cwl_input["id"]
                    )
                    port_name = posixpath.relpath(global_name, step_name)
                    for inner_input in cwl_step.embedded_tool.tool.get("inputs", []):
                        inner_global_name = streamflow.cwl.utils.get_name(
                            step_name, inner_cwl_prefix, inner_input["id"]
                        )
                        inner_port_name = posixpath.relpath(
                            inner_global_name, step_name
                        )
                        if port_name == inner_port_name:
                            connection = self._get_connection(
                                cwl_prefix=cwl_prefix,
                                prefix=prefix,
                                source=source,
                                target_parameter=_get_cwl_entity_id(inner_input["id"]),
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
                streamflow.cwl.utils.get_name(
                    posixpath.sep, posixpath.sep, self.cwl_definition.tool["id"]
                )
                if "#" in self.cwl_definition.tool["id"]
                else posixpath.sep
            )
            for cwl_input in self.cwl_definition.tool.get("inputs"):
                input_name = streamflow.cwl.utils.get_name(
                    posixpath.sep, cwl_prefix, cwl_input["id"]
                )
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
            streamflow.cwl.utils.get_name(
                posixpath.sep, posixpath.sep, self.cwl_definition.tool["id"]
            )
            if "#" in self.cwl_definition.tool["id"]
            else posixpath.sep
        )
        path = self.cwl_definition.tool["id"].split("#")[0][7:]
        self.files_map[path] = os.path.basename(path)
        entity_id = _get_cwl_entity_id(self.cwl_definition.tool["id"]).split("#")[0]
        main_entity = _get_workflow_template(entity_id, entity_id)
        main_entity["@type"].append("File")
        main_entity["sha1"] = _file_checksum(
            path, hashlib.new("sha1", usedforsecurity=False)
        )
        # Add programming language
        programming_language = _get_cwl_programming_language(self.loading_context)
        self.graph[programming_language["@id"]] = programming_language
        main_entity["programmingLanguage"] = {"@id": programming_language["@id"]}
        # Add metadata
        self._add_metadata(self.cwl_definition, main_entity)
        # Add inputs and outputs
        self._add_params(cwl_prefix, posixpath.sep, main_entity, self.cwl_definition)
        # Add steps if present
        if (
            isinstance(self.cwl_definition, cwltool.workflow.Workflow)
            and len(self.cwl_definition.steps) > 0
        ):
            main_entity["step"] = []
            jsonld_steps = [
                self._get_step(cwl_prefix, posixpath.sep, s)
                for s in self.cwl_definition.steps
            ]
            self._register_steps(
                cwl_prefix=cwl_prefix,
                prefix=posixpath.sep,
                jsonld_entity=main_entity,
                jsonld_steps=jsonld_steps,
                cwl_steps=self.cwl_definition.steps,
            )
            # Connect output sources
            workflow_inputs = [inp["@id"] for inp in main_entity.get("output", [])]
            for cwl_output in self.cwl_definition.tool.get("outputs", []):
                if source := cwl_output.get("outputSource"):
                    connection = self._get_connection(
                        cwl_prefix=cwl_prefix,
                        prefix=posixpath.sep,
                        source=source,
                        target_parameter=_get_cwl_entity_id(cwl_output["id"]),
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
            for property_value in property_values:
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
            value = streamflow.core.utils.flatten_list(value)
            return {
                "@id": "#" + str(uuid.uuid4()),
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
                "@id": "#" + str(uuid.uuid4()),
                "@type": "PropertyValue",
                "name": name,
                "value": value,
            }
        elif (value := get_token_value(token)) is not None:
            return {
                "@id": "#" + str(uuid.uuid4()),
                "@type": "PropertyValue",
                "name": name,
                "value": str(value),
            }
        else:
            return None
