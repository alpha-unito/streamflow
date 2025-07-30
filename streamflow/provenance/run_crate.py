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
from collections.abc import Container, MutableMapping, MutableSequence
from typing import Any, cast
from zipfile import ZipFile

from yattag import Doc

import streamflow.core.utils
import streamflow.cwl.utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowProvenanceException
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.provenance import ProvenanceManager
from streamflow.core.utils import get_job_tag
from streamflow.core.workflow import Job, Port, Status, Token, Workflow
from streamflow.log_handler import logger
from streamflow.version import VERSION
from streamflow.workflow.step import ExecuteStep

ESCAPED_COMMA = re.compile(r"\\,")
ESCAPED_DOT = re.compile(r"\\.")
ESCAPED_EQUAL = re.compile(r"\\=")


def _has_type(obj: MutableMapping[str, Any], _type: str) -> bool:
    if "@type" not in obj:
        return False
    elif isinstance(obj["@type"], str):
        return obj["@type"] == _type
    elif isinstance(obj["@type"], Container):
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


def _is_existing_directory(path: str, graph: MutableMapping[str, Any]) -> bool:
    if path == posixpath.sep:
        return True
    else:
        path = posixpath.relpath(posixpath.normpath(path), posixpath.sep)
        return bool(graph.get(path, {}).get("@type") == "Directory")


def checksum(data: str) -> str:
    sha1_checksum = hashlib.new("sha1", usedforsecurity=False)
    sha1_checksum.update(data.encode("utf-8"))
    return sha1_checksum.hexdigest()


def file_checksum(path: str, name: str) -> str:
    hash_function = hashlib.new(name=name, usedforsecurity=False)
    with open(path, "rb") as f:
        while data := f.read(2**16):
            hash_function.update(data)
    return hash_function.hexdigest()


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
                    {"@id": "https://w3id.org/ro/wfrun/process/0.3"},
                    {"@id": "https://w3id.org/ro/wfrun/workflow/0.3"},
                    {"@id": "https://w3id.org/ro/wfrun/provenance/0.3"},
                    {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"},
                ],
                "datePublished": (
                    datetime.datetime.now(datetime.timezone.utc)
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
                str,
                MutableMapping[
                    str,
                    MutableMapping[str, MutableSequence[MutableMapping[str, Any]]],
                ],
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
        if step_name is not None and step_name in self.step_map:
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
        self, path: str, jsonld_map: MutableMapping[str, Any]
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
                        jsonld_object["@id"] = checksum(
                            "".join(sorted([part["@id"] for part in inner_has_part]))
                        )
                        jsonld_object["alternateName"] = os.path.basename(element_path)
                    else:
                        jsonld_object["@id"] = os.path.basename(element_path)
                else:
                    _checksum = file_checksum(element_path, "sha1")
                    jsonld_object = {
                        "@id": _checksum,
                        "@type": "File",
                        "alternateName": os.path.basename(element_path),
                        "sha1": _checksum,
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
                for action_name, create_actions in (
                    self.create_action_map[wf_id].get(parent["@id"], {}).items()
                ):
                    if step_name.startswith(action_name):
                        for create_action in create_actions[tag]:
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

    def add_environment(
        self,
        create_action: MutableMapping[str, Any],
        name: str,
        value: Any,
        type_: str = "Text",
    ) -> None:
        instrument = self.graph[create_action["instrument"]["@id"]]
        if (parameter_id := f"{instrument['@id']}#{name.lower()}") not in self.graph:
            env_parameter = {
                "@id": parameter_id,
                "@type": "FormalParameter",
                "additionalType": type_,
                "name": name,
            }
            self.graph[env_parameter["@id"]] = env_parameter
            instrument.setdefault("environment", []).append(
                {"@id": env_parameter["@id"]}
            )
        env_property = {
            "@id": f"#{name.lower()}-pv",
            "@type": "PropertyValue",
            "exampleOfWork": {"@id": parameter_id},
            "name": name,
            "value": value,
        }
        self.graph[env_property["@id"]] = env_property
        create_action.setdefault("environment", []).append({"@id": env_property["@id"]})

    async def add_file(self, file: MutableMapping[str, str]) -> None:
        if "src" not in file:
            raise WorkflowProvenanceException(
                "Property `src` is mandatory when specifying the `--add-file` option."
            )
        src = file["src"]
        dst = file.get("dst", posixpath.sep)
        dst_parent = posixpath.dirname(posixpath.normpath(dst))
        if src in self.files_map:
            if logger.isEnabledFor(logging.WARNING):
                logger.warning(f"File {src} is already present in the archive.")
        else:
            if os.path.isfile(os.path.realpath(src)):
                _checksum = file_checksum(src, "sha1")
                jsonld_file = {
                    "@type": "File",
                    "sha1": _checksum,
                }
                if _is_existing_directory(dst, self.graph):
                    dst = posixpath.relpath(
                        posixpath.join(dst, _checksum), posixpath.sep
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
                                checksum(
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
    async def add_initial_inputs(self, wf_id: int, workflow: Workflow) -> None: ...

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
        if logger.isEnabledFor(logging.WARNING):
            if keys[-1] in current_obj:
                logger.warning(
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
    ) -> None:
        # Add main entity
        main_entity = await self.get_main_entity()
        self.step_map[posixpath.sep] = main_entity
        self.graph[main_entity["@id"]] = main_entity
        self.graph["./"]["hasPart"].append({"@id": main_entity["@id"]})
        self.graph["./"]["mainEntity"] = {"@id": main_entity["@id"]}
        # Add StreamFlow engine
        engine = {
            "@id": f"#{uuid.uuid4()}",
            "@type": "SoftwareApplication",
            "name": f"StreamFlow {VERSION}",
            "softwareVersion": VERSION,
        }
        self.graph[engine["@id"]] = engine
        # Add StreamFlow configuration file
        config_file = None
        if config:
            config_checksum = file_checksum(config, "sha1")
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
        # Add executions
        for wf_id, workflow in enumerate(self.workflows):
            wf_obj = await self.context.database.get_workflow(workflow.persistent_id)
            execution = {
                "@id": f"#{uuid.uuid4()}",
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
            ).setdefault(posixpath.sep, {}).setdefault("0", []).append(execution)
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
                "@id": f"#{uuid.uuid4()}",
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
                    for execution in await self.context.database.get_executions_by_step(
                        step.persistent_id
                    ):
                        create_action = {
                            "@id": f"#{uuid.uuid4()}",
                            "@type": "CreateAction",
                            "actionStatus": _get_action_status(
                                Status(execution["status"])
                            ),
                            "endTime": streamflow.core.utils.get_date_from_ns(
                                execution["end_time"]
                            ),
                            "instrument": {"@id": jsonld_step["workExample"]["@id"]},
                            "name": f"Run of workflow/{jsonld_step['@id']}",
                            "startTime": streamflow.core.utils.get_date_from_ns(
                                execution["start_time"]
                            ),
                        }
                        job = (
                            await Token.load(
                                self.context, execution["job_token"], self.db_context
                            )
                        ).value
                        await self.process_job(
                            create_action, cast(ExecuteStep, step), job
                        )
                        self.graph[create_action["@id"]] = create_action
                        self.create_action_map.setdefault(wf_id, {}).setdefault(
                            jsonld_step["workExample"]["@id"], {}
                        ).setdefault(step_name, {}).setdefault(
                            get_job_tag(job.name), []
                        ).append(
                            create_action
                        )
                        create_actions.append(create_action)
                # Add ControlAction
                if create_actions:
                    control_action = {
                        "@id": f"#{uuid.uuid4()}",
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
                    logger.warning(f"File {src} does not exist.")
        print(f"Successfully created run_crate archive at {path}")

    @abstractmethod
    async def get_main_entity(self) -> MutableMapping[str, Any]: ...

    @abstractmethod
    async def get_property_value(
        self, name: str, token: Token
    ) -> MutableMapping[str, Any] | None: ...

    @abstractmethod
    async def process_job(
        self, create_action: MutableMapping[str, Any], step: ExecuteStep, job: Job
    ) -> None: ...

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
