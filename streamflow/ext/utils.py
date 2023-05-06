from __future__ import annotations

import logging
import os
import sys
from typing import Any, MutableMapping

import jsonref
from importlib_metadata import entry_points

from streamflow.core.exception import InvalidPluginException
from streamflow.core.utils import get_class_fullname
from streamflow.ext.plugin import StreamFlowPlugin
from streamflow.log_handler import logger

PLUGIN_ENTRY_POINT = "unito.streamflow.plugin"


def _filter_by_name(classes: MutableMapping[str, Any], name: str):
    filtered_classes = {}
    for class_ in classes:
        ext_objs = [ext for ext in classes[class_] if ext["name"] == name]
        if len(ext_objs) > 0:
            filtered_classes[class_] = ext_objs
    return filtered_classes


def _get_property_desc(k: str, obj: MutableMapping[str, Any]) -> str:
    property_desc = [k]
    if "type" in obj:
        property_desc[0] = f"{property_desc[0]}: {_get_type_repr(obj)}"
    elif "oneOf" in obj:
        types = [_get_type_repr(oo) for oo in obj["oneOf"] if "type" in oo]
        property_desc[0] = f"{property_desc[0]}: Union[{', '.join(types)}]"
    if "default" in obj:
        property_desc[0] = f"{property_desc[0]} (default: {obj['default']})"
    if "description" in obj:
        property_desc.append(obj["description"])
    return "\n".join(property_desc)


def _get_type_repr(obj: MutableMapping[str, Any]) -> str | None:
    if "type" in obj:
        if obj["type"] == "object":
            return obj.get("title", "object")
        else:
            return obj["type"]
    else:
        return None


def list_extensions():
    if plugins := entry_points(group=PLUGIN_ENTRY_POINT):
        plugin_objs = []
        max_sizes = {
            "name": 0,
            "package": 0,
            "version": 0,
            "class": 0,
        }
        for plugin in plugins:
            plugin_class = (plugin.load())()
            if isinstance(plugin_class, StreamFlowPlugin):
                plugin_objs.append(
                    {
                        "name": plugin.name,
                        "package": plugin.dist.name,
                        "version": plugin.dist.version,
                        "class": get_class_fullname(type(plugin_class)),
                    }
                )
                for k in max_sizes:
                    max_sizes[k] = max(max_sizes[k], len(plugin_objs[-1][k]))
        format_string = (
            "{:<"
            + str(max(max_sizes["name"] + 2, 6))
            + "}"
            + "{:<"
            + str(max(max_sizes["package"] + 2, 9))
            + "}"
            + "{:<"
            + str(max(max_sizes["version"] + 2, 9))
            + "}"
            + "{:<"
            + str(max(max_sizes["class"], 10))
            + "}"
        )
        print(format_string.format("NAME", "PACKAGE", "VERSION", "CLASS_NAME"))
        for plugin_obj in plugin_objs:
            print(
                format_string.format(
                    plugin_obj["name"],
                    plugin_obj["package"],
                    plugin_obj["version"],
                    plugin_obj["class"],
                )
            )
    else:
        print("No StreamFlow plugins detected.", file=sys.stderr)


def load_extensions():
    plugins = entry_points(group=PLUGIN_ENTRY_POINT)
    for plugin in plugins:
        plugin = (plugin.load())()
        if isinstance(plugin, StreamFlowPlugin):
            plugin.register()
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    f"Successfully registered plugin {get_class_fullname(type(plugin))}"
                )
        else:
            raise InvalidPluginException(
                "StreamFlow plugins must extend the streamflow.ext.StreamFlowPlugin class"
            )


def show_extension(
    plugin: str, name: str | None, type_: str | None, show_schema: bool = False
):
    if plugins := entry_points(name=plugin, group=PLUGIN_ENTRY_POINT):
        plugin = plugins[plugin]
        plugin_class = (plugin.load())()
        if isinstance(plugin_class, StreamFlowPlugin):
            plugin_class.register()
            print(f"NAME: {plugin.name}")
            print(f"PACKAGE: {plugin.dist.name}")
            print(f"VERSION: {plugin.dist.version}")
            print(f"CLASS_NAME: {get_class_fullname(type(plugin_class))}\n")
            plugin_classes = plugin_class.classes_
            if type_ is not None:
                plugin_classes = {k: v for k, v in plugin_classes.items() if k == type_}
                if len(plugin_classes) == 0:
                    print(
                        f"It does not provide any StreamFlow extension of type `{type_}`"
                    )
                else:
                    if name is not None:
                        plugin_classes = _filter_by_name(plugin_classes, name)
                        if len(plugin_classes) == 0:
                            print(
                                f"It does not provide any StreamFlow extension of type `{type_}` with name `{name}`"
                            )
            elif name is not None:
                plugin_classes = _filter_by_name(plugin_classes, name)
                if len(plugin_classes) == 0:
                    print(
                        f"It does not provide any StreamFlow extension with name `{name}`"
                    )
            elif len(plugin_classes) == 0:
                print("It does not provide any StreamFlow extension")
            if len(plugin_classes) > 0:
                ext_objs = {}
                max_sizes = {"name": 0, "class": 0}
                for extension_point, items in plugin_classes.items():
                    for item in items:
                        ext_objs.setdefault(extension_point, []).append(
                            {
                                "name": item["name"],
                                "class": get_class_fullname(item["class"]),
                            }
                        )
                        for k in max_sizes:
                            max_sizes[k] = max(
                                max_sizes[k], len(ext_objs[extension_point][-1][k])
                            )
                        if show_schema:
                            entity_schema = item["class"].get_schema()
                            with open(entity_schema) as f:
                                ext_objs[extension_point][-1]["schema"] = jsonref.loads(
                                    f.read(),
                                    base_uri=f"file://{os.path.dirname(entity_schema)}/",
                                    jsonschema=True,
                                )
                format_string = (
                    "{:<"
                    + str(max(max_sizes["name"] + 2, 6))
                    + "}"
                    + "{:<"
                    + str(max(max_sizes["class"] + 2, 10))
                    + "}"
                )
                for extension_point, ext_obj in ext_objs.items():
                    print(
                        f"It provides {len(ext_obj)} "
                        f"extension{'s' if len(ext_obj) > 1 else ''} "
                        f"to the `{extension_point}` extension point:"
                    )
                    print(format_string.format("NAME", "CLASS_NAME"))
                    for ext_point in ext_obj:
                        print(
                            format_string.format(
                                ext_point["name"],
                                ext_point["class"],
                            )
                        )
                if show_schema:
                    print("\nSCHEMAS:")
                    for extension_point, ext_obj in ext_objs.items():
                        for ext_point in ext_obj:
                            title = f"`{ext_point['name']}` {extension_point} ({ext_point['class']})"
                            print(f"\n{title}")
                            print("-" * len(title))
                            property_descs = []
                            for k, v in (
                                ext_point["schema"].get("properties", {}).items()
                            ):
                                if k in ext_point["schema"].get("required", []):
                                    property_descs.append(_get_property_desc(k, v))
                            if property_descs:
                                print("\nREQUIRED\n")
                                print("\n---\n".join(property_descs))
                                property_descs = []
                            for k, v in (
                                ext_point["schema"].get("properties", {}).items()
                            ):
                                if k not in ext_point["schema"].get("required", []):
                                    property_descs.append(_get_property_desc(k, v))
                            if property_descs:
                                print("\nOPTIONAL\n")
                                print("\n---\n".join(property_descs))
        else:
            print(
                "Invalid plugin: it does not extend the streamflow.ext.StreamFlowPlugin class"
            )
    else:
        print(f"No StreamFlow plugin named `{plugin}` detected.", file=sys.stderr)
