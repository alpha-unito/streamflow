from __future__ import annotations

import logging
import os
import sys
from typing import Any, MutableMapping

import jsonref
from importlib_metadata import entry_points

from streamflow.core.exception import InvalidPluginException
from streamflow.core.utils import get_class_fullname
from streamflow.ext.plugin import StreamFlowPlugin, extension_points
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


def list_extensions(name: str | None, type_: str | None):
    extensions = {}
    max_sizes = {
        "name": 0,
        "class": 0,
        "plugin": 0,
    }
    for ext, classes in extension_points.items():
        if type_ is None or ext == type_:
            for n, v in classes.items():
                if name is None or name == n:
                    extensions.setdefault(ext, {})[n] = {
                        "name": n,
                        "class": get_class_fullname(v),
                        "plugin": "-",
                    }
                    for k in max_sizes:
                        max_sizes[k] = max(max_sizes[k], len(extensions[ext][n][k]))
    if plugins := entry_points(group=PLUGIN_ENTRY_POINT):
        for plugin in plugins:
            plugin_class = (plugin.load())()
            plugin_class.register()
            if isinstance(plugin_class, StreamFlowPlugin):
                plugin_classes = (
                    {k: v for k, v in plugin_class.classes_.items() if k == type_}
                    if type_ is not None
                    else plugin_class.classes_
                )
                if name is not None:
                    plugin_classes = _filter_by_name(plugin_classes, name)
                for ext, items in plugin_classes.items():
                    for item in items:
                        extensions.setdefault(ext, {})[item["name"]] = {
                            "name": item["name"],
                            "class": get_class_fullname(item["class"]),
                            "plugin": plugin.name,
                        }
                        for k in max_sizes:
                            max_sizes[k] = max(
                                max_sizes[k], len(extensions[ext][item["name"]][k])
                            )
    format_string = (
        "{:<"
        + str(max(max_sizes["name"] + 2, 6))
        + "}"
        + "{:<"
        + str(max(max_sizes["class"] + 2, 12))
        + "}"
        + "{:<"
        + str(max(max_sizes["plugin"], 8))
        + "}"
    )
    if extensions:
        for ext, items in extensions.items():
            if type_ is None:
                print(
                    f"There {'are' if len(items) > 1 else 'is'} "
                    f"{len(items)} available extension{'s' if len(items) > 1 else ''} "
                    f"for the `{ext}` extension point:"
                )
            print(format_string.format("NAME", "CLASS_NAME", "PLUGIN"))
            for item in items.values():
                print(
                    format_string.format(
                        item["name"],
                        item["class"],
                        item["plugin"],
                    )
                )
            print("")
    else:
        print(
            "There are no available StreamFlow extensions{type}{sep}{name}.".format(
                type=f" of type `{type_}`" if type_ is not None else "",
                sep=" and" if type_ is not None and name is not None else "",
                name=f" with name `{name}`" if name is not None else "",
            ),
            file=sys.stderr,
        )


def list_plugins():
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


def show_extension(name: str, type_: str):
    plugin = "-"
    if name in extension_points[type_]:
        class_ = extension_points[type_][name]
    else:
        class_ = None
        for plugin_obj in entry_points(group=PLUGIN_ENTRY_POINT):
            plugin_class = (plugin_obj.load())()
            if isinstance(plugin_class, StreamFlowPlugin):
                plugin_class.register()
                plugin_classes = _filter_by_name(
                    {k: v for k, v in plugin_class.classes_.items() if k == type_}, name
                )
                for item in plugin_classes.get(type_, []):
                    if item["name"] == name:
                        class_ = item["class"]
                        plugin = plugin_obj.name
    if class_ is not None:
        class_name = get_class_fullname(class_)
        entity_schema = class_.get_schema()
        with open(entity_schema) as f:
            entity_schema = jsonref.loads(
                f.read(),
                base_uri=f"file://{os.path.dirname(entity_schema)}/",
                jsonschema=True,
            )
        format_string = (
            "{:<"
            + str(max(len(name) + 2, 6))
            + "}"
            + "{:<"
            + str(max(len(class_name) + 2, 10))
            + "}"
            + "{:<"
            + str(max(len(plugin), 8))
            + "}"
        )
        print(format_string.format("NAME", "CLASS_NAME", "PLUGIN"))
        print(format_string.format(name, class_name, plugin))
        property_descs = []
        for k, v in entity_schema.get("properties", {}).items():
            if k in entity_schema.get("required", []):
                property_descs.append(_get_property_desc(k, v))
        if property_descs:
            print("\nREQUIRED\n")
            print("\n---\n".join(property_descs))
            property_descs = []
        for k, v in entity_schema.get("properties", {}).items():
            if k not in entity_schema.get("required", []):
                property_descs.append(_get_property_desc(k, v))
        if property_descs:
            print("\nOPTIONAL\n")
            print("\n---\n".join(property_descs))
    else:
        print(
            f"No StreamFlow extension `{name}` of type `{type_}` detected.",
            file=sys.stderr,
        )


def show_plugin(plugin: str):
    if plugins := entry_points(name=plugin, group=PLUGIN_ENTRY_POINT):
        plugin_obj = plugins[plugin]
        plugin_class = (plugin_obj.load())()
        if isinstance(plugin_class, StreamFlowPlugin):
            plugin_class.register()
            print(f"NAME: {plugin_obj.name}")
            print(f"PACKAGE: {plugin_obj.dist.name}")
            print(f"VERSION: {plugin_obj.dist.version}")
            print(f"CLASS_NAME: {get_class_fullname(type(plugin_class))}\n")
            classes = plugin_class.classes_
            if len(classes) == 0:
                print("It does not provide any StreamFlow extension")
            else:
                ext_objs = {}
                max_sizes = {"name": 0, "class": 0}
                for extension_point, items in classes.items():
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
        else:
            print(
                "Invalid plugin: it does not extend the streamflow.ext.StreamFlowPlugin class"
            )
    else:
        print(f"No StreamFlow plugin named `{plugin}` detected.", file=sys.stderr)
