from __future__ import annotations

import json
import logging
import sys
from pathlib import PurePosixPath
from typing import Any, MutableMapping, MutableSequence

from importlib_metadata import entry_points
from referencing._core import Resolver, Resource

from streamflow.config.schema import SfSchema
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


def _flatten_all_of(entity_schema):
    for obj in entity_schema["allOf"]:
        if "allOf" in obj:
            obj["properties"] = {**_flatten_all_of(obj), **obj.get("properties", {})}
        entity_schema["properties"] = {
            **obj.get("properties", {}),
            **entity_schema.get("properties", {}),
        }
    del entity_schema["allOf"]
    return dict(sorted(entity_schema["properties"].items()))


def _get_property_desc(
    k: str, obj: MutableMapping[str, Any], refs: MutableMapping[str, Any]
) -> str:
    property_desc = [k]
    if "type" in obj:
        property_desc[0] = f"{property_desc[0]}: {_get_type_repr(obj, refs)}"
    elif "oneOf" in obj:
        types = [_get_type_repr(oo, refs) for oo in obj["oneOf"] if "type" in oo]
        property_desc[0] = f"{property_desc[0]}: Union[{', '.join(types)}]"
    if "default" in obj:
        property_desc[0] = f"{property_desc[0]} (default: {obj['default']})"
    if "description" in obj:
        property_desc.append(obj["description"])
    return "\n".join(property_desc)


def _replace_refs(contents: Any, resolver: Resolver):
    refs = {}
    _resolve_refs(contents, resolver, PurePosixPath("/"), refs)
    refs = {k: v for k, v in sorted(refs.items())}
    for k, v in refs.items():
        path = PurePosixPath(k)
        element = contents
        for part in path.parts[1:]:
            if isinstance(element, MutableMapping):
                element = element[part]
            elif isinstance(element, MutableSequence):
                element = element[int(part)]
        element.update(v)


def _resolve_refs(
    contents: Any,
    resolver: Resolver,
    path: PurePosixPath,
    refs: MutableMapping[str, Any],
):
    if isinstance(contents, MutableMapping):
        for k, v in contents.items():
            _resolve_refs(v, resolver, path / k, refs)
    elif isinstance(contents, MutableSequence) and not isinstance(contents, str):
        for i, v in enumerate(contents):
            _resolve_refs(v, resolver, path / str(i), refs)
    if isinstance(contents, MutableMapping) and isinstance(contents.get("$ref"), str):
        resolved = resolver.lookup(contents.pop("$ref"))
        _resolve_refs(resolved.contents, resolved.resolver, path, refs)
        refs[path.as_posix()] = resolved.contents


def _get_type_repr(
    obj: MutableMapping[str, Any], refs: MutableMapping[str, Any]
) -> str | None:
    if "type" in obj:
        if obj["type"] == "object":
            if "patternProperties" in obj:
                if len(obj["patternProperties"]) > 1:
                    types = [
                        _get_type_repr(t, refs)
                        for t in obj["patternProperties"].values()
                    ]
                    type_ = f"Union[{', '.join(types)}]"
                else:
                    type_ = _get_type_repr(
                        list(obj["patternProperties"].values())[0], refs
                    )
                return f"Map[str, {type_}]"
            elif "title" in obj:
                refs[obj["title"]] = obj.get("properties", {})
                return obj["title"]
            else:
                return "object"
        else:
            return obj["type"]
    else:
        return None


def _split_refs(refs: MutableMapping[str, Any], processed: MutableSequence[str]):
    refs_descs = {}
    subrefs = {}
    for k, v in refs.items():
        refs_descs[k] = [
            _get_property_desc(name, prop, subrefs) for name, prop in v.items()
        ]
        processed.append(k)
    if subrefs := {k: v for k, v in subrefs.items() if k not in processed}:
        refs_descs = {**refs_descs, **_split_refs(subrefs, processed)}
    return refs_descs


def _split_schema(schema: MutableMapping[str, Any]):
    required, optional = [], []
    refs = {}
    for k, v in schema.get("properties", {}).items():
        if k in schema.get("required", []):
            required.append(_get_property_desc(k, v, refs))
        else:
            optional.append(_get_property_desc(k, v, refs))
    return required, optional, refs


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
        schema = SfSchema()
        with open(entity_schema) as f:
            resource = Resource.from_contents(json.load(f))
            entity_schema = resource.contents
        _replace_refs(entity_schema, schema.registry.resolver(base_uri=resource.id()))
        if "allOf" in entity_schema:
            entity_schema["properties"] = _flatten_all_of(entity_schema)
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
        required, optional, refs = _split_schema(entity_schema)
        if required:
            print("\n===================\nREQUIRED PROPERTIES\n===================\n")
            print("\n---\n".join(required))
        if optional:
            print("\n===================\nOPTIONAL PROPERTIES\n===================\n")
            print("\n---\n".join(optional))
        if refs:
            print("\n================\nTYPE DEFINITIONS\n================")
            refs = _split_refs(refs, [])
            for key, ref in refs.items():
                print("\n" + "-" * len(key))
                print(key)
                print("-" * len(key) + "\n")
                print("\n---\n".join(ref))
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
