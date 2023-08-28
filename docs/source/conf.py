# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------
import importlib
import os.path

project = 'StreamFlow'
copyright = '2023, Alpha Research Group, Computer Science Dept., University of Torino'
author = 'Iacopo Colonnelli'
version = '0.2'
release = '0.2.0'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.extlinks',
    'sphinx-jsonschema',
    'sphinx_rtd_theme'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = [
]


# -- Options for HTML output -------------------------------------------------

html_logo = 'images/streamflow_logo.png'

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


def setup(app):
    app.add_css_file('theme_overrides.css')


# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    "logo_only": True
}

extlinks = {
    'config-schema': ('https://raw.githubusercontent.com/alpha-unito/streamflow/' + release +
                      '/streamflow/config/schemas/v1.0/%s', 'GH#'),
    'repo': ('https://github.com/alpha-unito/streamflow/tree/' + release + '/%s', 'GH#')
}

# JSONSchema extensions
sjs = importlib.import_module("sphinx-jsonschema")
sjs_wide_format = importlib.import_module("sphinx-jsonschema.wide_format")


def _patched_simpletype(self, schema):
    rows = []
    if 'title' in schema and (not self.options['lift_title'] or self.nesting > 1):
        rows.append(self._line(self._cell('*' + schema['title'] + '*')))
        del schema['title']
    self._check_description(schema, rows)
    if 'type' in schema:
        if '$ref' in schema:
            ref = self._reference(schema)
            rows.extend(self._prepend(self._cell('type'), ref))
            del schema['type']
        elif type(schema['type']) == list:
            cells = [self._line(self._decodetype(t)) for t in schema['type']]
            rows.extend(self._prepend(self._cell('type'), cells))
            del schema['type']
    rows.extend(_original_simpletype(self, schema))
    return rows


_original_simpletype = sjs_wide_format.WideFormat._simpletype
sjs_wide_format.WideFormat._simpletype = _patched_simpletype


def _patched_arraytype(self, schema):
    if 'items' in schema:
        if type(schema['items']) == list:
            return _original_arraytype(self, schema)
        else:
            schema['unique'] = 'uniqueItems' in schema['items']
            if 'type' in schema['items']:
                schema['type'] = schema['items']['type'] + '[]'
                rows = self._simpletype(schema)
                return rows
            else:
                rows = _original_arraytype(self, schema)
                rows.extend(self._bool_or_object(schema, 'unique'))
            return rows
    else:
        return _original_arraytype(self, schema)


_original_arraytype = sjs_wide_format.WideFormat._arraytype
sjs_wide_format.WideFormat._arraytype = _patched_arraytype


def _patched_objectproperties(self, schema, key):
    rows = []
    if key in schema:
        rows.append(self._line(self._cell(key)))

        for prop in schema[key].keys():
            # insert spaces around the regexp OR operator
            # allowing the regexp to be split over multiple lines.
            proplist = prop.split('|')
            dispprop = self._escape(' | '.join(proplist))
            if 'required' in schema:
                if prop in schema['required']:
                    dispprop = f'**{dispprop}**\n(required)'
            label = self._cell(dispprop)

            if isinstance(schema[key][prop], dict):
                obj = schema[key][prop]
                rows.extend(self._dispatch(obj, label)[0])
            else:
                rows.append(self._line(label, self._cell(schema[key][prop])))
        del schema[key]
    return rows


sjs_wide_format.WideFormat._objectproperties = _patched_objectproperties


def _patched_complexstructures(self, schema):
    rows = []
    if 'oneOf' in schema:
        types = []
        for obj in schema['oneOf']:
            if 'type' in obj:
                if obj['type'] == 'object' and '$ref' in obj:
                    types.extend(self._reference(obj))
                else:
                    types.append(self._line(self._decodetype(obj['type'])))
                del obj['type']
        if not list(filter(bool, schema['oneOf'])):
            del schema['oneOf']
        rows.extend(self._prepend(self._cell('type'), types))
    rows.extend(_original_complexstructures(self, schema))
    return rows


_original_complexstructures = sjs_wide_format.WideFormat._complexstructures
sjs_wide_format.WideFormat._complexstructures = _patched_complexstructures


def patched_transform(self, schema):
    table, definitions = original_transform(self, schema)
    table['classes'] += ['jsonschema-table']
    return table, definitions


original_transform = sjs_wide_format.WideFormat.transform
sjs_wide_format.WideFormat.transform = patched_transform


def patched_run(self, schema, pointer=''):
    if 'id' in schema:
        del schema['id']
    elif '$id' in schema:
        del schema['$id']
    if 'type' in schema:
        del schema['type']
    if 'additionalProperties' in schema:
        del schema['additionalProperties']
    if 'required' in schema and 'properties' in schema:
        props = {}
        for prop in schema['required']:
            if prop in schema['properties']:
                props[prop] = schema['properties'][prop]
                del schema['properties'][prop]
        schema['properties'] = {**props, **schema['properties']}
    return original_run(self, schema, pointer)


original_run = sjs_wide_format.WideFormat.run
sjs_wide_format.WideFormat.run = patched_run


def patched_get_json_data(self):
    schema, source, pointer = original_get_json_data(self)

    if self.arguments:
        filename, pointer = self._splitpointer(self.arguments[0])
    else:
        filename, pointer = None, ''

    if 'allOf' in schema:
        for obj in schema['allOf']:
            if '$ref' in obj:
                target_file = os.path.join(os.path.dirname(filename), obj['$ref'])
                target_schema, _ = self.from_file(target_file)
                target_schema = self.ordered_load(target_schema)
                schema['properties'] = {**target_schema.get('properties', {}), **schema['properties']}
        del schema['allOf']
        schema['properties'] = dict(sorted(schema['properties'].items()))
    return schema, source, pointer


original_get_json_data = sjs.JsonSchema.get_json_data
sjs.JsonSchema.get_json_data = patched_get_json_data