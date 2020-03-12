import logging
import os
from typing import Any, List

from cwltool.pathmapper import PathMapper, MapperEnt, dedup
from cwltool.stdfsaccess import abspath
from cwltool.utils import convert_pathsep_to_unix
from schema_salad import validate
from schema_salad.sourceline import SourceLine
from six.moves import urllib
from typing_extensions import Text

from streamflow.log_handler import _logger


class RemotePathMapper(PathMapper):

    def __init__(self, referenced_files, basedir, stagedir, separateDirs=True):
        # type: (List[Any], Text, Text, bool) -> None
        """Initialize the PathMapper."""
        self._remote_files = []
        super().__init__(referenced_files, basedir, stagedir, separateDirs)

    def remote_files(self) -> List[str]:
        return self._remote_files

    def visit(self, obj, stagedir, basedir, copy=False, staged=False):
        if obj["class"] == "File" and not ("contents" in obj and obj["location"].startswith("_:")):
            with SourceLine(obj, "location", validate.ValidationException, _logger.isEnabledFor(logging.DEBUG)):
                deref = abspath(obj["location"], basedir)
                if urllib.parse.urlsplit(deref).scheme in ['http', 'https']:
                    super().visit(obj, stagedir, basedir, copy, staged)
                else:
                    if os.path.exists(deref):
                        super().visit(obj, stagedir, basedir, copy, staged)
                    else:
                        self._remote_files.append(obj["location"])
                        tgt = convert_pathsep_to_unix(
                            os.path.join(stagedir, obj["basename"]))
                        self._pathmap[obj["location"]] = MapperEnt(
                            deref, tgt, "WritableFile" if copy else "File", staged)
        else:
            super().visit(obj, stagedir, basedir, copy, staged)
