import hashlib
import io
import os
import re
import shutil
import tempfile
from pathlib import Path

from cwltool.tests.util import get_data
from runcrate.report import dump_crate_actions

from streamflow.core import utils
from streamflow.cwl.runner import main as cwl_runner
from streamflow.main import main as sf_main
from tests.utils.utils import create_file, get_streamflow_config



def test_scatter() -> None:
    """Test runcrate on a scatter workflow."""
    workdir = Path(tempfile.gettempdir(), utils.random_name())
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = os.path.join(workdir, "sqlite.db")
    wf_name = "wf_scatter_two_dotproduct"
    sf_path = create_file(
        get_streamflow_config()
        | {
            "database": {
                "type": "sqlite",
                "config": {
                    "connection": db_path,
                },
            }
        },
        "yaml",
        workdir=str(workdir),
    )
    try:
        params = [
            get_data("tests/wf/scatter-wf4.cwl"),
            get_data("tests/wf/scatter-job2.json"),
            "--streamflow-file",
            sf_path,
            "--name",
            wf_name,
        ]
        assert cwl_runner(params) == 0
        # Generate archive
        assert (
            sf_main(
                [
                    "prov",
                    wf_name,
                    "--file",
                    sf_path,
                    "--outdir",
                    str(workdir),
                ]
            )
            == 0
        )
        # Create report
        with io.StringIO() as fd, io.StringIO() as fd_cons:
            dump_crate_actions(os.path.join(workdir, f"{wf_name}.crate.zip"), fd)
            fd.seek(0)
            for line in fd.readlines():
                # Replace variable attributes (e.g. timestamp)
                if re.match(
                    r"action: #([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\n",
                    line,
                ):
                    line = "action: #defaultidentifier\n"
                elif m := re.match(
                    r"(\s+)(started:\s\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+\d{2}:\d{2}\n)",
                    line,
                ):
                    line = f"{m.group(1)}started: timestamp\n"
                elif m := re.match(
                    r"(\s+)ended:\s\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+\d{2}:\d{2}\n",
                    line,
                ):
                    line = f"{m.group(1)}ended: timestamp\n"
                fd_cons.write(line)
            # Checksum report
            fd_cons.seek(0)
            sha1_checksum = hashlib.new("sha1", usedforsecurity=False)
            while data := fd_cons.read(2**16):
                sha1_checksum.update(data.encode())
            checksum = sha1_checksum.hexdigest()
            assert checksum == "f35e3ff32e0c560efe4478c66a4992b75d8701a6"
    finally:
        shutil.rmtree(workdir)
