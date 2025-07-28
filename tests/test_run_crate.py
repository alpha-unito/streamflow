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


def _test(wf_name: str, wf_path: str, config_path: str, expected_checksum: str) -> None:
    workdir = Path(tempfile.gettempdir(), utils.random_name())
    workdir.mkdir(parents=True, exist_ok=True)
    sf_path = create_file(
        get_streamflow_config()
        | {
            "database": {
                "type": "sqlite",
                "config": {
                    "connection": os.path.join(workdir, "sqlite.db"),
                },
            }
        },
        "yaml",
        workdir=str(workdir),
    )
    try:
        # Execute workflow
        assert (
            cwl_runner(
                [
                    wf_path,
                    config_path,
                    "--streamflow-file",
                    sf_path,
                    "--name",
                    wf_name,
                ]
            )
            == 0
        )
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
            assert (
                hashlib.sha1(fd_cons.read().encode("utf-8")).hexdigest()
                == expected_checksum
            )
    finally:
        shutil.rmtree(workdir)


def test_scatter() -> None:
    """Test runcrate on a scatter workflow."""
    _test(
        wf_name="wf_scatter_two_dotproduct",
        wf_path=get_data("tests/wf/scatter-wf4.cwl"),
        config_path=get_data("tests/wf/scatter-job2.json"),
        expected_checksum="f35e3ff32e0c560efe4478c66a4992b75d8701a6",
    )


def test_loop() -> None:
    """Test runcrate on a loop workflow."""
    _test(
        wf_name="wf_loop_value_from",
        wf_path=get_data("tests/loop-ext/value-from-loop.cwl"),
        config_path=get_data("tests/loop-ext/two-vars-loop-job.yml"),
        expected_checksum="",
    )
