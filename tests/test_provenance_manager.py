from __future__ import annotations

import asyncio
import logging
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import pytest

from streamflow.config.validator import SfValidator
from streamflow.core.workflow import Status
from streamflow.main import build_context, main
from tests.utils.data import get_data_path
from tests.utils.workflow import create_workflow


def _write_streamflow_file(tmp_path: Path) -> Path:
    """Write a minimal valid StreamFlow configuration file for the tests."""
    streamflow_file = tmp_path / "streamflow.yml"
    streamflow_file.write_text(
        "version: v1.0\n"
        "database:\n"
        "  type: default\n"
        "  config:\n"
        f"    connection: {tmp_path / 'streamflow.db'}\n"
    )
    return streamflow_file


async def _seed_executed_workflow(
    streamflow_file: Path, name: str, cwl_file: str
) -> None:
    """Create a COMPLETED workflow execution in the StreamFlow database."""
    context = build_context(SfValidator().validate_file(str(streamflow_file)))
    try:
        workflow, _ = await create_workflow(context, num_port=0, save=False)
        workflow.name = name
        workflow.config["file"] = cwl_file
        await workflow.save(context.database)
        timestamp = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
        await context.database.update_workflow(
            cast(int, workflow.persistent_id),
            {
                "status": Status.COMPLETED.value,
                "start_time": timestamp,
                "end_time": timestamp,
            },
        )
    finally:
        await context.close()


def test_create_archive(tmp_path: Path) -> None:
    """Test that the prov CLI archives a completed workflow into an RO-Crate."""
    streamflow_file = _write_streamflow_file(tmp_path)
    asyncio.run(
        _seed_executed_workflow(
            streamflow_file,
            "test-workflow",
            str(get_data_path("cwl", "example", "main.cwl")),
        )
    )
    archive_path = tmp_path / "out" / "test.crate.zip"
    ret = main(
        [
            "prov",
            "test-workflow",
            "-f",
            str(streamflow_file),
            "-t",
            "run_crate/cwl",
            "--outdir",
            str(archive_path.parent),
            "--name",
            str(archive_path.name),
        ]
    )
    assert ret == 0
    assert archive_path.exists()
    assert "ro-crate-metadata.json" in zipfile.ZipFile(archive_path).namelist()


def test_create_archive_missing_workflow(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the prov CLI rejects a workflow that was never executed."""
    streamflow_file = _write_streamflow_file(tmp_path)
    caplog.set_level(logging.ERROR)
    ret = main(
        [
            "prov",
            "missing-workflow",
            "-f",
            str(streamflow_file),
            "-t",
            "run_crate/cwl",
            "--outdir",
            str(tmp_path / "out"),
        ]
    )
    assert ret != 0
    assert "is not present in the database" in caplog.text


def test_create_archive_unsupported_type(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the prov CLI rejects an unsupported provenance type."""
    streamflow_file = _write_streamflow_file(tmp_path)
    asyncio.run(
        _seed_executed_workflow(
            streamflow_file,
            "test-workflow",
            str(get_data_path("cwl", "example", "main.cwl")),
        )
    )
    caplog.set_level(logging.ERROR)
    ret = main(
        [
            "prov",
            "test-workflow",
            "-f",
            str(streamflow_file),
            "-t",
            "not-prov-type",
            "--outdir",
            str(tmp_path / "out"),
        ]
    )
    assert ret != 0
    assert "provenance format is not supported" in caplog.text
