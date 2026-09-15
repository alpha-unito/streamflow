import hashlib
import os
from tempfile import TemporaryDirectory

import pytest
import pytest_asyncio

from streamflow.core.context import StreamFlowContext
from streamflow.main import build_context
from streamflow.parser import report_parser
from streamflow.report import create_report
from tests.utils.data import get_data_path


@pytest_asyncio.fixture(scope="session")
async def context() -> StreamFlowContext:
    _context = build_context(
        {
            "database": {
                "type": "sqlite",
                "config": {
                    "connection": os.path.join(get_data_path(), "sqlite", "sqlite.db")
                },
            },
            "path": os.getcwd(),
        }
    )
    yield _context
    await _context.close()


@pytest.mark.asyncio
async def test_single_workflow_single_execution(
    context: StreamFlowContext,
) -> None:
    """Test producing a report for a single execution of a workflow."""
    digests = {
        "csv": "05ea34f00c0351d7812395abb175b6f3a98e131c4fcbc82053a940a853e08582",
        "json": "42d5dc4158afac546ae85b436e95873d3f611be1da90b005b95796cb8d95e09a",
    }
    with TemporaryDirectory() as tmpdir:
        await create_report(
            context,
            report_parser.parse_args(
                [
                    "wf_scatter_two_dotproduct",
                    "--outdir",
                    tmpdir,
                    "--format",
                ]
                + list(digests.keys())
            ),
        )
        for fmt in digests.keys():
            report = os.path.join(tmpdir, f"report.{fmt}")
            assert os.path.exists(report)
            assert hashlib.sha256(open(report, "rb").read()).hexdigest() == digests[fmt]


@pytest.mark.asyncio
async def test_single_workflow_group_by_step(
    context: StreamFlowContext,
) -> None:
    """Test producing a report for a single execution of a workflow grouped by step."""
    digests = {
        "csv": "05ea34f00c0351d7812395abb175b6f3a98e131c4fcbc82053a940a853e08582",
        "json": "635d13087c8170abcb5a1e38b92df3eed7590efd0d4f05e5c93a314e4862ca66",
    }
    with TemporaryDirectory() as tmpdir:
        await create_report(
            context,
            report_parser.parse_args(
                [
                    "wf_scatter_two_dotproduct",
                    "--outdir",
                    tmpdir,
                    "--group-by-step",
                    "--format",
                ]
                + list(digests.keys())
            ),
        )
        for fmt in digests.keys():
            report = os.path.join(tmpdir, f"report.{fmt}")
            assert os.path.exists(report)
            assert hashlib.sha256(open(report, "rb").read()).hexdigest() == digests[fmt]


@pytest.mark.asyncio
async def test_single_workflow_all_instances(context: StreamFlowContext) -> None:
    """Test producing a report for all executions of a workflow."""
    digests = {
        "csv": "aeb96671257cf1ec501e8c33b568d69c98948a7cd72e1b8c048015a7faf71054",
        "json": "119aed0fcadd3210310fac5d713b6191768513a8cca6f4ae47076bd905eeee07",
    }
    with TemporaryDirectory() as tmpdir:
        await create_report(
            context,
            report_parser.parse_args(
                [
                    "wf_scatter_two_dotproduct",
                    "--outdir",
                    tmpdir,
                    "--all",
                    "--format",
                ]
                + list(digests.keys())
            ),
        )
        for fmt in digests.keys():
            report = os.path.join(tmpdir, f"report.{fmt}")
            assert os.path.exists(report)
            assert hashlib.sha256(open(report, "rb").read()).hexdigest() == digests[fmt]


@pytest.mark.asyncio
async def test_multiple_workflows(context: StreamFlowContext) -> None:
    """Test producing a report for multiple workflows."""
    digests = {
        "csv": "f604f9cf7af0841d667a6e549c7fd2125b2d30b19fd85abeef18ec3f27291ad8",
        "json": "e65e81e87a1af032fd8daf110ec510c22f88a8a2cde3be0dbcabb0c0664201b7",
    }
    with TemporaryDirectory() as tmpdir:
        await create_report(
            context,
            report_parser.parse_args(
                [
                    "wf_scatter_two_dotproduct,wf_scatter_dotproduct_twoempty",
                    "--outdir",
                    tmpdir,
                    "--format",
                ]
                + list(digests.keys())
            ),
        )
        for fmt in digests.keys():
            report = os.path.join(tmpdir, f"report.{fmt}")
            assert os.path.exists(report)
            assert hashlib.sha256(open(report, "rb").read()).hexdigest() == digests[fmt]
