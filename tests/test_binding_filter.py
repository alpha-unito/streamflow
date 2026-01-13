from collections.abc import MutableSequence

import pytest

from streamflow.core.deployment import DeploymentConfig, Target
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.workflow import Job, Token
from streamflow.deployment.filter import MatchingBindingFilter
from streamflow.workflow.token import ListToken, ObjectToken
from tests.utils.workflow import BaseFileToken, random_job_name


@pytest.fixture
def standard_job() -> Job:
    return Job(
        name=random_job_name(),
        workflow_id=0,
        inputs={
            "file_type": Token("Hello.java"),
            "compiler": Token("javac"),
            "int_port": Token(100),
            "string_port": Token("valid_data"),
            "unsupported_file": BaseFileToken("dummy.txt"),
            "unsupported_list": ListToken([Token("d1")]),
            "unsupported_object": ObjectToken({"d2": Token("d1")}),
        },
        input_directory=None,
        output_directory=None,
        tmp_directory=None,
    )


@pytest.fixture
def mock_targets() -> MutableSequence[Target]:
    return [
        Target(
            deployment=DeploymentConfig(name="locally", type="local", config={}),
            service=None,
            workdir=None,
        ),
        Target(
            deployment=DeploymentConfig(name="lumi", type="ssh", config={}),
            service=None,
            workdir=None,
        ),
        Target(
            deployment=DeploymentConfig(name="lumi", type="ssh", config={}),
            service="fast",
            workdir=None,
        ),
        Target(
            deployment=DeploymentConfig(name="leonardo", type="ssh", config={}),
            service="boost",
            workdir=None,
        ),
        Target(
            deployment=DeploymentConfig(name="discarded", type="ssh", config={}),
            service=None,
            workdir=None,
        ),
    ]


@pytest.mark.asyncio
async def test_simple_match_rule_success(
    standard_job: Job,
    mock_targets: MutableSequence[Target],
) -> None:
    """Tests a single-condition matching rule with no service requirement."""
    basic_filter = MatchingBindingFilter(
        name="basic_filter",
        filters=[
            {
                "target": "locally",
                "job": [
                    {"port": "file_type", "match": "Hello.java"},
                ],
            }
        ],
    )
    # Rule: Match 'locally' AND file_type='Hello.java'
    filtered_targets = await basic_filter.get_targets(standard_job, mock_targets)
    matched_targets = [(t.deployment.name, t.service) for t in filtered_targets]
    assert len(filtered_targets) == 1
    assert set(matched_targets) == {("locally", None)}


@pytest.mark.asyncio
async def test_and_logic_success(
    standard_job: Job,
    mock_targets: MutableSequence[Target],
) -> None:
    """Tests that a rule with multiple port predicates (AND logic)"""
    and_filter = MatchingBindingFilter(
        name="and_filter",
        filters=[
            {
                "target": {"deployment": "lumi", "service": "fast"},
                "job": [
                    {"port": "file_type", "match": "Hello.java"},
                    {"port": "compiler", "match": "javac"},
                ],
            },
        ],
    )
    # Rule: Match 'lumi' AND service='fast' AND (file_type='Hello.java' AND compiler='javac')
    filtered_targets = await and_filter.get_targets(standard_job, mock_targets)
    matched_targets = [(t.deployment.name, t.service) for t in filtered_targets]
    assert len(filtered_targets) == 1
    assert set(matched_targets) == {("lumi", "fast")}


@pytest.mark.asyncio
async def test_or_logic_success(
    standard_job: Job,
    mock_targets: MutableSequence[Target],
) -> None:
    """
    Tests that a target deployment can be selected by different filter rules (**OR** logic).
    This test also confirms the correct handling of implicit string casting.
    """
    or_filter = MatchingBindingFilter(
        name="or_filter",
        filters=[
            # Rule 1 (no match port values)
            {
                "target": {"deployment": "lumi", "service": "fast"},
                "job": [
                    {"port": "file_type", "match": "Ciao.java"},
                    {"port": "compiler", "match": "javac"},
                ],
            },
            # Rule 2
            {
                "target": {"deployment": "lumi", "service": "fast"},
                "job": [
                    {"port": "file_type", "match": "Hello.java"},
                    {"port": "compiler", "match": "javac"},
                ],
            },
            # Rule 3
            {
                "target": "lumi",
                "job": [
                    {"port": "int_port", "match": "100"},
                ],
            },
            # Rule 4
            {
                "target": "locally",
                "job": [
                    {"port": "file_type", "match": "Hello.java"},
                ],
            },
            # Rule 5 (No Match, service mismatch)
            {
                "target": {"deployment": "lumi", "service": "slow"},
                "job": [
                    {"port": "file_type", "match": "Hello.java"},
                ],
            },
        ],
    )
    filtered_targets = await or_filter.get_targets(standard_job, mock_targets)
    # Expected matches: lumi/fast (from Rule 2 & 3), lumi/None (from Rule 3), and locally/None (from Rule 4)
    matched_targets = [(t.deployment.name, t.service) for t in filtered_targets]
    expected_matches = {("lumi", "fast"), ("lumi", None), ("locally", None)}
    assert len(filtered_targets) == 3
    assert set(matched_targets) == expected_matches


@pytest.mark.asyncio
async def test_no_match_targets(
    standard_job: Job,
    mock_targets: MutableSequence[Target],
) -> None:
    """
    Tests that an exception is raised when the filter finds no matching targets.
    """
    matching_filter = MatchingBindingFilter(
        name="main_filter",
        filters=[
            {
                "target": {"deployment": "lumi", "service": "fast"},
                "job": [
                    {"port": "file_type", "match": "Ciao.java"},
                    {"port": "compiler", "match": "javac"},
                ],
            },
            {"target": "lumi", "job": [{"port": "int_port", "match": "101"}]},
            {
                "target": "locally",
                "job": [{"port": "file_type", "match": "Ciao.java"}],
            },
        ],
    )
    with pytest.raises(WorkflowExecutionException) as excinfo:
        await matching_filter.get_targets(standard_job, mock_targets)
    assert "did not find any matching targets" in str(excinfo.value)


@pytest.mark.asyncio
async def test_missing_job_input(
    standard_job: Job,
    mock_targets: MutableSequence[Target],
) -> None:
    """
    Tests that an exception is raised when a filter rule references a non-existent job input.
    """
    matching_filter = MatchingBindingFilter(
        name="main_filter",
        filters=[
            {
                "target": {"deployment": "lumi", "service": "fast"},
                "job": [
                    {"port": "filename", "match": "Hello.java"},
                    {"port": "compiler", "match": "javac"},
                ],
            },
        ],
    )
    with pytest.raises(ValueError) as excinfo:
        await matching_filter.get_targets(standard_job, mock_targets)
    assert "has no input 'filename'" in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "port_name",
    ["unsupported_file", "unsupported_list", "unsupported_object"],
)
async def test_unsupported_input_type_raises_definition_exception(
    standard_job: Job,
    mock_targets: MutableSequence[Target],
    port_name: str,
) -> None:
    """
    Tests that an exception is raised if a filter attempts to
    match against an unsupported type (FileToken, ListToken, ObjectToken).
    """
    unsupported_filter = MatchingBindingFilter(
        name="type_filter",
        filters=[
            {"target": "lumi", "job": [{"port": port_name, "match": "irrelevant"}]}
        ],
    )
    with pytest.raises(WorkflowDefinitionException) as excinfo:
        await unsupported_filter.get_targets(standard_job, mock_targets)
    assert "cannot be of type 'file', 'list', or 'object'" in str(excinfo.value)
    assert port_name in str(excinfo.value)
