"""Test the CWL Loop extension.

From https://github.com/common-workflow-language/cwltool/blob/6f0e1d941a61063828aa074bcbaae55bedf05167/tests/test_loop.py).
"""

import json
from typing import MutableMapping, MutableSequence

from cwltool.tests.util import get_data

from streamflow.cwl.runner import main


def test_validate_loop() -> None:
    """Affirm that a loop workflow validates."""
    params = [
        "--validate",
        get_data("tests/loop/single-var-loop.cwl"),
    ]
    assert main(params) == 0


def test_validate_loop_fail_scatter() -> None:
    """Affirm that a loop workflow does not validate if scatter and loop directives are on the same step."""
    params = [
        "--validate",
        get_data("tests/loop/invalid-loop-scatter.cwl"),
    ]
    assert main(params) == 1


def test_validate_loop_fail_when() -> None:
    """Affirm that a loop workflow does not validate if when and loop directives are on the same step."""
    params = [
        "--validate",
        get_data("tests/loop/invalid-loop-when.cwl"),
    ]
    assert main(params) == 1


def test_validate_loop_fail_no_loop_when() -> None:
    """Affirm that a loop workflow does not validate if no loopWhen directive is specified."""
    params = [
        "--validate",
        get_data("tests/loop/invalid-no-loopWhen.cwl"),
    ]
    assert main(params) == 1


def test_validate_loop_fail_on_workflow() -> None:
    """Affirm that a workflow does not validate if it contains a Loop requirement."""
    params = [
        "--validate",
        get_data("tests/loop/invalid-loop-workflow.cwl"),
    ]
    assert main(params) == 1


def test_validate_loop_fail_on_command_line_tool() -> None:
    """Affirm that a CommandLineTool does not validate if it contains a Loop requirement."""
    params = [
        "--validate",
        get_data("tests/loop/invalid-loop-command-line-tool.cwl"),
    ]
    assert main(params) == 1


def test_validate_loop_fail_on_expression_tool() -> None:
    """Affirm that an ExpressionTool does not validate if it contains a Loop requirement."""
    params = [
        "--validate",
        get_data("tests/loop/invalid-loop-expression-tool.cwl"),
    ]
    assert main(params) == 1


def test_validate_loop_fail_on_hint() -> None:
    """Affirm that a loop workflow does not validate if it contains a Loop hint."""
    params = [
        "--validate",
        get_data("tests/loop/invalid-loop-hint.cwl"),
    ]
    assert main(params) == 1


def test_loop_fail_non_boolean_loop_when() -> None:
    """Affirm that a loop workflow fails if loopWhen directive returns a non-boolean value."""
    params = [
        get_data("tests/loop/invalid-non-boolean-loopWhen.cwl"),
        get_data("tests/loop/two-vars-loop-job.yml"),
    ]
    assert main(params) == 1


def test_loop_single_variable(capsys) -> None:
    """Test a simple loop case with a single variable."""
    params = [
        get_data("tests/loop/single-var-loop.cwl"),
        get_data("tests/loop/single-var-loop-job.yml"),
    ]
    main(params)
    expected = {"o1": 10}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_loop_single_variable_no_iteration(capsys) -> None:
    """Test a simple loop case with a single variable and a false loopWhen condition."""
    params = [
        get_data("tests/loop/single-var-loop-no-iteration.cwl"),
        get_data("tests/loop/single-var-loop-job.yml"),
    ]
    main(params)
    expected = {"o1": None}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_loop_two_variables(capsys) -> None:
    """Test a loop case with two variables, which are both back-propagated between iterations."""
    params = [
        get_data("tests/loop/two-vars-loop.cwl"),
        get_data("tests/loop/two-vars-loop-job.yml"),
    ]
    main(params)
    expected = {"o1": 10}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_loop_two_variables_single_backpropagation(capsys) -> None:
    """Test a loop case with two variables, but when only one of them is back-propagated between iterations."""
    params = [
        get_data("tests/loop/two-vars-loop-2.cwl"),
        get_data("tests/loop/two-vars-loop-job.yml"),
    ]
    main(params)
    expected = {"o1": 10}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_loop_with_all_output_method(capsys) -> None:
    """Test a loop case with outputMethod set to all."""
    params = [
        get_data("tests/loop/all-output-loop.cwl"),
        get_data("tests/loop/single-var-loop-job.yml"),
    ]
    main(params)
    expected = {"o1": [2, 3, 4, 5, 6, 7, 8, 9, 10]}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_loop_with_all_output_method_no_iteration(capsys) -> None:
    """Test a loop case with outputMethod set to all and a false loopWhen condition."""
    params = [
        get_data("tests/loop/all-output-loop-no-iteration.cwl"),
        get_data("tests/loop/single-var-loop-job.yml"),
    ]
    main(params)
    expected: MutableMapping[str, MutableSequence[int]] = {"o1": []}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_loop_value_from(capsys) -> None:
    """Test a loop case with a variable generated by a valueFrom directive."""
    params = [
        get_data("tests/loop/value-from-loop.cwl"),
        get_data("tests/loop/two-vars-loop-job.yml"),
    ]
    main(params)
    expected = {"o1": 10}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_loop_value_from_fail_no_requirement() -> None:
    """Test that a workflow loop fails if a valueFrom directive is specified without StepInputExpressionRequirement."""
    params = [
        get_data("tests/loop/invalid-value-from-loop-no-requirement.cwl"),
        get_data("tests/loop/two-vars-loop-job.yml"),
    ]
    assert main(params) == 1


def test_loop_inside_scatter(capsys) -> None:
    """Test a loop subworkflow inside a scatter step."""
    params = [
        get_data("tests/loop/loop-inside-scatter.cwl"),
        get_data("tests/loop/loop-inside-scatter-job.yml"),
    ]
    main(params)
    expected = {"o1": [10, 10, 10, 10, 10]}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_nested_loops(capsys) -> None:
    """Test a workflow with two nested loops."""
    params = [
        get_data("tests/loop/loop-inside-loop.cwl"),
        get_data("tests/loop/two-vars-loop-job.yml"),
    ]
    main(params)
    expected = {"o1": [2, 3, 4]}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_nested_loops_all(capsys) -> None:
    """Test a workflow with two nested loops, both with outputMethod set to all."""
    params = [
        get_data("tests/loop/loop-inside-loop-all.cwl"),
        get_data("tests/loop/two-vars-loop-job.yml"),
    ]
    main(params)
    expected = {"o1": [[2], [2, 3], [2, 3, 4]]}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_multi_source_loop_input(capsys) -> None:
    """Test a loop with two sources, which are selected through a pickValue directive."""
    params = [
        get_data("tests/loop/multi-source-loop.cwl"),
        get_data("tests/loop/single-var-loop-job.yml"),
    ]
    main(params)
    expected = {"o1": [2, 3, 4, 5, 8, 11, 14, 17, 20]}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected


def test_multi_source_loop_input_fail_no_requirement() -> None:
    """Test that a loop with two sources fails without MultipleInputFeatureRequirement."""
    params = [
        get_data("tests/loop/invalid-multi-source-loop-no-requirement.cwl"),
        get_data("tests/loop/single-var-loop-job.yml"),
    ]
    assert main(params) == 1


def test_default_value_loop(capsys) -> None:
    """Test a loop whose source has a default value."""
    params = [
        get_data("tests/loop/default-value-loop.cwl"),
        get_data("tests/loop/single-var-loop-job.yml"),
    ]
    main(params)
    expected = {"o1": [8, 11, 14, 17, 20]}
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected
