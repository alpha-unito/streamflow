.PHONY: benchmark codespell codespell-check coverage-report flake8 format format-check pyupgrade test testcov typing

benchmark:
	python -m pytest benchmark -rs ${PYTEST_EXTRA}

codespell:
	codespell -w $(shell git ls-files ':!streamflow/cwl/antlr')

codespell-check:
	codespell $(shell git ls-files ':!streamflow/cwl/antlr')

coverage.xml: testcov
	coverage xml

coverage-report: testcov
	coverage report

format:
	ruff check --fix streamflow tests docs/source/conf.py docs/tests benchmark
	black --target-version py311 streamflow tests docs/source/conf.py docs/tests benchmark

format-check:
	ruff check streamflow tests docs/source/conf.py docs/tests benchmark
	black --target-version py311 --diff --check streamflow tests docs/source/conf.py docs/tests benchmark

pyupgrade:
	pyupgrade --py3-only --py311-plus $(shell git ls-files '*.py' ':!streamflow/cwl/antlr')

test:
	python -m pytest tests -rs ${PYTEST_EXTRA}

testcov:
	python -m pytest tests -rs --cov --junitxml=junit.xml -o junit_family=legacy --cov-report= ${PYTEST_EXTRA}

typing:
	mypy streamflow tests docs/source/conf.py docs/tests
