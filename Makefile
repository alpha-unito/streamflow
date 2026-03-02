.PHONY: benchmark codespell codespell-check coverage-report flake8 format format-check pyupgrade test testcov typing

benchmark:
	python -m pytest benchmark -rs ${PYTEST_EXTRA}

codespell:
	codespell -w $(shell git ls-files | grep -v streamflow/cwl/antlr)

codespell-check:
	codespell $(shell git ls-files | grep -v streamflow/cwl/antlr)

coverage.xml: testcov
	coverage xml

coverage-report: testcov
	coverage report

flake8:
	flake8 --exclude streamflow/cwl/antlr streamflow tests benchmark

format:
	isort streamflow tests benchmark
	black streamflow tests benchmark

format-check:
	isort --check-only streamflow tests benchmark
	black --diff --check streamflow tests benchmark

pyupgrade:
	pyupgrade --py3-only --py310-plus $(shell git ls-files | grep .py | grep -v streamflow/cwl/antlr)

test:
	python -m pytest tests -rs ${PYTEST_EXTRA}

testcov:
	python -m pytest tests -rs --cov --junitxml=junit.xml -o junit_family=legacy --cov-report= ${PYTEST_EXTRA}

typing:
	mypy streamflow tests