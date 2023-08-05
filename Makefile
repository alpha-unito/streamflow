codespell:
	codespell -w $(shell git ls-files | grep -v streamflow/cwl/antlr)

codespell-check:
	codespell $(shell git ls-files | grep -v streamflow/cwl/antlr)

coverage.xml: testcov
	coverage xml

coverage-report: testcov
	coverage report

flake8:
	flake8 --exclude streamflow/cwl/antlr streamflow tests

format:
	black streamflow tests

format-check:
	black --diff --check streamflow tests

pyupgrade:
	pyupgrade --py3-only --py38-plus $(shell git ls-files | grep .py | grep -v streamflow/cwl/antlr)

test:
	python -m pytest -rs ${PYTEST_EXTRA}

testcov:
	python -m pytest -rs --cov --cov-report= ${PYTEST_EXTRA}