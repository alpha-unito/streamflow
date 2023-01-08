MODULE=streamflow

PYSOURCES=$(wildcard ${MODULE}/**.py tests/*.py) setup.py
SCHEMAS=$(wildcard ${MODULE}/**/schemas)


codespell:
	codespell -w $(shell git ls-files | grep -v streamflow/cwl/antlr)

codespell-check:
	codespell $(shell git ls-files | grep -v streamflow/cwl/antlr)

coverage.xml: testcov
	coverage xml

coverage-report: testcov
	coverage report

flake8: $(PYSOURCES)
	flake8 $^

format:
	black --exclude streamflow/cwl/antlr setup.py streamflow tests

format-check:
	black --diff --check --exclude streamflow/cwl/antlr setup.py streamflow tests

test: $(PYSOURCES)
	python -m pytest -rs ${PYTEST_EXTRA}

testcov: $(PYSOURCES)
	python -m pytest -rs --cov --cov-config=.coveragerc --cov-report= ${PYTEST_EXTRA}