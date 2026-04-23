codespell:
	codespell -w $(shell git ls-files ':!streamflow/cwl/antlr')

codespell-check:
	codespell $(shell git ls-files ':!streamflow/cwl/antlr')

coverage.xml: testcov
	coverage xml

coverage-report: testcov
	coverage report

format:
	ruff check --fix streamflow tests
	black --target-version py310 streamflow tests

format-check:
	ruff check streamflow tests
	black --target-version py310 --diff --check streamflow tests

pyupgrade:
	pyupgrade --py3-only --py310-plus $(shell git ls-files '*.py' ':!streamflow/cwl/antlr')

test:
	python -m pytest -rs ${PYTEST_EXTRA}

testcov:
	python -m pytest -s --cov --junitxml=junit.xml -o junit_family=legacy --cov-report= ${PYTEST_EXTRA}

typing:
	mypy streamflow tests
