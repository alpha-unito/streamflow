#!/bin/bash

venv() {
  if ! test -d "$1" ; then
	  if command -v virtualenv > /dev/null; then
      virtualenv -p python3 "$1"
	  else
	    python3 -m venv "$1"
	  fi
  fi
  source "$1"/bin/activate
}

# Version of the standard to test against
# Current options: v1.0, v1.1, v1.2
VERSION=${VERSION:-"v1.2"}

# Which commit of the standard's repo to use
# Defaults to the last commit of the main branch
COMMIT=${COMMIT:-"main"}

# Comma-separated list of test names that should be excluded from execution
# Defaults to "docker_entrypoint, inplace_update_on_file_content"
EXCLUDE=${EXCLUDE:-"docker_entrypoint,modify_file_content"}

# Name of the CWLDockerTranslator plugin to use for test execution
# This parameter allows to test automatic CWL requirements translators
DOCKER=${DOCKER:-"docker"}

# Additional arguments for the pytest command
# Defaults to none
# PYTEST_EXTRA=

# The directory where this script resides
SCRIPT_DIRECTORY="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Download archive from GitHub
if [[ "${VERSION}" = "v1.0" ]] ; then
  REPO="common-workflow-language"
else
  REPO="cwl-$VERSION"
fi

if [ ! -d "${REPO}-${COMMIT}" ] ; then
  if [ ! -f "${COMMIT}.tar.gz" ] ; then
	  wget "https://github.com/common-workflow-language/${REPO}/archive/${COMMIT}.tar.gz"
  fi
  tar xzf "${COMMIT}.tar.gz"
fi

# Setup environment
venv cwl-conformance-venv
pip install -U setuptools wheel pip
pip install -r "${SCRIPT_DIRECTORY}/requirements.txt"
pip install -r "${SCRIPT_DIRECTORY}/test-requirements.txt"

# Set conformance test filename
if [[ "${VERSION}" = "v1.0" ]] ; then
  CONFORMANCE_TEST="${SCRIPT_DIRECTORY}/${REPO}-${COMMIT}/${VERSION}/conformance_test_v1.0.yaml"
else
  CONFORMANCE_TEST="${SCRIPT_DIRECTORY}/${REPO}-${COMMIT}/conformance_tests.yaml"
fi
mv "${CONFORMANCE_TEST}" "${CONFORMANCE_TEST%".yaml"}.cwltest.yaml"
CONFORMANCE_TEST="${CONFORMANCE_TEST%".yaml"}.cwltest.yaml"

# Build command
TEST_COMMAND="python -m pytest ${CONFORMANCE_TEST} -n auto -rs"
if [[ -n "${EXCLUDE}" ]] ; then
  TEST_COMMAND="${TEST_COMMAND} --cwl-exclude ${EXCLUDE}"
fi
TEST_COMMAND="${TEST_COMMAND} --cov --cov-report= ${PYTEST_EXTRA}"

# Cleanup coverage
rm -rf "${SCRIPT_DIRECTORY}/.coverage" "${SCRIPT_DIRECTORY}/coverage.xml"

# Run test
cp "${SCRIPT_DIRECTORY}/tests/cwl-conformance/conftest.py" "$(dirname "${CONFORMANCE_TEST}")/"
cp "${SCRIPT_DIRECTORY}/tests/cwl-conformance/streamflow-${DOCKER}.yml" "$(dirname "${CONFORMANCE_TEST}")/streamflow.yml"
bash -c "${TEST_COMMAND}"
RETURN_CODE=$?

# Coverage report
if [ "${RETURN_CODE}" -eq "0" ] ; then
  coverage report
  coverage xml
fi

# Cleanup
deactivate
rm -rf "${COMMIT}.tar.gz" "${SCRIPT_DIRECTORY}/${REPO}-${COMMIT}" "${SCRIPT_DIRECTORY}/cwl-conformance-venv"

# Exit
exit ${RETURN_CODE}