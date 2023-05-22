#!/bin/bash

ANTLR4_VERSION="$(pip show antlr4-python3-runtime | grep Version | awk '{print $2}')"
SCRIPT_DIRECTORY="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
WORKDIR=$(mktemp -d)

cd "${WORKDIR}" || exit
curl -fsSLO "https://www.antlr.org/download/antlr-${ANTLR4_VERSION}-complete.jar"
curl -fsSLO "https://raw.githubusercontent.com/antlr/grammars-v4/master/javascript/ecmascript/Python/ECMAScript.g4"
java -jar "antlr-${ANTLR4_VERSION}-complete.jar" -Dlanguage=Python3 ECMAScript.g4
mv ./*.py "${SCRIPT_DIRECTORY}/streamflow/cwl/antlr/"
rm -rf "${WORKDIR}"
