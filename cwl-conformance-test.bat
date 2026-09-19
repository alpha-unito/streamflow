echo off

rem Version of the standard to test against
rem Current options: v1.0, v1.1, v1.2, and v1.3
if "%VERSION%"=="" set "VERSION=v1.2"

rem Which commit of the standard's repo to use
rem Defaults to the last commit of the main branch
if "%COMMIT%"=="" set "COMMIT=main"

rem Comma-separated list of test names that should be excluded from execution
rem Defaults to "docker_entrypoint, inplace_update_on_file_content"
if "%EXCLUDE%"=="" set "EXCLUDE=docker_entrypoint,modify_file_content"

rem Name of the CWLDockerTranslator plugin to use for test execution
rem This parameter allows to test automatic CWL requirements translators
if "%DOCKER%"=="" set "DOCKER=docker"

rem Additional arguments for the pytest command
rem Defaults to none
rem set "PYTEST_EXTRA="

rem The directory where this script resides
set "SCRIPT_DIRECTORY=%~dp0"
set "SCRIPT_DIRECTORY=%SCRIPT_DIRECTORY:~0,-1%"

rem Download archive from GitHub
if "%VERSION%"=="v1.0" (
    set "REPO=common-workflow-language"
) else (
    set "REPO=cwl-%VERSION%"
)

if not exist "%REPO%-%COMMIT%" (
    if not exist "%COMMIT%.tar.gz" (
        echo Downloading %REPO% @ %COMMIT%...
        pwsh -Command "Invoke-WebRequest -Uri https://github.com/common-workflow-language/%REPO%/archive/%COMMIT%.tar.gz -OutFile %COMMIT%.tar.gz"
    )
    tar -xzf "%COMMIT%.tar.gz"
)

rem Setup environment
call :venv cwl-conformance-venv
python -m pip install -U setuptools wheel pip
python -m pip install -r "%SCRIPT_DIRECTORY%\requirements.txt"
python -m pip install -r "%SCRIPT_DIRECTORY%\test-requirements.txt"
if "%VERSION%"=="v1.3" (
    python -m pip uninstall -y cwl-utils
    python -m pip install git+https://github.com/common-workflow-language/cwl-utils.git@refs/pull/370/head
)

rem Set conformance test filename
if "%VERSION%"=="v1.0" (
    set "CONFORMANCE_TEST=%SCRIPT_DIRECTORY%\%REPO%-%COMMIT%\%VERSION%\conformance_test_v1.0.yaml"
) else (
    set "CONFORMANCE_TEST=%SCRIPT_DIRECTORY%\%REPO%-%COMMIT%\conformance_tests.yaml"
)
move "%CONFORMANCE_TEST%" "%CONFORMANCE_TEST:.yaml=.cwltest.yaml%"
set "CONFORMANCE_TEST=%CONFORMANCE_TEST:.yaml=.cwltest.yaml%"

rem Build command
set "TEST_COMMAND=python -m pytest "%CONFORMANCE_TEST%" -n auto -rs"
if not "%EXCLUDE%"=="" (
    set "TEST_COMMAND=%TEST_COMMAND% --cwl-exclude %EXCLUDE%"
)
set "TEST_COMMAND=%TEST_COMMAND% --cov --junitxml=junit.xml -o junit_family=legacy --cov-report= %PYTEST_EXTRA%"

rem Cleanup coverage
if exist "%SCRIPT_DIRECTORY%\.coverage" del "%SCRIPT_DIRECTORY%\.coverage"
if exist "%SCRIPT_DIRECTORY%\coverage.xml" del "%SCRIPT_DIRECTORY%\coverage.xml"
if exist "%SCRIPT_DIRECTORY%\junit.xml" del "%SCRIPT_DIRECTORY%\junit.xml"

rem Run test
copy "%SCRIPT_DIRECTORY%\tests\cwl-conformance\conftest.py" "%~dpn1"
copy "%SCRIPT_DIRECTORY%\tests\cwl-conformance\streamflow-%DOCKER%.yml" "%~dpn1\streamflow.yml"
cmd /c "%TEST_COMMAND%"
set "RETURN_CODE=%ERRORLEVEL%"

rem Coverage report
if "%RETURN_CODE%"=="0" (
    coverage report
    coverage xml
)

rem Cleanup
rd /s /q "%SCRIPT_DIRECTORY%\%REPO%-%COMMIT%"
rd /s /q "%SCRIPT_DIRECTORY%\cwl-conformance-venv"
del "%COMMIT%.tar.gz" 2>nul

rem Exit
exit /b %RETURN_CODE%
goto :eof

:venv
if not exist "%~1" (
    where virtualenv >nul 2>&1 && (
        virtualenv -p python "%~1"
    ) || (
        python -m venv "%~1"
    )
)
call "%~1\Scripts\activate.bat"
goto :eof