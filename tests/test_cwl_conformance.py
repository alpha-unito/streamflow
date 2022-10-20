import logging
import os.path
import shutil
import subprocess
import tempfile
import zipfile
from typing import Optional, MutableSequence
from urllib.request import urlretrieve

import psutil
import pytest

log = logging.getLogger(__name__)


def _download_spec(url: str, outdir: str, basename: str) -> str:
    zipname = os.path.join(outdir, 'cwl.zip')
    cwlname = os.path.join(outdir, 'cwl')
    urlretrieve('https://www.github.com/common-workflow-language/' + url, os.path.join(outdir, zipname))
    with zipfile.ZipFile(zipname, 'r') as z:
        z.extractall(outdir)
    os.remove(zipname)
    shutil.move(os.path.join(outdir, basename), cwlname)
    return cwlname


def _log_stdout(pipe):
    for line in iter(pipe.readline, b''):
        line = line.decode('utf-8').strip('\r\n')
        if line:
            log.info(line)


def _run_conformance(spec: str,
                     testfile: str,
                     skipped_tests: Optional[MutableSequence[str]]):
    cmd = ['cwltest',
           '--test={}'.format(testfile),
           '--timeout=3600',
           '-j={}'.format(psutil.cpu_count()),
           '--basedir={}'.format(spec)]
    if skipped_tests:
        cmd.append('-N{}'.format(','.join(skipped_tests)))
    log.info('Running {}'.format(' '.join(cmd)))
    process = subprocess.Popen(cmd, cwd=spec, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    with process.stdout:
        _log_stdout(process.stdout)
    return process.wait()


@pytest.mark.conformance
@pytest.mark.cwl10
def test_cwl10():
    with tempfile.TemporaryDirectory() as outdir:
        commit = '1c1f122f780075d910fdfdea7e15e46eef3c078d'
        url = 'common-workflow-language/archive/{}.zip'.format(commit)
        spec = _download_spec(url, outdir, 'common-workflow-language-{}'.format(commit))
        returncode = _run_conformance(
            spec=spec,
            testfile=os.path.join(spec, 'v1.0', 'conformance_test_v1.0.yaml'),
            skipped_tests=[
                '173'   # docker_entrypoint
            ])
        assert returncode == 0


@pytest.mark.conformance
@pytest.mark.cwl11
def test_cwl11():
    with tempfile.TemporaryDirectory() as outdir:
        commit = '6397014050177074c9ccd0d771577f7fa9f728a3'
        url = 'cwl-v1.1/archive/{}.zip'.format(commit)
        spec = _download_spec(url, outdir, 'cwl-v1.1-{}'.format(commit))
        returncode = _run_conformance(
            spec=spec,
            testfile=os.path.join(spec, 'conformance_tests.yaml'),
            skipped_tests=[
                '174',  # docker_entrypoint
                '199',  # stdin_shorcut
                '235'   # inplace_update_on_file_content
            ])
        assert returncode == 0


@pytest.mark.conformance
@pytest.mark.cwl12
def test_cwl12():
    with tempfile.TemporaryDirectory() as outdir:
        commit = '9b091ed7e0bef98b3312e9478c52b89ba25792de'
        url = 'cwl-v1.2/archive/{}.zip'.format(commit)
        spec = _download_spec(url, outdir, 'cwl-v1.2-{}'.format(commit))
        returncode = _run_conformance(
            spec=spec,
            testfile=os.path.join(spec, 'conformance_tests.yaml'),
            skipped_tests=[
                '177',  # docker_entrypoint
                '237'   # inplace_update_on_file_content
            ])
        assert returncode == 0
