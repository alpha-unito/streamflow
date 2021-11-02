import logging
import os.path
import shutil
import subprocess
import tempfile
import zipfile
from typing import Optional, MutableSequence
from urllib.request import urlretrieve

import psutil

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
    subprocess.run(cmd, cwd=spec, stderr=subprocess.STDOUT)


def test_cwl10():
    with tempfile.TemporaryDirectory() as outdir:
        commit = '1c1f122f780075d910fdfdea7e15e46eef3c078d'
        url = 'common-workflow-language/archive/{}.zip'.format(commit)
        spec = _download_spec(url, outdir, 'common-workflow-language-{}'.format(commit))
        _run_conformance(
            spec=spec,
            testfile=os.path.join(spec, 'v1.0', 'conformance_test_v1.0.yaml'),
            skipped_tests=[
                '173'   # docker_entrypoint
            ])


def test_cwl11():
    with tempfile.TemporaryDirectory() as outdir:
        commit = '6397014050177074c9ccd0d771577f7fa9f728a3'
        url = 'cwl-v1.1/archive/{}.zip'.format(commit)
        spec = _download_spec(url, outdir, 'cwl-v1.1-{}'.format(commit))
        _run_conformance(
            spec=spec,
            testfile=os.path.join(spec, 'conformance_tests.yaml'),
            skipped_tests=[
                '174',  # docker_entrypoint
                '199',  # stdin_shorcut
                '235'   # inplace_update_on_file_content
            ])


def test_cwl12():
    with tempfile.TemporaryDirectory() as outdir:
        commit = 'beb4cb65e672e0652c19da070e19688c412b0551'
        url = 'cwl-v1.2/archive/{}.zip'.format(commit)
        spec = _download_spec(url, outdir, 'cwl-v1.2-{}'.format(commit))
        _run_conformance(
            spec=spec,
            testfile=os.path.join(spec, 'conformance_tests.yaml'),
            skipped_tests=[
                '175',  # docker_entrypoint
                '236'   # inplace_update_on_file_content
            ])
