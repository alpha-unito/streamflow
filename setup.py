import os.path
import pathlib
from os import path

from setuptools import setup

from streamflow.version import VERSION

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="streamflow",
    version=VERSION,
    packages=[
        "streamflow",
        "streamflow.config",
        "streamflow.core",
        "streamflow.cwl",
        "streamflow.cwl.antlr",
        "streamflow.data",
        "streamflow.deployment",
        "streamflow.deployment.connector",
        "streamflow.persistence",
        "streamflow.recovery",
        "streamflow.scheduling",
        "streamflow.scheduling.policy",
        "streamflow.workflow"
    ],
    package_data={
        "streamflow.config": ["schemas/v1.0/*.json"],
        "streamflow.data": ["schemas/*.json"],
        "streamflow.deployment": ["schemas/*.json"],
        "streamflow.deployment.connector": ["schemas/*.json"],
        "streamflow.persistence": ["schemas/*.sql", "schemas/*.json"],
        "streamflow.recovery": ["schemas/*.json"],
        "streamflow.scheduling.policy": ["schemas/*.json"]
    },
    include_package_data=True,
    url="https://github.com/alpha-unito/streamflow",
    download_url="".join(["https://github.com/alpha-unito/streamflow/releases"]),
    author="Iacopo Colonnelli",
    author_email="iacopo.colonnelli@unito.it",
    description="StreamFlow framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=open(os.path.join(pathlib.Path(__file__).parent, 'requirements.txt')).read().splitlines(),
    extras_require={
        "report": [
            "pandas",
            "plotly",
            "kaleido"
        ],
        "docs": [
            "sphinx",
            "sphinx-jsonschema",
            "sphinx-rtd-theme"
        ]
    },
    tests_require=[
        'pytest',
        'cwltest'
    ],
    python_requires=">=3.8, <4",
    entry_points={"console_scripts": [
        "streamflow=streamflow.main:run",
        "cwl-runner=streamflow.cwl.runner:run"
    ]},
    zip_safe=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: POSIX",
        "Operating System :: MacOS",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
    ],
)
