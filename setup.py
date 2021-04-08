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
        "streamflow.data",
        "streamflow.deployment",
        "streamflow.deployment.connector",
        "streamflow.recovery",
        "streamflow.scheduling",
        "streamflow.workflow"
    ],
    package_data={"streamflow.config": ["schemas/v1.0/*.json"]},
    include_package_data=True,
    url="https://github.com/alpha-unito/streamflow",
    download_url="".join(["https://github.com/alpha-unito/streamflow/releases"]),
    author="Iacopo Colonnelli",
    author_email="iacopo.colonnelli@unito.it",
    description="StreamFlow framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        "aiohttp",
        "asyncssh",
        "cachetools",
        "cwltool",
        "Jinja2",
        "jsonref",
        "jsonschema",
        "kubernetes_asyncio",
        "uvloop"
    ],
    python_requires=">=3.8, <4",
    entry_points={"console_scripts": ["streamflow=streamflow.main:run"]},
    zip_safe=True,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: System :: Distributed Computing",
    ],
)
