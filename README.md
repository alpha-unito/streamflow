# StreamFlow

The StreamFlow framework is a container-native *Workflow Management System (WMS)* written in Python 3.
It has been designed around two main principles:
* Allow the execution of tasks in **multi-container environments**, in order to support concurrent execution
of multiple communicating tasks in a multi-agent ecosystem.
* Relax the requirement of a single shared data space, in order to allow for **hybrid workflow** executions on top of
multi-cloud or hybrid cloud/HPC infrastructures.

## Use StreamFlow

The StreamFlow module is available on [PyPI](https://pypi.org/project/streamflow/), so you can install it using pip.

```bash
pip install streamflow
```

Please note that StreamFlow requires `python >= 3.7`. Then you can execute it directly from the CLI

```bash
streamflow /path/to/streamflow.yml
```

## Contribute to StreamFlow

StreamFlow uses [pipenv](https://pipenv.kennethreitz.org/en/latest/) to guarantee deterministic builds.
Therefore, the recommended way to manage dependencies is by means of the `pipenv` command.

As a first step, get StreamFlow from [GitHub](https://github.com/alpha-unito/streamflow) 
```bash
git clone git@github.com:alpha-unito/streamflow.git
```

Then you can install all the requred packages using the `pipenv` command
```bash
pip install --user pipenv
cd streamflow
pipenv install
```

Finally, you can run StreamFlow in the generated virtual environment
```bash
pipenv run python -m streamflow
```

## StreamFlow Team

Iacopo Colonnelli <iacopo.colonnelli@unito.it> (creator and maintainer)  
Barbara Cantalupo <barbara.cantalupo@unito.it> (maintainer)  
Marco Aldinucci <aldinuc@di.unito.it> (maintainer)  