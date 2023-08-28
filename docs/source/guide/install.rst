=======
Install
=======

You can install StreamFlow as a Python package with ``pip``, run it in a `Docker <https://www.docker.com/>`_ container or deploy it on `Kubernetes <https://kubernetes.io/>`_ with `Helm <https://helm.sh/>`_.

Pip
===

The StreamFlow module is available on `PyPI <https://pypi.org/project/streamflow/>`_, so you can install it using the following command::

    pip install streamflow

Please note that StreamFlow requires ``python >= 3.8`` to be installed on the system. Then you can execute your workflows through the StreamFlow CLI::

    streamflow /path/to/streamflow.yml

Docker
======

StreamFlow Docker images are available on `Docker Hub <https://hub.docker.com/r/alphaunito/streamflow>`_. To download the latest StreamFlow image, you can use the following command::

    docker pull alphaunito/streamflow:latest
