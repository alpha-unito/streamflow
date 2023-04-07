=============
FluxConnector
=============

The `Flux Framework <https://flux-framework.org/>`_ connector allows running jobs on a cluster with Flux Framework in a High Performance Computing Context. Although Flux can work in a local testing container or a cloud environment and has a Python SDK, to match the design here, we follow suit and use a :ref:`SSHConnection <SSHConnection>` pointing to a login node.

For a quick demo or tutorial, see our `example workflow <https://github.com/alpha-unito/streamflow/tree/master/examples/flux>`_.

.. jsonschema:: ../../../streamflow/config/schemas/v1.0/queue_manager.json
    :lift_description: true
