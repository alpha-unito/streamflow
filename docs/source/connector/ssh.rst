=============
SSHConnector
=============

The `Secure SHell <https://en.wikipedia.org/wiki/Secure_Shell>`_ (SSH) connector relies on SSH technology to connect with farms of independent, potentially heterogeneous remote nodes. A single SSH deployment can contain multiple nodes identified by their hostnames. Each hostname is supposed to point to a single node, and distinct hostnames must point to different nodes.

A single deployment can contain multiple nodes, which represent the deployment unit. Note that SSH nodes are already active, reducing the "deployment" phase to opening an :ref:`SSHConnection <SSHConnection>`. Nodes in the same deployment are not supposed to be directly connected. Consequently, data transfers always involve the StreamFlow management node, adopting a two-step copy strategy. The binding unit and the scheduling unit coincide with the single SSH host.

.. jsonschema:: ../../../streamflow/deployment/connector/schemas/ssh.json
    :lift_description: true
    :lift_definitions: true
    :auto_reference: true
    :auto_target: true
