============
SSH Connector
============

.. meta::
   :keywords: StreamFlow, ssh, remote, deployment
   :description: SSH connector reference for StreamFlow

Overview
========

The SSH connector executes workflow tasks on remote hosts via SSH, providing simple remote execution without batch scheduling systems.

Quick Reference
===============

============  ====================================
Type          ``ssh``
Category      HPC
Scalability   Single or multiple remote hosts
Best For      Remote execution, simple clusters
============  ====================================

Examples
========

With SSH Key Authentication
----------------------------

.. code-block:: yaml

   deployments:
     ssh-key:
       type: ssh
       config:
         hostname: hpc.example.com
         username: researcher
         sshKey: ~/.ssh/id_rsa
         sshKeyPassphrase: my-passphrase  # Optional

With Password Authentication
-----------------------------

.. code-block:: yaml

   deployments:
     ssh-password:
       type: ssh
       config:
         hostname: 192.168.1.100
         username: user
         password: secret  # Not recommended for production

Multiple Hosts
--------------

.. code-block:: yaml

   deployments:
     ssh-cluster:
       type: ssh
       config:
         nodes:
           - hostname: node1.example.com
             username: user
             sshKey: ~/.ssh/id_rsa
           - hostname: node2.example.com
             username: user
             sshKey: ~/.ssh/id_rsa
           - hostname: node3.example.com
             username: user
             sshKey: ~/.ssh/id_rsa

With Custom Port and Timeout
-----------------------------

.. code-block:: yaml

   deployments:
     ssh-custom:
       type: ssh
       config:
         hostname: bastion.example.com
         username: admin
         sshKey: ~/.ssh/id_ed25519
         port: 2222
         connectionTimeout: 30
         maxConnections: 10

Prerequisites
=============

* SSH access to remote host(s)
* SSH key authentication configured (recommended)
* Appropriate file permissions on SSH keys (``chmod 600``)
* Remote host in SSH known_hosts or host key verification disabled

Security Considerations
=======================

* **Use SSH keys** instead of passwords for authentication
* **Protect private keys** with passphrases
* **Limit maxConnections** to avoid overwhelming remote systems
* **Use jump hosts/bastions** for accessing secured networks

Connection Pooling
==================

The ``maxConnections`` option controls concurrent SSH connections:

* **Low values (1-5):** Conservative, safe for small systems
* **Medium values (10-20):** Balanced for typical workloads
* **High values (50+):** For large-scale parallel execution

Platform Support
================

**Linux:** Full support  
**macOS:** Full support  
**Windows:** Not supported

Configuration
=============

.. jsonschema:: https://streamflow.di.unito.it/schemas/deployment/connector/ssh.json
    :lift_description: true

Related Documentation
=====================

**User Guide:**
   - :doc:`/user-guide/configuring-deployments` - Deployment configuration guide
   - :doc:`/user-guide/troubleshooting` - SSH connection troubleshooting
   - :doc:`/user-guide/advanced-patterns/stacked-locations` - Container stacking with SSH

**Connectors:**
   - :doc:`index` - All HPC connectors
   - :doc:`slurm` - For batch-scheduled execution
   - :doc:`pbs` - For PBS-managed systems
   - :doc:`/reference/connectors/singularity` - Container integration

**External Resources:**
   - `OpenSSH Documentation <https://www.openssh.com/manual.html>`_
