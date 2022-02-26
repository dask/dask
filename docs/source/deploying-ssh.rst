SSH
===

It is easy to set up Dask on informally managed networks of machines using SSH.
This can be done manually using SSH and the
Dask :doc:`command line interface <deploying-cli>`,
or automatically using either the :class:`dask.distributed.SSHCluster` Python *cluster manager* or the
``dask-ssh`` command line tool. This document describes both of these options.

.. note::
   Before instaniating a ``SSHCluster`` it is recommended to configure keyless SSH
   for your local machine and other machines. For example, on a Mac to SSH into
   localhost (local machine) you need to ensure the Remote Login option is set in
   System Preferences -> Sharing. In addition, ``id_rsa.pub`` should be in
   ``authorized_keys`` for keyless login.

Python Interface
----------------

.. currentmodule:: dask.distributed

.. autofunction:: SSHCluster

Command Line
------------

The convenience script ``dask-ssh`` opens several SSH connections to your
target computers and initializes the network accordingly. You can
give it a list of hostnames or IP addresses::

   $ dask-ssh 192.168.0.1 192.168.0.2 192.168.0.3 192.168.0.4

Or you can use normal UNIX grouping::

   $ dask-ssh 192.168.0.{1,2,3,4}

Or you can specify a hostfile that includes a list of hosts::

   $ cat hostfile.txt
   192.168.0.1
   192.168.0.2
   192.168.0.3
   192.168.0.4

   $ dask-ssh --hostfile hostfile.txt

.. note::

   The command line documentation here may differ depending on your installed
   version. We recommend referring to the output of ``dask-ssh --help``.

.. click:: distributed.cli.dask_ssh:main
   :prog: dask-ssh
   :show-nested:
