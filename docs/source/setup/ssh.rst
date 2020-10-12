SSH
===

It is easy to set up Dask on informally managed networks of machines using SSH.
This can be done manually using SSH and the
Dask :doc:`command line interface <cli>`,
or automatically using either the ``SSHCluster`` Python command or the
``dask-ssh`` command line tool. This document describes both of these options.

Python Interface
----------------

.. currentmodule:: distributed.deploy.ssh   #doctest: +SKIP

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

The ``dask-ssh`` utility depends on the ``paramiko``::

    python -m pip install paramiko

.. note::

   The command line documentation here may differ depending on your installed
   version. We recommend referring to the output of ``dask-ssh --help``.

.. click:: distributed.cli.dask_ssh:main
   :prog: dask-ssh
   :show-nested:
