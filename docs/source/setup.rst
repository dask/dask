Setup Network
=============

A ``distributed`` network consists of one ``Center`` node and several
``Worker`` nodes.  One can set these up in a variety of ways

``dcluster``
------------

The convenience script ``dcluster`` opens several ssh connections to your
target computers and initializes the network accordingly.  You can give it a
list of hostnames or IP addresses::

   $ dcluster 192.168.0.1 192.168.0.2 192.168.0.3 192.168.0.4

Or you can use normal UNIX grouping::

   $ dcluster 192.168.0.{1,2,3,4}

Or you can specify a hostfile that includes a list of hosts::

   $ cat hostfile.txt
   192.168.0.1
   192.168.0.2
   192.168.0.3
   192.168.0.4

   $ dcluster --hostfile hostfile.txt


``dcenter`` and ``dworker``
---------------------------

Alternatively you can launch the ``dcenter`` and ``dworker`` processes in some
other way either by manually SSHing into remote nodes or by using a deployment
system like SunGrid Engine.

Launch ``dcenter`` on one node::

   $ dcenter
   Start center at 192.168.0.1:8787

Then launch ``dworker`` on the rest of the nodes, providing the address to the
node that hosts ``dcenter``::

   $ dworker 192.168.0.1:8787
   Start worker at:            192.168.0.2:8788
   Registered with center at:  192.168.0.1:8787

   $ dworker 192.168.0.1:8787
   Start worker at:            192.168.0.3:8788
   Registered with center at:  192.168.0.1:8787

   $ dworker 192.168.0.1:8787
   Start worker at:            192.168.0.4:8788
   Registered with center at:  192.168.0.1:8787


``Center`` and ``Worker`` objects
---------------------------------

Alternatively you can start up the ``distributed.center.Center`` and
``distributed.worker.Worker`` objects within a Python session manually.
