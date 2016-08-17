Frequently Asked Questions
==========================

Too many open file descriptors?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Your operating system imposes a limit to how many open files or open network
connections any user can have at once.  Depending on the scale of your
cluster the ``dask-scheduler`` may run into this limit.

By default most Linux distributions set this limit at 1024 open
files/connections and OS-X at 128 or 256.  Each worker adds a few open
connections to a running scheduler (somewhere between one and ten, depending on
how contentious things get.)

If you are on a managed cluster you can usually ask whoever manages your
cluster to increase this limit.  If you have root access and know what you are
doing you can change the limits on Linux by editing
``/etc/security/limits.conf``.  Instructions are here under the heading "User
Level FD Limits":
http://www.cyberciti.biz/faq/linux-increase-the-maximum-number-of-open-files/
