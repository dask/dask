EC2 Startup Script
==================

First, add your AWS credentials to ``~/.aws/credentials`` like this::

     [default]
     aws_access_key_id = YOUR_ACCESS_KEY
     aws_secret_access_key = YOUR_SECRET_KEY

For other ways to manage or troubleshoot credentials, see the `boto3 docs <https://boto3.readthedocs.io/en/latest/guide/quickstart.html>`_.

Now, you can quickly deploy a scheduler and workers on EC2 using the ``dec2`` quickstart application::

  pip install dec2
  dec2 up --keyname YOUR-AWS-KEY --keypair ~/.ssh/YOUR-AWS-SSH-KEY.pem

This provisions a cluster on Amazon's EC2 cloud service, installs Anaconda, and
sets up a scheduler and workers.  In then prints out instructions on how to
connect to the cluster.

Options
-------

The ``dec2`` startup script comes with the following options for creating a
cluster::

   $ dec2 up --help
   Usage: dec2 up [OPTIONS]

   Options:
     --keyname TEXT                Keyname on EC2 console  [required]
     --keypair PATH                Path to the keypair that matches the keyname [required]
     --name TEXT                   Tag name on EC2
     --region-name TEXT            AWS region  [default: us-east-1]
     --ami TEXT                    EC2 AMI  [default: ami-d05e75b8]
     --username TEXT               User to SSH to the AMI  [default: ubuntu]
     --type TEXT                   EC2 Instance Type  [default: m3.2xlarge]
     --count INTEGER               Number of nodes  [default: 4]
     --security-group TEXT         Security Group Name  [default: dec2-default]
     --volume-type TEXT            Root volume type  [default: gp2]
     --volume-size INTEGER         Root volume size (GB)  [default: 500]
     --file PATH                   File to save the metadata  [default: cluster.yaml]
     --provision / --no-provision  Provision salt on the nodes  [default: True]
     --dask / --no-dask            Install Dask.Distributed in the cluster [default: True]
     --nprocs INTEGER              Number of processes per worker  [default: 1]
     -h, --help                    Show this message and exit.

Connect
-------

Connection instructions follow successful completion of the ``dec2 up``
command.  The involve the following::

    dec2 ssh            # SSH into head node
    ipython             # Start IPython console on head node

.. code-block:: python

   >>> from distributed import Executor, s3, progress
   >>> e = Executor('127.0.0.1:8786')

This executor now has access to all the cores of your cluster.


Destroy
-------

You can destroy your cluster from your local machine with the destroy command::

   dec2 destroy
