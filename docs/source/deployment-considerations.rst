Deployment Considerations
=========================

Thanks to the efforts of the open-source community, there are tools to deploy Dask :ref:`pretty much anywhere <deployment-options>`—if you can get computers to talk to each other, you can probably turn them into a Dask cluster.

**However, getting Dask running is often not the last step, but the first step.** This document attempts to cover some of the things *outside of Dask* you may have to think about when managing a Dask deployment.

These challenges are especially relevant when managing Dask for a team or organization, or transitioning into Dask for production (automated) use, but many will also come up for individual Dask users on distributed systems.


Consistent software environments
--------------------------------
For Dask to function properly, the same set of Python packages, at the same versions, need to be installed on the scheduler and workers as on the client. One of the most common stumbling points in deploying Dask on multiple machines is keeping what's installed on the cluster up to date with what's on the client, especially if they run on different systems (a laptop and a cloud provider, for example). For possible approaches, see :doc:`software-environments`.

Some ways of maintaining consistent environments may also require extra infrastructure. For example, using Docker images is common in cloud deployments, but then you need somewhere to store the images, such as `DockerHub <https://hub.docker.com/>`_, `AWS ECR <https://aws.amazon.com/ecr/>`_, `GCP Container Registry <https://cloud.google.com/container-registry>`_, etc. As use matures, you'll also want to version-control your Dockerfile and automatically build and publish new images when it changes, using a CI/CD system like `GitHub Actions <https://github.com/marketplace/actions/build-and-push-docker-images>`_, `Google Cloud Build <https://cloud.google.com/build/docs/build-push-docker-image>`_, or others.

Software environments can be particularly challenging when managing Dask for multiple users. Sometimes, using the same locked-down set of packages is sufficient for a team, but often individuals will need different packages, or different versions of the same package (including Dask itself!), to be productive. In these cases, you might end up giving end-users access to the build system (requiring them to build and maintain Docker images themselves, for example), or if that isn't allowable in your organization, creating custom infrastructure, or resorting to workarounds like the :class:`~distributed.diagnostics.plugin.PipInstall` or :class:`~distributed.diagnostics.plugin.UploadDirectory` plugins.

Additional challenges can include getting local packages or scripts onto the cluster (and ensuring they're up to date), as well as packages installed from private Git or PyPI repos.

Environment management options without additional infrastructure:

* :class:`~distributed.diagnostics.plugin.PipInstall` plugin
* :class:`~distributed.diagnostics.plugin.UploadDirectory` plugin
* Coiled's `package sync <https://docs.coiled.io/user_guide/software/sync.html?utm_source=dask-docs&utm_medium=deployment-considerations>`_ automatically replicates a local environment onto a cluster, including local packages and Git dependencies.


Logging
-------
When clusters break, logs may be your only tool to debug them. At a minimum, you should have a way to retain logs from all the machines in the cluster. You may also want to ingest these logs into a dedicated logging system, so you can search the logs, or view logs from multiple systems interleaved by time. (If deploying on a cloud provider, this might already be set up for you with the provider's logging system, though be aware of potential charges for log storage.)

When managing dask for a team, you'll also have to figure out how individual (and potentially less-technical) users can access logs and metrics for their own clusters. In a large organization, this may even include preventing users from accessing logs from other users' clusters.


Getting credentials on the cluster
----------------------------------
Your laptop may have access to connect to an S3 bucket or a database, but depending on how your cluster is deployed, the workers may not. This can lead to errors where code that works locally fails with authentication errors when run on the cluster. The :doc:`how-to/connect-to-remote-data` page has more discussion of this.

Especially when used by teams, mature Dask deployments will want to avoid users passing their own credentials to the cluster directly in code, using strategies such as generating temporary tokens for users, authenticating workers under service accounts, etc.


Controlling access
------------------
Most non-commercial deployment libraries rely on the user launching the cluster to have access to the underlying system the cluster will run on (such as a cloud provider, an HPC cluster, Kubernetes, etc.). When enabling a team to use Dask, this may not be the case: you want to let users launch clusters on demand, which might create cloud VMs or Kubernetes pods, without actually giving them permission to create cloud VMs or Kubernetes pods directly. `Dask-gateway <https://gateway.dask.org/>`_ is a common way to do this, but that does require additional administration.

Most Dask deployment options set reasonable defaults for access (i.e. not making your cluster accessible to anyone on the Internet), but you still should make sure that your clusters (or your users' clusters) aren't accessible to unauthorized users.

Additionally, if you're connecting to a cluster over the Internet, you should ensure that that connection is encrypted, since sensitive information, such as credentials or proprietary data, may flow over it. You might do this by port-forwarding your connection over SSH, using :doc:`TLS <tls>`, or using `Dask-gateway <https://gateway.dask.org/>`_ or a commercial offering that manages this automatically.


Controlling cost
----------------
It's easy to forget to shut down a cluster and run up an expensive bill over the weekend. Some deployment libraries also may not always be able to fully clean up a cluster—for example, `dask-cloudprovider <https://cloudprovider.dask.org/>`_ won't clean up cloud resources if the client Python process (or machine!) shuts down abruptly.

Therefore, when launching clusters automatically in production, or enabling many team members to launch them, you should be confident that all resources will be cleaned up, or shut down if they exceed a cost threshold.

When managing Dask for a team, you may also want a way to limit how much individual users can spend, to prevent accidental overruns.


Monitoring cost
---------------
It's good to be able to answer questions such as:

- How much are we spending on Dask?
- What are we spending it on? (machines, machines that should have been turned off, network egress that shouldn't have happened, etc.)
- Who/what is responsible?

Most deployment tools don't build in this sort of monitoring. Organizations that need it either end up building their own tools, or turning to commercial deployment offerings.


Managing networking
-------------------
The Dask client needs to be able to talk to the scheduler, which is potentially on a different system. Users like to be able to access the :doc:`dashboard <dashboard>` from a Web browser. The machines in the cluster need to be able to talk to each other. Typically, whatever :ref:`deployment system <deployment-options>` you use will handle this for you. Sometimes, though, there can be additional considerations around what type of networking to use for best performance. Networking also can have costs associated—cloud providers may charge fixed or usage-based rates for certain types of networking configurations, for example.

You may also have other systems on restricted networks that workers need to access to read and write data, or call APIs. Connecting to those networks could add additional complexity.

Some organizations may have additional network security policies, such as requiring all traffic to be encrypted. Dask supports this with :doc:`TLS <tls>`. Some deployment systems enable this automatically using self-signed certificates; others may require additional configuration, especially if using certificates from your organization.


Observability
-------------
The :doc:`dashboard <dashboard>` is a powerful tool for monitoring live clusters. But once the cluster stops (or breaks), the dashboard is gone, so it's invaluable for debugging to record information that lasts longer than the cluster. This is especially important when running automated jobs.

Dask provides :doc:`Prometheus metrics <prometheus>`, which offer close to dashboard-level detail, but can persist long after the cluster has shut down, making them especially valuable for monitoring and debugging production workarounds. They also can be aggregated, which is helpful when running many clusters at once, or even used to trigger automated alerts. Using these metrics requires deploying and managing `Prometheus <https://prometheus.io>`_ (or a Prometheus-compatible service), configuring networking so it can access the machines in the cluster, and typically also deploying `Grafana <https://grafana.com/>`_ to visualize metrics and create dashboards.


Storing local data off the local machine
----------------------------------------
If you're deploying Dask on a cluster, most data is probably already stored remotely, since a major reason for deploying Dask instead of :ref:`running locally <deployment-single-machine>` is to run workers closer to the data. However, it can be common to also have some smaller, auxiliary data files locally.

In that case, you may need somewhere to store those auxiliary files remotely, where workers can access them. Depending on your deployment system, there are many options, from network file systems to cloud object stores like S3. Regardless, this can be another piece of infrastructure to manage and secure.


Note on managed Dask offerings
------------------------------
As shown, setting up and managing a mature Dask deployment, especially for team or production use, can involve a fair amount of complexity outside of Dask itself. Addressing these challenges is generally out of scope for the open-source Dask deployment tools, but there are other projects as well as commercial Dask deployment services that handle many of these considerations. In alphabetical order:

- `Coiled <https://coiled.io?utm_source=dask-docs&utm_medium=deployment-considerations>`_ handles the creation and management of Dask clusters on cloud computing environments (AWS, Azure, and GCP).
- `Saturn Cloud <https://saturncloud.io/>`_ lets users create Dask clusters in a hosted platform or within their own AWS accounts.
