Install Dask
============

Dask is available to Anaconda users via either the conda or pip commands, both of which are included in Anaconda. If you do not already have Anaconda, `download Anaconda now <https://www.continuum.io/downloads>`_.

Dask can then be installed using ``conda``::

    conda install dask

Or dask can be installed with ``pip``::

    pip install dask


Test dask install
-----------------

Because dask is a Python library, rather than a graphical program or command line program, to test that dask is working you can run a small Python program. 

In this sample test, you will create a conda environment and name it dasktest. Activate the environment, then at the command line type `python` to start an interactive Python shell, run a brief set of commands to display an array of random numbers, then quit and deactivate the dasktest environment:

.. code::

   conda create -n dasktest dask
source activate dasktest
python

import dask.array
import numpy
a=dask.array.random.normal(6,.1,size=(4,4),chunks=(2,2))
numpy.array(a)

quit()
source deactivate

If dask is installed correctly, this will print out a 4x4 array of random numbers close to 6, similar to:

.. code::

   >>>array([[ 6.11636086,  6.02888376,  6.02715362,  6.09789198],
       [ 6.17750019,  6.10241887,  6.0984278 ,  5.80852248],
       [ 6.00780242,  6.03833159,  6.0115587 ,  6.06790745],
       [ 5.9882688 ,  5.91056546,  5.9793473 ,  5.93219086]])


Install parts of dask
---------------------

Different components of dask have different dependencies that are only relevant for that component.

* ``dask.array``: numpy
* ``dask.bag``: dill
* ``dask.dataframe``: pandas, bcolz (in development)

The base ``pip`` install of dask is fairly minimal.  This is to protect
lightweight ``dask.bag`` users from having to install heavyweight dependencies
like ``bcolz`` or ``pandas``.  You can either install these dependencies
separately or, when installing with ``pip``  you can specify which set of
dependencies you would like as a parameter::

   pip install dask[array]
   pip install dask[bag]
   pip install dask[dataframe]
   pip install dask[complete]
