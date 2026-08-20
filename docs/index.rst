BrAinPI documentation
=====================

BrAinPI is a Flask service for browsing multiscale microscopy datasets and
presenting them through Neuroglancer, OpenSeadragon, and virtual OME-Zarr v2
endpoints. The repository `README
<https://github.com/CBI-PITT/BrAinPI/blob/main/README.md>`_ contains installation,
configuration, deployment, endpoint, cache, and validation guidance.

Core modules
------------

.. automodule:: config_tools
   :members:

.. automodule:: neuroGlancer
   :members:

.. automodule:: openSeadragon
   :members:

.. automodule:: ome_zarr_ep
   :members:

Loaders
-------

.. automodule:: loader_indexing
   :members:

.. automodule:: loader_axes
   :members:

.. automodule:: ome_zarr_loader
   :members:

.. automodule:: tiff_loader
   :members:

.. automodule:: jp2_loader
   :members:

.. automodule:: nd2_loader
   :members:

.. automodule:: nifti_loader
   :members:

.. automodule:: terafly_loader
   :members:

.. automodule:: ng_precomputed_loader
   :members:

Utilities
---------

.. automodule:: utils
   :members:

.. automodule:: s3_utils
   :members:

.. automodule:: cache_tools
   :members:

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
