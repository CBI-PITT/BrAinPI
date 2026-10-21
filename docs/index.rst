BrAinPI documentation
=====================

BrAinPI is a Flask service for browsing multiscale microscopy datasets and
presenting them through Neuroglancer, OpenSeadragon, and virtual OME-Zarr v2
endpoints. The repository `README
<https://github.com/CBI-PITT/BrAinPI/blob/main/README.md>`_ contains installation
and startup guidance. Detailed operational guides are available in the repository:

* `Deployment and troubleshooting
  <https://github.com/CBI-PITT/BrAinPI/blob/main/docs/deployment.md>`_
* `Storage management
  <https://github.com/CBI-PITT/BrAinPI/blob/main/docs/storage.md>`_
* `Viewer and API reference
  <https://github.com/CBI-PITT/BrAinPI/blob/main/docs/viewer-reference.md>`_

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
