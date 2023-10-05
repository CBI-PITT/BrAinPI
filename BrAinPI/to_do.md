# Priority:

-Coordination endpoint handling for all link creation methods
-Regularize html link handling
-TIFF endpoint to request arbitrary tiff files at any resolution from any dataset
-Endpoint to return precise sub-resolution volumes of whole datasets (ie 10um isotropic volume for atlas registration)



# Would be nice:

-Open Imaris files using zarr store to take advantage of chunk caching.



# Wish list: #

-Customize chunks received for ome.zarr
-Customize compression received for ome.zarr
-Support for H5_big_data_viewer files
-Enable on the fly transformation to CCF
-Large 2D image viewer to support multiscale OME-TIFF delivery with open sea dragon  (http://openseadragon.github.io/)



# Issues:

-Forced 8bit data delivery sometimes results in errors and black tiles returned