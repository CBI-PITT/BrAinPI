# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 20:49:45 2022

@author: awatson
"""

import numpy as np
from spatial_image import to_spatial_image
from multiscale_spatial_image import to_multiscale
import zarr

import os
import sys
import glob
import time
from itertools import product
from natsort import natsorted
from io import BytesIO
from skimage import io, img_as_float32, img_as_ubyte, img_as_uint
from skimage.transform import rescale
from skimage.filters import gaussian
import dask.array as da
from dask.delayed import delayed
import zarr
from distributed import Client
import dask
import tifffile

'''
Works to downsample block by block stripped accross z
'''

if os.name == 'nt':
    path = r'Z:\cbiPythonTools\bil_api\converters\H5_zarr_store3'
else:
    path = r'/CBI_FastStore/cbiPythonTools/bil_api/converters/H5_zarr_store3'
    
if path not in sys.path:
    sys.path.append(path)

from H5_zarr_store6 import H5Store
# from Z:\cbiPythonTools\bil_api\converters\H5_zarr_store3 import H5Store

# if os.name == 'nt':
#     full_res_zarr = r'z:/testData/h5_zarr_test3/scale_0'
# else:
#     full_res_zarr = r'/CBI_FastStore/testData/h5_zarr_test3/scale_0'

# if os.name == 'nt':
#     down_2 = r'z:/testData/h5_zarr_test3/scale_1'
# else:
#     down_2 = r'/CBI_FastStore/testData/h5_zarr_test3/scale_1'

if os.name == 'nt':
    out = r'z:/testData/h5_zarr_test4'
else:
    out = r'/CBI_FastStore/testData/h5_zarr_test4'
    

# store = H5Store(full_res_zarr, verbose=True)

# array = da.from_zarr(store)

## Testing
# array = np.random.randint(0, 65534, size=(20,1280,1280), dtype=np.uint16)
array = np.random.randint(0, 65534, size=(1,2,128,1280,1280), dtype=np.uint16)

image = to_spatial_image(array,dims=('t', 'c', 'z', 'y', 'x'))
# image = to_spatial_image(array,dims=('z','y', 'x'))
print(image)

multiscale = to_multiscale(image, [2,4,8])
# multiscale = to_multiscale(image, [{'z':2,'y':2,'x':2},{'z':4,'y':4,'x':4},{'z':8,'y':8,'x':8}])
# multiscale = to_multiscale(image)
print(multiscale)

out_store = H5Store(out, verbose=True)
# out_store = zarr.storage.DirectoryStore(out, dimension_separator='/')
multiscale.to_zarr(out_store)





