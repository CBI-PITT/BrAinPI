# -*- coding: utf-8 -*-
"""
Created on Sat Jul 30 10:52:40 2022

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

ext = '.tif'
if os.name == 'nt':
    path = r'Z:\cbiPythonTools\bil_api\converters\H5_zarr_store6'
else:
    path = r'/CBI_FastStore/cbiPythonTools/bil_api/converters/H5_zarr_store6'
    
if path not in sys.path:
    sys.path.append(path)

from H5_zarr_store6 import H5Store
# from Z:\cbiPythonTools\bil_api\converters\H5_zarr_store3 import H5Store

if os.name == 'nt':
    jp2_location = r'H:\globus\pitt\bil\fMOST RAW'
else:
    jp2_location = r'/CBI_Hive/globus/pitt/bil/fMOST RAW'

if os.name == 'nt':
    out_location = r'h:/globus/pitt/bil/fmost_h5_zarr_multiscal_test'
else:
    out_location = r'/CBI_Hive/globus/pitt/bil/fmost_h5_zarr_multiscal_test'

jp2=True
if jp2==True:
    ext = '.jp2'
    if os.name== 'nt':
        jp2_location = r'H:\globus\pitt\bil\jp2\download.brainimagelibrary.org\8a\d7\8ad742d9c0b886fd\Calb1_GFP_F_F5_200420\level1'
        out_location = r'H:\globus\pitt\bil\jp2\zarr_h5_test_multiscale'
    else:
        jp2_location = r'/CBI_Hive/globus/pitt/bil/jp2/download.brainimagelibrary.org/8a/d7/8ad742d9c0b886fd/Calb1_GFP_F_F5_200420/level1'
        out_location = r'/CBI_Hive/globus/pitt/bil/jp2/zarr_h5_test_multiscale'

# if os.name == 'nt':
#     out_location = r'H:\testData\h5_zarr_test3'
# else:
#     out_location = r'/CBI_Hive/testData/h5_zarr_test3'

compression_level = 8

def read(filepath,key=None):
    print('Reading {}'.format(filepath))
    with open(filepath, "rb") as fh:
        buf = BytesIO(fh.read())
    try:
        with tifffile.imread(buf,aszarr=True) as z:
            x = zarr.open(z)
            if key is None:
                return x[:]
            else:
                return x[key]
        
    except Exception:
        if key is None:
            return io.imread(buf)
        else:
            return io.imread(buf)[key]



# os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"
colors = natsorted(glob.glob(os.path.join(jp2_location,'*')))
files = []
for cc in colors:
    if jp2:
        files.append(
            natsorted(glob.glob(os.path.join(cc,'*' + ext)))
            )
    else:
        files.append(
            natsorted(glob.glob(os.path.join(cc,'*' + ext)))
            )

print('Reading Test Image')
test_image = read(files[0][0])

# files = [organize_by_groups(x,storage_chunks[2]) for x in files]

print('Building Virtual Stack')
stack = []
for color in files:
    
    s = [delayed(read)(x) for x in color]
    s = [da.from_delayed(x,shape=test_image.shape,dtype=test_image.dtype) for x in s]
    # s = da.concatenate(s)
    s = da.stack(s)
    stack.append(s)
stack = da.stack(stack)
stack = stack[None,...]
print(stack)
# time.sleep(10)
    
image = to_spatial_image(stack,dims=('t', 'c', 'z', 'y', 'x'))
# image = to_spatial_image(array,dims=('z','y', 'x'))
print(image)

print('Forming Multiscale')
multiscale = to_multiscale(image, [2,2,2,2,2])
# # multiscale = to_multiscale(image, [{'z':2,'y':2,'x':2},{'z':4,'y':4,'x':4},{'z':8,'y':8,'x':8}])
# # multiscale = to_multiscale(image)
print(multiscale)

print('Registering Compression')
from numcodecs import Blosc
compression_level = 8
compressor=Blosc(cname='zstd', clevel=compression_level, shuffle=Blosc.BITSHUFFLE)

import zarr.storage
# switch to using chosen compressor
zarr.storage.default_compressor = compressor

print('Creating H5_Store')
# out_store = H5Store(out_location, verbose=True)
out_store = zarr.storage.DirectoryStore(out_location, dimension_separator='/')
print('Writing OME-NGFF')
multiscale.to_zarr(out_store)









