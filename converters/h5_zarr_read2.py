# -*- coding: utf-8 -*-
"""
Created on Wed Jul 27 15:17:28 2022

@author: awatson
"""

'''
Attempt to downsample by 2x 
'''

import numpy as np
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

from H5_zarr_store4 import H5Store
# from Z:\cbiPythonTools\bil_api\converters\H5_zarr_store3 import H5Store

if os.name == 'nt':
    full_res_zarr = r'z:/testData/h5_zarr_test3/scale_0'
else:
    full_res_zarr = r'/CBI_FastStore/testData/h5_zarr_test3/scale_0'

if os.name == 'nt':
    down_2 = r'z:/testData/h5_zarr_test3/scale_1'
else:
    down_2 = r'/CBI_FastStore/testData/h5_zarr_test3/scale_1'
    
def half(img):
    return rescale(img,0.5)

def blur(img):
    if img.dtype == float:
        img = img/65535
    a = img_as_float32(img)
    a = gaussian(a,0.5)
    
    # location = block_info[0]['array-location']
    # print(location)
    # print(block_info)
    return img_as_uint(a)

def save(block, zarr_array, block_info=None):
    if block_info is not None:
        location = block_info[0]['array-location']
        location = [slice(*x) for x in location]
        # print('Saving : {}'.format(location))
        zarr_array[tuple(location)] = block
    return np.zeros(block.shape,dtype=bool)
    
#     {0: {'shape': (1000,),
#      'num-chunks': (10,),
#      'chunk-location': (4,),
#      'array-location': [(400, 500)]},
#  None: {'shape': (1000,),
#         'num-chunks': (10,),
#         'chunk-location': (4,),
#         'array-location': [(400, 500)],
#         'chunk-shape': (100,),
#         'dtype': dtype('float64')}}

store = H5Store(full_res_zarr, verbose=True)

z = zarr.open(store)

origional_shape = z.shape
origional_chunks = z.chunks

# for t in range(origional_shape[0]):
#     for c in range(origional_shape[1]):
        
        
#         working = da.from_zarr(store)
#         working = working[t,c]
#         working = working.map_blocks(blur)
#         working = working[::2,::2,::2]
        
#         break

print('Opening Zar')
working = da.from_zarr(store)
print('gauss')
working = working.map_blocks(blur)
# working = working.map_overlap(blur,depth=(0,0,2,2,2))
print('subsample')
working = working[:,:,::2,::2,::2]


from numcodecs import Blosc
print('Storing')
sim_jobs = 8
compression_level = 8
compressor=Blosc(cname='zstd', clevel=compression_level, shuffle=Blosc.BITSHUFFLE)
store1 = H5Store(down_2, verbose=True)
array1 = zarr.zeros(working.shape, chunks=(1,1,16,256,256), store=store1, overwrite=True, compressor=compressor,dtype=working.dtype)
# print('Rechunking')
# working = working.rechunk(array1.chunks)
print('saving')
working = working.map_blocks(save,array1)


def run():
    with dask.config.set({'temporary_directory': '/CBI_FastStore/tmp_dask'}):
        
        num_submit = 2
        to_compute = []
        with Client(n_workers=sim_jobs,threads_per_worker=os.cpu_count()//sim_jobs) as client:
            print('Computing')
            for y,x in product(range(0,working.shape[-2],array1.chunks[-2]),range(0,working.shape[-1],array1.chunks[-1])):
                
                ystop = y+array1.chunks[-2] if y+array1.chunks[-2] < array1.shape[-2] else None
                xstop = x+array1.chunks[-1] if x+array1.chunks[-1] < array1.shape[-1] else None
                print('Computing {},{}'.format((y,ystop),(x,xstop)))
                tmp = working[:,:,:,
                        y:ystop,
                        x:xstop
                        ]
                to_compute.append(tmp)
                del tmp
                
            priority = 0
            computing = []
            while True:
                while len(computing) < num_submit and len(to_compute) > 0:
                    print('Submitting {}'.format(to_compute[0]))
                    comp = client.compute(to_compute[0],priority=priority)
                    priority=-1
                    del to_compute[0]
                    computing.append(comp)
                    del comp
                
                computing = [x for x in computing if x.status != 'finished']
                
                if len(to_compute) == 0:
                    break
                break
            while len(computing) > 0:
                computing = [x for x in computing if x.status != 'finished']


if __name__ == '__main__':
    run()
                
            
                
                    
            


# with dask.config.set({'temporary_directory': '/CBI_FastStore/tmp_dask'}):
    
#     with Client(n_workers=sim_jobs,threads_per_worker=os.cpu_count()//sim_jobs) as client:
#         print('Computing')
#         for y,x in product(range(working.shape[-2]),range(working.shape[-2])):
#             print('Computing {},{}'.format(y,x))
#             working[:,:,:,y,x].compute()
#         # da.store(working,array1,lock=False)

# print('Computing')

# # working.compute()
# for y,x in product(range(0,working.shape[-2],array1.chunks[-2]),range(0,working.shape[-1],array1.chunks[-1])):
    
#     ystop = y+array1.chunks[-2] if y+array1.chunks[-2] < array1.shape[-2] else None
#     xstop = x+array1.chunks[-1] if x+array1.chunks[-1] < array1.shape[-1] else None
#     print('Computing {},{}'.format((y,ystop),(x,xstop)))
#     working[:,:,:,
#             y:ystop,
#             x:xstop
#             ].compute()














