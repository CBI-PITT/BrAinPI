# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 20:38:10 2022

@author: awatson
"""

import numpy as np
import os
import sys
import glob
import time
import math
from natsort import natsorted
from io import BytesIO
from skimage import io
import dask.array as da
from dask.delayed import delayed
import zarr
from distributed import Client
import dask
import tifffile

'''
45.49245145075851 hours for 2 color fmost
compression_level = 8
storage_chunks = (1,1,8,512,512)
'''

if os.name == 'nt':
    path = r'Z:\cbiPythonTools\bil_api\converters\H5_zarr_store3'
else:
    path = r'/CBI_FastStore/cbiPythonTools/bil_api/converters/H5_zarr_store3'
    
if path not in sys.path:
    sys.path.append(path)

from H5_zarr_store6 import H5Store
from tiff_manager import tiff_manager_3d
# from Z:\cbiPythonTools\bil_api\converters\H5_zarr_store3 import H5Store

if os.name == 'nt':
    jp2_location = r'H:\globus\pitt\bil\fMOST RAW'
else:
    jp2_location = r'/CBI_Hive/globus/pitt/bil/fMOST RAW'

if os.name == 'nt':
    out_location = r'Z:\testData\h5_zarr_test4/scale0'
    # out_location = r'Z:\testData\h5_zarr_test4/scale0'
else:
    out_location = r'/CBI_FastStore/testData/h5_zarr_test4/scale0'
    # out_location = r'/CBI_Hive/globus/pitt/bil/h5_zarr_test4/scale0'

jp2=False
if jp2==True:
    if os.name== 'nt':
        jp2_location = r'H:\globus\pitt\bil\jp2\download.brainimagelibrary.org\8a\d7\8ad742d9c0b886fd\Calb1_GFP_F_F5_200420\level1'
        out_location = r'H:\globus\pitt\bil\jp2\zarr_h5_test'
    else:
        jp2_location = r'/CBI_Hive/globus/pitt/bil/jp2/download.brainimagelibrary.org/8a/d7/8ad742d9c0b886fd/Calb1_GFP_F_F5_200420/level1'
        out_location = r'/CBI_Hive/globus/pitt/bil/jp2/zarr_h5_test'

# if os.name == 'nt':
#     out_location = r'H:\testData\h5_zarr_test3'
# else:
#     out_location = r'/CBI_Hive/testData/h5_zarr_test3'

def organize_by_groups(a_list,group_len):

    new = []
    working = []
    idx = 0
    for aa in a_list:
        working.append(aa)
        idx += 1
        
        if idx == group_len:
            new.append(working)
            idx = 0
            working = []
    
    if working != []:
        new.append(working)
    return new

# chunk_limit_MB = 1024
# cpu_number = 32
# storage_chunks = (1,1,4,512,512)
# chunk_depth = (test_image.shape[1]//4) - (test_image.shape[1]//4)%storage_chunks[3]
# z_plane_shape = (30967,20654)

def determine_read_depth(storage_chunks,num_workers,z_plane_shape,chunk_limit_MB=1024,cpu_number=os.cpu_count()):
    chunk_depth = storage_chunks[3]
    current_chunks = (storage_chunks[0],storage_chunks[1],storage_chunks[2],chunk_depth,z_plane_shape[1])
    current_size = math.prod(current_chunks)*2/1024/1024
    
    if current_size >= chunk_limit_MB:
        return chunk_depth
    
    while current_size <= chunk_limit_MB:
        chunk_depth += storage_chunks[3]
        current_chunks = (storage_chunks[0],storage_chunks[1],storage_chunks[2],chunk_depth,z_plane_shape[1])
        current_size = math.prod(current_chunks)*2/1024/1024
        
        if chunk_depth >= z_plane_shape[0]:
            chunk_depth = z_plane_shape[0]
            break
    return chunk_depth
        

sim_jobs = 8
compression_level = 8
storage_chunks = (1,1,4,1024,1024)
chunk_limit_MB = 2048
# read_depth = 512

def test():
    # os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"
    colors = natsorted(glob.glob(os.path.join(jp2_location,'*')))
    files = []
    for cc in colors:
        if jp2:
            files.append(
                natsorted(glob.glob(os.path.join(cc,'*.jp2')))
                )
        else:
            files.append(
                natsorted(glob.glob(os.path.join(cc,'*.tif')))
                )
    
    # print('Reading Test Image')
    # test_image = tiff_manager(files[0][0],desired_chunk_depth=4096)
    # return test_image
    
    print('Building Virtual Stack')
    stack = []
    for color in files:
        
        s = organize_by_groups(color,storage_chunks[2])
        test_image = tiff_manager_3d(s[0],desired_chunk_depth_y=storage_chunks[2])
        # chunk_depth = (test_image.shape[1]//4) - (test_image.shape[1]//4)%storage_chunks[3]
        chunk_depth = determine_read_depth(storage_chunks,num_workers=sim_jobs,z_plane_shape=test_image.shape[1:],chunk_limit_MB=chunk_limit_MB)
        test_image = tiff_manager_3d(s[0],desired_chunk_depth_y=chunk_depth)
        print(test_image.shape)
        print(test_image.chunks)
        
        s = [test_image.clone_manager_new_file_list(x) for x in s]
        print(len(s))
        for ii in s:
            print(ii.chunks)
            print(len(ii.fileList))
        # print(s[-3].chunks)
        print('From_array')
        print(s[0].dtype)
        s = [da.from_array(x,chunks=x.chunks,name=False,asarray=False) for x in s]
        # print(s)
        print(len(s))
        s = da.concatenate(s)
        # s = da.stack(s)
        print(s)
        stack.append(s)
    stack = da.stack(stack)
    stack = stack[None,...]
    
    # sc =  stack.chunksize
    # stack = stack.rechunk((sc[0],sc[1],storage_chunks[2],sc[3],sc[4]))
    
    print(stack)
    # return stack
    
    # time.sleep(10)
    # return stack
    from numcodecs import Blosc
    compressor=Blosc(cname='zstd', clevel=compression_level, shuffle=Blosc.BITSHUFFLE)
    
    # store = H5Store(r'Z:\testData\test_h5_store2')
    # z = zarr.zeros((1, 2, 11500, 20000, 20000), chunks=(1,1,256,256,256), store=store, overwrite=True, compressor=compressor)
    
    store = H5Store(out_location,verbose=2)
    # z = zarr.zeros(stack.shape, chunks=(1,1,1,1024,1024), store=store, overwrite=True, compressor=compressor)
    # z = zarr.zeros(stack.shape, chunks=stack.chunksize, store=store, overwrite=True, compressor=compressor)
    
    z = zarr.zeros(stack.shape, chunks=storage_chunks, store=store, overwrite=True, compressor=compressor,dtype=stack.dtype)
    
    # Align stack chunks with zarr chunks in z (since this is how the h5 files are stored)
    # Default values are to have y*4 and x*4 chunksize to reduce the number of chunks that dask has to manage by 16 fold (4*4)
    # This improves 1) write efficiency, 2) storage efficiency, 3) avoids any io collisions
    # stack = stack.rechunk((z.chunks[0],z.chunks[1],z.chunks[2],z.chunks[3]*10,z.chunks[4]*10))
    # stack = stack.rechunk((z.chunks[0],z.chunks[1],z.chunks[2]*8,z.chunks[3]*5,z.chunks[4]*5))
    # stack = stack.rechunk((1,1,2,1024,1024))
    
    # with Client('c001.cbiserver:8786') as client:
    # return stack

    if os.name == 'nt':
        with Client(n_workers=1,threads_per_worker=1) as client:
            da.store(stack,z,lock=False)
    else:
        with dask.config.set({'temporary_directory': '/CBI_FastStore/tmp_dask'}):
            
            # with Client(n_workers=sim_jobs,threads_per_worker=os.cpu_count()//sim_jobs) as client:
            # with Client(n_workers=8,threads_per_worker=2) as client:
            with Client(n_workers=16,threads_per_worker=1) as client:
                # print(client.run(lambda: os.environ["HDF5_USE_FILE_LOCKING"]))
                da.store(stack,z,lock=False)
                # da.to_zarr(stack,store)
                # return stored_array
    
    # with Client(n_workers=sim_jobs,threads_per_worker=os.cpu_count()//sim_jobs) as client:
    #     da.store(stack, z,lock=False)

if __name__ == '__main__':
    print('Running')
    start = time.time()
    test()
    total = time.time() - start
    total_hours = total /60/60
    print('Zarr conversion took {} hours'.format(total_hours))
    
    
