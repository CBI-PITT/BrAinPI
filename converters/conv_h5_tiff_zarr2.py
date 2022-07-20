# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 20:38:10 2022

@author: awatson
"""

import numpy as np
import os
import sys
import glob
from natsort import natsorted
from skimage import io
import dask.array as da
from dask.delayed import delayed
import zarr
from distributed import Client

if os.name == 'nt':
    path = r'Z:\cbiPythonTools\bil_api\converters\H5_zarr_store3'
else:
    path = r'/CBI_FastStore/cbiPythonTools/bil_api/converters/H5_zarr_store3'
    
if path not in sys.path:
    sys.path.append(path)

from H5_zarr_store3 import H5Store
# from Z:\cbiPythonTools\bil_api\converters\H5_zarr_store3 import H5Store

if os.name == 'nt':
    jp2_location = r'H:\globus\pitt\bil\fMOST RAW'
else:
    jp2_location = r'/CBI_Hive/globus/pitt/bil/fMOST RAW'

if os.name == 'nt':
    out_location = r'Z:\testData\h5_zarr_test2'
else:
    out_location = r'/CBI_FastStore/testData/h5_zarr_test2'

sim_jobs = 32//2
storage_chunks = (1,1,2,1024,1024)
def test():
    # os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"
    colors = natsorted(glob.glob(os.path.join(jp2_location,'*')))
    files = []
    for cc in colors:
        files.append(
            natsorted(glob.glob(os.path.join(cc,'*.tif')))
            )
    
    test_image = io.imread(files[0][0])
    
    stack = []
    for color in files:
            
        s = [delayed(io.imread)(x) for x in color]
        s = [da.from_delayed(x,test_image.shape,dtype=test_image.dtype) for x in s]
        s = da.stack(s)
        stack.append(s)
    stack = da.stack(stack)
    stack = stack[None,...]
    
    from numcodecs import Blosc
    compressor=Blosc(cname='zstd', clevel=8, shuffle=Blosc.BITSHUFFLE)
    
    # store = H5Store(r'Z:\testData\test_h5_store2')
    # z = zarr.zeros((1, 2, 11500, 20000, 20000), chunks=(1,1,256,256,256), store=store, overwrite=True, compressor=compressor)
    
    store = H5Store(out_location)
    # z = zarr.zeros(stack.shape, chunks=(1,1,1,1024,1024), store=store, overwrite=True, compressor=compressor)
    # z = zarr.zeros(stack.shape, chunks=stack.chunksize, store=store, overwrite=True, compressor=compressor)
    z = zarr.zeros(stack.shape, chunks=storage_chunks, store=store, overwrite=True, compressor=compressor)
    
    # Align stack chunks with zarr chunks in z (since this is how the h5 files are stored)
    # Default values are to have y*4 and x*4 chunksize to reduce the number of chunks that dask has to manage by 16 fold (4*4)
    # This improves 1) write efficiency, 2) storage efficiency, 3) avoids any io collisions
    # stack = stack.rechunk((z.chunks[0],z.chunks[1],z.chunks[2],z.chunks[3]*10,z.chunks[4]*10))
    stack = stack.rechunk((z.chunks[0],z.chunks[1],z.chunks[2],stack.chunksize[3],stack.chunksize[4]))
    # stack = stack.rechunk((1,1,2,1024,1024))
    
    
    with Client() as client:
        # print(client.run(lambda: os.environ["HDF5_USE_FILE_LOCKING"]))
        for tt in range(stack.shape[0]):
            for cc in range(stack.shape[1]):
                for zz in range(0,stack.shape[2],storage_chunks[2]*4):
                    print(zz)
                    write = stack[tt,cc,zz:zz+storage_chunks[2]*4]
                    write = client.compute(write)
                    print(write)
                    z[tt,cc,zz:zz+storage_chunks[2]*4] = write[0]
                    del write
        
        # da.store(stack, z,lock=False)
        # da.to_zarr(stack,store)
        # return stored_array
    
    # with Client(n_workers=sim_jobs,threads_per_worker=os.cpu_count()//sim_jobs) as client:
    #     da.store(stack, z,lock=False)

if __name__ == '__main__':
    test()
