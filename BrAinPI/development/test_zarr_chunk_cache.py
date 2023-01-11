# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 15:07:26 2022

@author: awatson
"""

from diskcache import Cache
import zarr

directory = r'c:/code/cache'
cache=Cache(directory,size_limit=int(4e12))

from zarr.storage import LRUStoreCache

store_loc = r'Z:\testData\test_zarr10.omezarr\scale0'

store = H5_Shard_Store(store_loc)
cachestore = disk_cache_store(store,diskcache_object=cache,uuid='test')
# lru_head_store = LRUStoreCache_HeadSpace(cachestore, head_space_GB=450)
# zarray = zarr.open(lru_head_store)

lru_store = LRUStoreCache(cachestore,max_size=30*1024**3)
zarray = zarr.open(lru_store)





from collections import OrderedDict
import numpy as np

a = OrderedDict()
for ii in range(5000):
    
    a[ii] = np.ones((1024,1024,10))

for ii in range(5000):
    a.popitem(last=False)




a = {}
for ii in range(5000):
    
    a[ii] = np.ones((1024,1024,1))

for ii in tuple(a.keys()):
    del a[ii]
    
    




from stack_to_multiscale_ngff.h5_shard_store import H5_Shard_Store
from BrAinPI.zarr_chunk_cache import disk_cache_store
from BrAinPI.zarr_chunk_cache import LRUStoreCache_HeadSpace

from diskcache import Cache
import zarr
from itertools import product
import math

directory = '/CBI_FastStore/tmpCache/testCache'
cache=Cache(directory,size_limit=int(4e12))

# from zarr.storage import LRUStoreCache

store_loc = r'/CBI_FastStore/testData/test_zarr10.omezarr/scale0'

store = H5_Shard_Store(store_loc)
cachestore = disk_cache_store(store,diskcache_object=cache,uuid='test')
# zarray = zarr.open(cachestore)

lru_head_store = LRUStoreCache_HeadSpace(cachestore, head_space_GB=400)
zarray = zarr.open(lru_head_store)

chunks = zarray.chunks
for t,c,z,y,x in product(
        range(math.ceil(zarray.shape[0]//chunks[0])),
        range(math.ceil(zarray.shape[1]//chunks[1])),
        range(math.ceil(zarray.shape[2]//chunks[2])),
        range(math.ceil(zarray.shape[3]//chunks[3])),
        range(math.ceil(zarray.shape[4]//chunks[4]))
        ):
    
    tt = t*chunks[0]
    cc = c*chunks[1]
    zz = z*chunks[2]
    yy = y*chunks[3]
    xx = x*chunks[4]
    
    x = zarray[
        tt:tt+chunks[0] if tt+chunks[0] < zarray.shape[0] else zarray.shape[0],
        cc:cc+chunks[1] if cc+chunks[1] < zarray.shape[1] else zarray.shape[1],
        zz:zz+chunks[2] if zz+chunks[2] < zarray.shape[2] else zarray.shape[2],
        yy:yy+chunks[3] if yy+chunks[3] < zarray.shape[3] else zarray.shape[3],
        xx:xx+chunks[4] if xx+chunks[4] < zarray.shape[4] else zarray.shape[4],
        ]
    
# zarray[0,0]

# lru_store = LRUStoreCache(cachestore,max_size=30*1024**3)
# zarray = zarr.open(lru_store)
