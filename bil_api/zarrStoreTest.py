# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 19:31:50 2021

@author: alpha
"""

import zarr
import numpy as np

## LMDB not an option for big data
## In windows the full store is used even when empty
## No support for compression
## in Linux store is this provisioned 
## resulting in a massive file that must be moved
# LMDB_location = r'/CBI_FastStore/zarr/LMDB.zarr'
# store = zarr.LMDBStore(LMDB_location)
# z = zarr.zeros((5,10000, 10000), chunks=(1,512,512), dtype=np.uint16, store=store, overwrite=True)
# z[...] = np.random.randint(0, high=65534, size=z.shape, dtype=int)
# store.close()

np.random.seed(42)
LMDB_location = r'c:\code\DMB.zarr'
LMDB_location = r'/CBI_FastStore/zarr/DMB.zarr'
store = zarr.storage.DBMStore(LMDB_location)
z = zarr.zeros((500,30000, 30000), chunks=(1,1024,1024), dtype=np.uint16,store=store, overwrite=True)
for ii in range(z.shape[0]):
    print('Write Layer {}'.format(ii))
    z[ii] = np.random.randint(0, high=65534, size=z.shape[1::], dtype=int)
store.close()


np.random.seed(42)
SQL = r'c:\code\SQL1.zarr'
SQL = r'/CBI_FastStore/zarr/SQL.zarr'
store = zarr.storage.SQLiteStore(SQL)
z = zarr.zeros((500,30000, 30000), chunks=(1,1024,1024), dtype=np.uint16, store=store, overwrite=True)
for ii in range(z.shape[0]):
    print('Write Layer {}'.format(ii))
    z[ii] = np.random.randint(0, high=65534, size=z.shape[1::], dtype=int)
store.close()