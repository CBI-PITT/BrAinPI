# -*- coding: utf-8 -*-
"""
Created on Mon May 16 22:48:44 2022

@author: awatson
"""

import zarr
import numpy as np
from numcodecs import Blosc, blosc
import struct
import math
from itertools import product


'''
Below is a recipe for creating a zarr array and reading / decoding 
individual chunks
'''
compressor = Blosc(cname='zstd', clevel=1, shuffle=Blosc.BITSHUFFLE,blocksize=0)
# compressor = None

shape = (10,10010,10010)
dtype = np.uint16
chunks=(1,1000,1000)

z1 = zarr.open('C:/code/testZarr', mode='w', shape=shape,
               chunks=chunks, dtype=np.uint16,compressor=compressor)

z1[:] = 42

file = 'c:/code/testZarr/9.10.10'

with open(file,'rb') as f:
    z = f.read()
    if compressor:
        z = compressor.decode(z)
    z = np.frombuffer(z, dtype=np.uint16)
    z = z.reshape(chunks)


'''
For an array of a given size, determine how to 'make' a virtual zarr array
'''

shape = (1,2,512,40265,30564)
chunks = (1,1,1,1000,1000)

dtype = np.uint16
compressor = Blosc(cname='zstd', clevel=1, shuffle=Blosc.BITSHUFFLE,blocksize=0)

## Build virtual zarr chunks
chunk_range = []
chunk_mod = []
for sh,ch in zip(shape,chunks):
    chunk_range.append(math.ceil(sh/ch))
    chunk_mod.append(sh%ch)

chunk_template = '{}.{}.{}.{}.{}'
chunks_list = []
for t,c,z,y,x in product(
        range(chunk_range[0]),
        range(chunk_range[1]),
        range(chunk_range[2]),
        range(chunk_range[3]),
        range(chunk_range[4])
        ):
    tmp = chunk_template.format(t,c,z,y,x)
    print(tmp)
    chunks_list.append(tmp)
    
