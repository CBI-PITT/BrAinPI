# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 16:45:04 2022

@author: awatson
"""

import caterva as cat
import numpy as np
from skimage import io

shape = (8_000, 8_000)
chunks = (4_000, 100)
blocks = (500, 25)
dtype = np.dtype("f8")
itemsize = dtype.itemsize

c_data = cat.empty(shape, itemsize, chunks=chunks, blocks=blocks)




import caterva as cat
shape = (8_000, 8_000)
chunks = (4_000, 100)
blocks = (500, 25)
dtype = np.dtype("f8")
itemsize = dtype.itemsize

data = np.arange(np.prod(shape), dtype=dtype).reshape(shape)
c_data = cat.asarray(data, chunks=chunks, blocks=blocks)


import caterva as cat
import numpy as np

shape = (1_000, 1_000)
chunks = (500, 21)
blocks = (200, 11)
dtype = np.dtype("uint16")
itemsize = dtype.itemsize

a = cat.empty(shape, itemsize, chunks=chunks, blocks=blocks)

for i in range(shape[0]):
    a[i] = np.random.randint(0,65535,shape[1])
    # a[i] = np.linspace(0, 1, shape[1], dtype=dtype)



import caterva as cat
import numpy as np

shape = (1_000, 1_000)
chunks = (500, 20)
blocks = (200, 10)
dtype = np.dtype("f8")
itemsize = dtype.itemsize

a = cat.empty(shape, itemsize, chunks=chunks, blocks=blocks)

for i in range(shape[0]):
    a[i] = np.linspace(0, 1, shape[1], dtype=dtype)


b = a[5:7, 5:10]

c = np.asarray(b).view(dtype)

d = a[5:7, 5:10].view(dtype)



import caterva as cat
import numpy as np


shape = (1_000, 1_000)
chunks = (500, 20)
blocks = (200, 10)
dtype = np.dtype("f8")
itemsize = dtype.itemsize

a = cat.empty(shape, itemsize, chunks=chunks, blocks=blocks)

for i in range(shape[0]):
    a[i] = np.linspace(0, 1, shape[1], dtype=dtype)

###  Write Caterva
with open(r'z:\test.cat','wb') as f:
    f.write(a.to_buffer())

###  Read WHOLE Caterva
with open(r'z:\test.cat','rb') as f:
    b = f.read()
c = cat.from_buffer(b, shape, itemsize,chunks=chunks, blocks=blocks)





z = cat.open(r'z:\test.cat')






import caterva as cat
import numpy as np
import os
import shutil


shape = (128, 128)
chunks = (32, 32)
blocks = (8, 8)

urlpath = r'z:\test.cat'

# if os.path.exists(urlpath):
#     cat.remove(urlpath)

dtype = np.dtype(np.complex128)
itemsize = dtype.itemsize

# Create a numpy array
nparray = np.arange(int(np.prod(shape)), dtype=dtype).reshape(shape)

# Create a caterva array from a numpy array (on disk)
a = cat.from_buffer(bytes(nparray), nparray.shape, itemsize, chunks=chunks, blocks=blocks,
                    urlpath=urlpath, contiguous=False)

# Read a caterva array from disk
b = cat.open(urlpath)

# Convert a caterva array to a numpy array
nparray2 = cat.from_buffer(b.to_buffer(), b.shape, b.itemsize,chunks=b.chunks,blocks=b.blocks)
nparray2 = nparray2[:].view(dtype)

np.testing.assert_almost_equal(nparray, nparray2)

# Remove file on disk
if os.path.exists(urlpath):
    cat.remove(urlpath)





















