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

file = 'c:/code/test.zip'

shape = (1_000, 1_000)
chunks = (2, 2)
blocks = (1, 1)
dtype = np.dtype("f8")
itemsize = dtype.itemsize

a = cat.empty(shape, itemsize, chunks=chunks, blocks=blocks)

for i in range(shape[0]):
    a[i] = np.linspace(0, 1, shape[1], dtype=dtype)

###  Write Caterva
with open(file,'wb') as f:
    f.write(a.to_buffer())

###  Read WHOLE Caterva
with open(file,'rb') as f:
    b = f.read()
c = cat.from_buffer(b, shape, itemsize,chunks=chunks, blocks=blocks)





z = cat.open(file)






import caterva as cat
import numpy as np
import os
import shutil


shape = (1024, 1024, 1024)
chunks = (256, 256, 256)
blocks = (64,64,64)

#block32 = 1.92GB
#block64 = 2.00GB
#block128 = 2.00GB

urlpath = r'c:\code\test.cat'

if os.path.exists(urlpath):
    cat.remove(urlpath)

dtype = np.dtype(np.dtype('uint16'))
itemsize = dtype.itemsize

# Create a numpy array
np.random.seed(42)
nparray = np.random.randint(0,65534,shape)
# nparray = np.ones(shape)

# Create a caterva array from a numpy array (on disk)
a = cat.from_buffer(bytes(nparray), nparray.shape, itemsize, chunks=chunks, blocks=blocks,
                    urlpath=urlpath, contiguous=True, clevel=9, nthreads=4)

b = cat.empty(nparray.shape, itemsize, chunks=chunks, blocks=blocks,
                    urlpath=urlpath, contiguous=False, clevel=9, nthreads=4)
b[:] = nparray
# Read a caterva array from disk
b = cat.open(urlpath)

# Convert a caterva array to a numpy array
nparray2 = cat.from_buffer(b.to_buffer(), b.shape, b.itemsize,chunks=b.chunks,blocks=b.blocks)
nparray2 = nparray2[:].view(dtype)

np.testing.assert_almost_equal(nparray, nparray2)

# Remove file on disk
if os.path.exists(urlpath):
    cat.remove(urlpath)









import numpy as np
import caterva as cat
from time import time
import os

urlpath_sparse = "c:\code\ex_formats_sparse.caterva"
# urlpath_sparse = None
urlpath_contiguous = "c:\code\ex_formats_contiguous.caterva"
# urlpath_contiguous = None

if urlpath_sparse and os.path.exists(urlpath_sparse):
    cat.remove(urlpath_sparse)

if urlpath_contiguous and os.path.exists(urlpath_contiguous):
    cat.remove(urlpath_contiguous)

shape = (1000 * 1000,)
chunks = (100,)
blocks = (100,)
dtype = np.dtype(np.float64)
itemsize = dtype.itemsize

t0 = time()
a = cat.empty(shape, 8, chunks=chunks, blocks=blocks, urlpath=urlpath_sparse,
             contiguous=False)
for nchunk in range(a.nchunks):
    a[nchunk * chunks[0]: (nchunk + 1) * chunks[0]] = np.arange(chunks[0], dtype=dtype)
t1 = time()

print(f"Time: {(t1 - t0):.4f} s")
print(a.nchunks)
an = np.array(a[:]).view(dtype)


t0 = time()
b = cat.empty(shape, itemsize=itemsize, chunks=chunks, blocks=blocks, urlpath=urlpath_contiguous, contiguous=True)

print(b.nchunks)
for nchunk in range(shape[0] // chunks[0]):
    b[nchunk * chunks[0]: (nchunk + 1) * chunks[0]] = np.arange(chunks[0], dtype=dtype)
t1 = time()

print(f"Time: {(t1 - t0):.4f} s")
print(b.nchunks)
bn = np.array(b[:]).view(dtype)

np.testing.assert_allclose(an, bn)



from io import BytesIO
from zipfile import ZipFile
file = 'c:/code/test.zip'

z = ZipFile(file,mode='w')

q = BytesIO()
a = cat.empty(shape, 8, chunks=chunks, blocks=blocks, urlpath=q,
             contiguous=False)










