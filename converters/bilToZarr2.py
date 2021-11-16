# -*- coding: utf-8 -*-
"""
Created on Fri Oct 29 09:46:38 2021

@author: alpha
"""

import os, glob, zarr
import numpy as np
from dask.delayed import delayed
import dask.array as da
from skimage import io


location = r"/CBI_Hive/globus/pitt/bil/CH1"
imageGeometry = (1.0,0.35,0.35)
origionalChunkSize = (1,1000,1000)

filesList = sorted(glob.glob(os.path.join(location,'*.tif')))

print('Finding Image Files')
testImage = io.imread(filesList[0])

imageStack = [delayed(io.imread)(x) for x in filesList]
imageStack = [da.from_delayed(x, shape=testImage.shape, dtype=testImage.dtype) for x in imageStack]
imageStack = da.stack(imageStack)


def imagePyramidNum(imageStack, origionalChunkSize):
    
    pyramidMap = {0:[imageStack.shape,origionalChunkSize]}
    out = imageStack.shape
    minimumChunkSize = origionalChunkSize
    topPyramidLevel = 0
    print(out)
    
    
    while True:
        if any([x<=y for x,y in zip(out,minimumChunkSize)]) == False:
            out = tuple([x//2 for x in out])
            topPyramidLevel += 1
            pyramidMap[topPyramidLevel] = [out,minimumChunkSize]
            
        else:
            minimumChunkSize = (minimumChunkSize[0]*4,minimumChunkSize[1]//2,minimumChunkSize[2]//2)
            break
        print(out)
    
    while True:
        if any([x<=y for x,y in zip(out,minimumChunkSize)]) == False:
            out = tuple([x//2 for x in out])
            topPyramidLevel += 1
            pyramidMap[topPyramidLevel] = [out,minimumChunkSize]
            # out = tuple([x//2 for x in out])
        else:
            minimumChunkSize = (minimumChunkSize[0]*4,minimumChunkSize[1]//2,minimumChunkSize[2]//2)
            break
        print(out)
    
    while True:
        if any([x<=y for x,y in zip(out,minimumChunkSize)]) == False:
            out = tuple([x//2 for x in out])
            topPyramidLevel += 1
            pyramidMap[topPyramidLevel] = [out,minimumChunkSize]
            # out = tuple([x//2 for x in out])
        else:
            minimumChunkSize = (minimumChunkSize[0]*4,minimumChunkSize[1]//2,minimumChunkSize[2]//2)
            break
        print(out)
        
    return pyramidMap


pyramidMap = imagePyramidNum(imageStack, origionalChunkSize)
print(pyramidMap)

zarrObjs = {}
toSave = []
subset = 1
for key in pyramidMap:
    print('Creating subset {}'.format(subset))
    workingStack = imageStack[::subset,::subset,::subset]
    store = zarr.NestedDirectoryStore(os.path.join(os.path.split(location)[0],'c01_{}.zarr'.format(key)))
    z = zarr.zeros(workingStack.shape, chunks=pyramidMap[key][1], store=store, dtype=workingStack.dtype, overwrite=True)
    toStore = da.store(workingStack,z,compute=False)
    toSave.append(toStore)
    subset *= 2


# for 
    