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
for key in pyramidMap:
    
    currentShape = imageStack[::2**key,::2**key,::2**key].shape
    store = zarr.NestedDirectoryStore(os.path.join(os.path.split(location)[0],'c01_{}.zarr'.format(key)))
    zarrObjs[key] = zarr.zeros(currentShape, chunks=pyramidMap[key][1], store=store, dtype=imageStack.dtype, overwrite=True)
    # zarrObjs[key] = zarr.open(os.path.join(os.path.split(location)[0],'c01_{}.zarr'.format(key)), mode='w', shape=pyramidMap[key][0], chunks=pyramidMap[key][1], dtype=imageStack.dtype)


referenceDepth = list(range(imageStack.shape[0]))
maxChunkDepth = pyramidMap[len(pyramidMap)-1][1][0]

startingLocation = 0
idx=0
while startingLocation <= imageStack.shape[0]:
    currentLayers = referenceDepth[startingLocation:startingLocation+maxChunkDepth]
    print('Reading Layers: {}'.format(currentLayers))
    fullResChunk = imageStack[currentLayers]
    fullResChunk = fullResChunk.compute()
    
    for key in pyramidMap:
        print('Writing Pyramid Level {}'.format(key))
        layersToWrite = referenceDepth[startingLocation:startingLocation+maxChunkDepth][::2**key]
        toWrite = fullResChunk[::2**key,::2**key,::2**key]
        
        start = idx*toWrite.shape[0]
        stop = start+toWrite.shape[0]
        zarrObjs[key][start:stop,:,:] = toWrite
    
    startingLocation += maxChunkDepth
    idx += 1
        # forStorage = fullResChunk[currentLayers,::2**key,::2**key]
        # zarrObjs[key][key*pyramidMap[key][1][0]::] = forStorage
    # break



# for idx,file in enumerate(filesList):
#     print('Reading {}'.format(file))
#     workingImage = io.imread(file)
#     for key in zarrObjs:
#         print('Writing Zarr level {}'.format(key))
#         zarrObjs[key][idx,...] = workingImage
#         workingImage = workingImage[1::2,1::2]

# for idx,file in enumerate(filesList):
#     print('Reading {}'.format(file))
#     workingImage = io.imread(file)
#     for key in zarrObjs:
#         print('Writing Zarr level {}'.format(key))
#         zarrObjs[key][idx,...] = workingImage
#         workingImage = workingImage[1::2,1::2]
    
    
    