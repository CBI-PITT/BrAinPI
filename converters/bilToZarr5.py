# -*- coding: utf-8 -*-
"""
Created on Fri Oct 29 09:46:38 2021

@author: alpha
"""

import os, glob, zarr, time
import numpy as np
import dask
from dask.delayed import delayed
import dask.array as da
from skimage import io
from skimage.filters import gaussian


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




# every = 16
# for start in range(imageStack.shape[0])[::every]:
#     toDo = imageStack[start:start+every+2]
#     toDo = toDo.rechunk((toDo.shape[0],512,512))
#     toDo = toDo.map_overlap(gaussian,depth = 2, boundary='reflect',trim=True)
#     break


# def saveALayer(file, zarrObjs, key, idx):
    
#     print('Reading {}'.format(file))
#     workingImage = io.imread(file)
#     for key in zarrObjs:
#         if idx%2**key == 0:
#             print('Writing pyramid subsample {}'.format(2**key))
#             zarrObjs[key][idx//(2**key),...] = workingImage[::2**key,::2**key]
            
def saveALayerFullRes(file, zarrObjs, idx):
    
    print('Reading {}'.format(file))
    workingImage = io.imread(file)
    print('Writing pyramid subsample {}'.format(2**0))
    zarrObjs[0][idx//(2**0),...] = workingImage[::2**0,::2**0]
    # for key in zarrObjs:
    #     if idx%2**key == 0:
    #         print('Writing pyramid subsample {}'.format(2**key))
    #         zarrObjs[key][idx//(2**key),...] = workingImage[::2**key,::2**key]


toSave = []
for idx,file in enumerate(filesList):
    toSave.append(delayed(saveALayerFullRes)(file, zarrObjs, idx))

start = time.time()
dask.compute(toSave)
print('{} minutes to complete'.format((time.time()-start)/60))


















    
    
    