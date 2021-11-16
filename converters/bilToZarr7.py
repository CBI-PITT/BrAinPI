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
from skimage import io, img_as_uint, img_as_float32
from skimage.filters import gaussian


location = r"/CBI_Hive/globus/pitt/bil"
zarrStack = os.path.join(location,'c01_0.zarr')

store = zarr.NestedDirectoryStore(zarrStack)

imageStack = da.from_zarr(store)


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


pyramidMap = imagePyramidNum(imageStack, imageStack.chunksize)
print(pyramidMap)

def smooth(image, sigma=(2*2)/6):
    # print('Running Smooth')
    out = img_as_float32(image)
    out = gaussian(out,sigma)
    return img_as_uint(out)

startAtLevel = 1
start = time.time()
zarrObjs = {}
for key in pyramidMap:
    zarrLoc = os.path.join(location,'c01_{}.zarr'.format(key))
    if key < startAtLevel:
        print('Skipping {}'.format(key))
        continue
    if key == 0:
        print('Skipping {}'.format(key))
        continue
    currentShape = imageStack[::2,::2,::2].shape
    
    
    print('Initializing Zarr Store: {}'.format(zarrLoc))
    store = zarr.NestedDirectoryStore(zarrLoc)
    zarrObjs[key] = zarr.zeros(currentShape, chunks=pyramidMap[key][1], store=store, dtype=imageStack.dtype, overwrite=True)
    
    downscale = 2
    sigma = (2*downscale)/6
    
    print('Rechunk')
    imageStack = imageStack.rechunk((10,1000,1000))
    print('Map Overlap')
    imageStack = imageStack.map_overlap(smooth,depth=2,boundary='reflect',trim=True)
    # print('Convert 16bit')
    # imageStack = imageStack.map_blocks(img_as_uint, dtype=np.uint16)
    print('Subsample')
    imageStack = imageStack[::2,::2,::2]
    print('Rechunk')
    imageStack = imageStack.rechunk(pyramidMap[key][1])
    print('Building Pyramid Level {}'.format(2**key))
    
    byLayers = 60 if imageStack.shape[0] >=60 else imageStack.shape[0]
    totalLayers = imageStack.shape[0]
    for ii in list(range(totalLayers))[::byLayers]:
        start = ii
        stop = ii+byLayers if ii+byLayers < totalLayers else totalLayers-1
        
        print('Making pyramid level {} set {} to {} of {}'.format(key,start,stop,totalLayers))
        zarrObjs[key][start:stop] = imageStack[start:stop].compute()
        
    print('Level {} took {} minutes'.format(key,(time.time()-start)/60))
    
    
    store = zarr.NestedDirectoryStore(zarrLoc)
    imageStack = da.from_zarr(store)
    
print('Total took {} minutes'.format((time.time()-start)/60))
    
    
    



# # every = 16
# # for start in range(imageStack.shape[0])[::every]:
# #     toDo = imageStack[start:start+every+2]
# #     toDo = toDo.rechunk((toDo.shape[0],512,512))
# #     toDo = toDo.map_overlap(gaussian,depth = 2, boundary='reflect',trim=True)
# #     break


# # def saveALayer(file, zarrObjs, key, idx):
    
# #     print('Reading {}'.format(file))
# #     workingImage = io.imread(file)
# #     for key in zarrObjs:
# #         if idx%2**key == 0:
# #             print('Writing pyramid subsample {}'.format(2**key))
# #             zarrObjs[key][idx//(2**key),...] = workingImage[::2**key,::2**key]
            
# def saveALayerFullRes(file, zarrObjs, idx):
    
#     print('Reading {}'.format(file))
#     workingImage = io.imread(file)
#     print('Writing pyramid subsample {}'.format(2**0))
#     zarrObjs[0][idx//(2**0),...] = workingImage[::2**0,::2**0]
#     # for key in zarrObjs:
#     #     if idx%2**key == 0:
#     #         print('Writing pyramid subsample {}'.format(2**key))
#     #         zarrObjs[key][idx//(2**key),...] = workingImage[::2**key,::2**key]


# toSave = []
# for idx,file in enumerate(filesList):
#     toSave.append(delayed(saveALayerFullRes)(file, zarrObjs, idx))

# start = time.time()
# dask.compute(toSave)
# print('{} minutes to complete'.format((time.time()-start)/60))


















    
    
    