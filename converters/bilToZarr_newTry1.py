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
# from skimage.filters import gaussian
from numcodecs import Blosc
import matplotlib.image as mpimg
from io import BytesIO
from distributed import Client

'''
Working to make resolution level 0 conversion of jp2 stack to
zarr-like dataset with zip store
'''

client = Client()

location = r"/CBI_Hive/globus/pitt/bil/jp2/download.brainimagelibrary.org/8a/d7/8ad742d9c0b886fd/Calb1_GFP_F_F5_200420/level1"
out_location = r"/CBI_Hive/globus/pitt/bil/jp2/zarr"
os.makedirs(out_location,exist_ok=True)
fileType = 'jp2'
imageGeometry = (1.0,0.35,0.35)
origionalChunkSize = (8,256,256)

compressor = Blosc(cname='zstd', clevel=5, shuffle=Blosc.BITSHUFFLE)

filesList = []
for ii in sorted(glob.glob(os.path.join(location,'*'))):
    filesList.append(sorted(glob.glob(os.path.join(ii,'*.{}'.format(fileType)))))

numColors = len(filesList)


# def read_buffer(fileName):
#     with open(fileName,'rb') as fh:
#         print('Reading {}'.format(fileName))
#         buf = BytesIO(fh.read())
#         print('Decoding Image {}'.format(fileName))
#         return io.imread(buf)

def read_buffer(fileName):
    with open(fileName,'rb') as fh:
        print('Reading {}'.format(fileName))
        return BytesIO(fh.read())
        # print('Decoding Image {}'.format(fileName))
        # return io.imread(buf)

def decode_image(buf):
        return io.imread(buf)


print('Finding Image Files')
testImage = read_buffer(filesList[0][0])
testImage = decode_image(testImage)



imageStack = []
for ii in filesList:
    
    stack = [delayed(read_buffer)(x) for x in ii]
    stack = [delayed(decode_image)(x) for x in stack]
    stack = [da.from_delayed(x, shape=testImage.shape, dtype=testImage.dtype) for x in stack]
    stack = da.stack(stack)
    imageStack.append(stack)

imageStack = da.stack(imageStack)

## Add time dimension as place holder (t,c,z,y,x):
imageStack = imageStack[None,:]



def imagePyramidNum(imageStack, origionalChunkSize):
    '''
    Map of pyramids accross a single 3D color
    '''
    
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


pyramidMap = imagePyramidNum(imageStack[0,0,0], origionalChunkSize) # put in last 3 dims
print(pyramidMap)


'''
Storage structure:
    resolution INT
        timepoint INT
            color INT
                z_shard_store_voxel_location INT
                    zarr_zip_store with YX Shards
'''

## Create zarr zipstore objects
# each zarr store will be 1 Z_chunk deep with YZ shards
# z_chunk stores will be named for the starting voxel location chunks of (8,256,256) (z,y,x)
# 0,8,16...

def store_location_formatter(res,t,c,z):
    return '{}/{}/{}/{}.zip'.format(res,t,c,z)

# zarrObjs = {} # Store all zarrObjects for easy write access
for t in range(imageStack.shape[0]):
    for c in range(imageStack.shape[1]):
        current_stack = imageStack[t,c]
        for key in pyramidMap:
            currentShape = current_stack[::2**key,::2**key,::2**key].shape
            
            for z_shards in range(0,currentShape[0],pyramidMap[key][1][0]):
                print(z_shards)
                
                # make location
                location = os.path.join(out_location,store_location_formatter(key,t,c,z_shards))
                os.makedirs(os.path.split(location)[0],exist_ok=True)
                
                with zarr.ZipStore(location) as store:
                    # zarrObjs[key] = zarr.zeros(currentShape, chunks=pyramidMap[key][1], store=store, dtype=imageStack.dtype, overwrite=True)
                    z = zarr.zeros((pyramidMap[key][1][0],*currentShape[1:]), chunks=pyramidMap[key][1], store=store, dtype=imageStack.dtype, compressor=compressor, overwrite=True)
                # zarrObjs[key] = zarr.open(os.path.join(os.path.split(location)[0],'c01_{}.zarr'.format(key)), mode='w', shape=pyramidMap[key][0], chunks=pyramidMap[key][1], dtype=imageStack.dtype)


def write_to_zip_store(toWrite,location=None):
    print('In write')
    if toWrite.shape==(0,0,0):
        return True
    with zarr.ZipStore(location) as store:
        print('In with')
        print(toWrite.shape)
        array = zarr.open(store)
        print('Reading {}'.format(location))
        # toWrite = toWrite.compute()
        print('Writing {}'.format(location))
        array[0:toWrite.shape[0],0:toWrite.shape[1],0:toWrite.shape[2]] = toWrite
        print('Completed {}'.format(location))
        return True

# ## Write first resolution 0 and 1 first
# # zarrObjs = {} # Store all zarrObjects for easy write access
# for t in range(imageStack.shape[0]):
#     for c in range(imageStack.shape[1]):
#         current_stack = imageStack[t,c]
#         for key in pyramidMap:
#             currentShape = current_stack[::2**key,::2**key,::2**key].shape
            
#             for z_shards in range(0,currentShape[0],pyramidMap[key][1][0]):
#                 print(z_shards)
#                 location = os.path.join(out_location,store_location_formatter(key,t,c,z_shards))
#                 with zarr.ZipStore(location) as store:
#                     array = zarr.open(store)
#                     print('Reading {}'.format(location))
#                     toWrite = current_stack[z_shards:z_shards+pyramidMap[key][1][0]].compute()
#                     print('Writing {}'.format(location))
#                     array[0:toWrite.shape[0],0:toWrite.shape[1],0:toWrite.shape[2]] = toWrite
#                     print('Completed {}'.format(location))
                        
                        
## Write first resolution 0 and 1 first
# zarrObjs = {} # Store all zarrObjects for easy write access
to_compute = []
for t in range(imageStack.shape[0]):
    for c in range(imageStack.shape[1]):
        current_stack = imageStack[t,c]
        for key in pyramidMap:
            if key > 0:
                break
            currentShape = current_stack[::2**key,::2**key,::2**key].shape
            
            for z_shards in range(0,currentShape[0],pyramidMap[key][1][0]):
                print(z_shards)
                location = os.path.join(out_location,store_location_formatter(key,t,c,z_shards))
                
                toWrite = current_stack[z_shards:z_shards+pyramidMap[key][1][0]]
                
                future = delayed(write_to_zip_store)(toWrite,location)
                # future = toWrite.map_blocks(write_to_zip_store, location=location, dtype=bool)
                to_compute.append(future)

to_compute = dask.compute(to_compute)

client.close()

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


















    
    
    