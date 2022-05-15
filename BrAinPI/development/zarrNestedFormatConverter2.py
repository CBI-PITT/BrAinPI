# -*- coding: utf-8 -*-
"""
Created on Fri Nov 19 12:18:24 2021

@author: alpha
"""

import os, glob, zarr, math
import numpy as np
import dask
import dask.array as da
from dask.delayed import delayed
from skimage import io
from skimage.filters import gaussian
from skimage import img_as_float32, img_as_uint, img_as_ubyte
import itertools

from distributed import Client


'''
Final result will
Expect data to be ordered as:
    T[0-9]
        CH[0-9]
            image0001.tif
            image0002.tif
            ...
'''

dataToConvert = r"H:\globus\pitt\bil"
dataToConvert = r"H:\globus\pitt\bil\TEST"
dataToConvert = "//136.142.29.160/CBI_Hive/globus/pitt/bil/TEST"
dataToConvert = "/CBI_Hive/globus/pitt/bil/TEST"


processingChunks = (5,1024,1024)
scale = (1.0, 0.35, 0.35)  ## (z,y,x) arbitraty units but is probably microns

outputLocation = os.path.join(dataToConvert,'fMOST.zarr')

print('Finding Timepoints')
timeDirs = sorted(glob.glob(os.path.join(dataToConvert,'T[0-9]')))

print('Finding Channels')
if len(timeDirs) > 0: 
    channelDirs = [sorted(glob.glob(os.path.join(x,'CH[0-9]'))) for x in timeDirs]
else:
    channelDirs = sorted(glob.glob(os.path.join(dataToConvert,'CH[0-9]')))

print('Finding Images')
if len(channelDirs) > 0: 
    images = [sorted(glob.glob(os.path.join(x,'*.tif'))) for x in channelDirs]
else:
    images = sorted(glob.glob(os.path.join(dataToConvert,'*.tif')))

print('Loading a sample image')
sampleImage = io.imread(images[0][0])

print('Forming lazy dask array')
data = []
for ch in images:
    data.append(
        [delayed(io.imread)(x) for x in ch]
        )

    data[-1] = [
        da.from_delayed(x,
        shape=(sampleImage.shape[0],sampleImage.shape[1]), 
        dtype=sampleImage.dtype) for x in data[-1]
        ]

stack = da.stack(data)

while len(stack.shape) < 5:
    stack = stack[None,...]


'''
Assumptions

After the previous code, 'stack' will be a multi-dimentional lazy dask array
dims=(t,c,z,y,x).  Less than 5 dims will remove the left-most dim.

for example stack.shape == (2, 11464, 30801, 20821) is assumed to be (c,z,y,x) with no time dim


multi resolution zarr arrays will be stored in a single directory ir each resolution series:
a single array t,c,z,y,x will be stored in each resoltion dir.

    r[0-9][0-9] = 5D array (t,c,z,y,x)
            
'''

################################################################################
'''
Resolution series should converge towards 3D isotropic but always with 2x reductions 
in each dimension and never going beyond iostropic.  
this requires knowing the origional z,y,x scale

Fit to exactly iostropic with reductions in resolution no greater than 2x

Chunks will change every 2 levels. (z*2,y//2,x//2)

RLevel:  Resolution            Pixel Shape         Chunks           Downsample Factor

resolutions = {
 0: ((1.0, 0.35, 0.35), (11464, 30801, 20821), (1, 1024, 1024), (1,1,1)),
 1: ((1.0, 0.7, 0.7), (11464, 15400, 10410), (1, 1024, 1024), (1,2,2)),
 2: ((1.0, 1.0, 1.0), (11464, 10779, 7286), (2, 512, 512), (1,1.43,1.43)),
 3: ((2.0, 2.0, 2.0), (5732, 5389, 3643), (2, 512, 512), (2,2,2)),
 4: ((4.0, 4.0, 4.0), (2866, 2694, 1821), (4, 256, 256), (2,2,2)),
 5: ((8.0, 8.0, 8.0), (1433, 1347, 910), (4, 256, 256), (2,2,2)),
 6: ((16.0, 16.0, 16.0), (716, 673, 455), (8, 128, 128), (2,2,2)),
 7: ((32.0, 32.0, 32.0), (358, 336, 227), (8, 128, 128), (2,2,2)),
 8: ((64.0, 64.0, 64.0), (179, 168, 113), (16, 64, 64), (2,2,2)),
 9: ((128.0, 128.0, 128.0), (89, 84, 56), (16, 64, 64), (2,2,2)),
 10: ((256.0, 256.0, 256.0), (44, 42, 28), (32, 32, 32), (2,2,2))
 }

'''


### Determine number of resolution series
maxChunkSize = (1,1024,1024)
minChunkSize = (32,64,64)
minPixelShape = 16
currentShape = stack.shape[-3::]
currentResolution = scale
currentChunks = maxChunkSize

resolutions = {0:(currentResolution,currentShape, currentChunks, (1,1,1))} # Key is resolution number, value = (resolution, pixels, chunks)

print('Calculating Resolution Levels')
level = 1
while all([x//2 > minPixelShape for x in currentShape]):
    
    if len(set(currentResolution)) == 1:
        resolutions[level] = (
            tuple([x*2 for x in currentResolution]),
            tuple([x//2 for x in currentShape]),
            currentChunks,
            (2,2,2)
            )
    
        currentResolution = resolutions[level][0]
        currentShape = resolutions[level][1]
    
    elif len(set(currentResolution)) > 1:
        
        resSet = sorted(list(set(currentResolution)))
        newResolution = []
        newPixels = []
        
        changeFactor = 1
        DSF = []
        for rr,ss in zip(currentResolution,currentShape):
            if rr < resSet[-1]:
                changeFactor = resSet[-1]/rr if resSet[-1]/rr <=2 else 2
            else:
                changeFactor = 1
            
            DSF.append(changeFactor)
            newResolution.append(rr*changeFactor)
            newPixels.append(ss//changeFactor)
        
        currentResolution = tuple(newResolution)
        currentShape = tuple([int(x) for x in newPixels])
        
        resolutions[level] = (
            currentResolution,
            currentShape,
            currentChunks,
            tuple(DSF)
            )
    
    level += 1
    # Set chunk shape for next resolution level
    if level%2 == 0:
        currentChunks = (currentChunks[0]*2,currentChunks[1]//2,currentChunks[2]//2)
    
print(resolutions)
    



###  Setup zarr layout
## Force a 5D array (t,c,z,y,x)

os.makedirs(outputLocation,exist_ok=True)

for r in resolutions:
    
    print('Making zarr dataset resolution level {}'.format(r))
    store = zarr.NestedDirectoryStore(outputLocation)
    if r == 0:
        root = zarr.group(store=store, overwrite=True)
    else:
        root = zarr.group(store=store, overwrite=False)
    root.zeros(str(r).zfill(2), 
               shape=(stack.shape[0],stack.shape[1],resolutions[r][1][0],resolutions[r][1][1],resolutions[r][1][2]),
               chunks=(1,1,resolutions[r][2][0],resolutions[r][2][1],resolutions[r][2][2]),
               dtype=sampleImage.dtype)
    
    del root
    del store

# ## Mount dask arrays from zarr
# zarr_arrays = {}
# for r in resolutions:
#     print('Opening zarr arrays {}'.format(str(r).zfill(2)))
#     location=os.path.join(outputLocation,str(r).zfill(2))
#     zarr_arrays[r] = zarr.open(
#         zarr.NestedDirectoryStore(location),
#         mode='a'
#         )


# ## For writing arrays we will split them by color
# toWrite = []
# for dd in range(stack.shape[1]):
#     toWrite.append(stack[:,[dd]])
    

def smooth(image, sigma=(0,0,0)):
    '''A function for delayed gaussian blur'''
    if sigma == (0,0,0):
        return image
    
    dtype = image.dtype
    workingImg = img_as_float32(image)
    workingImg = gaussian(workingImg,sigma)
    
    if dtype == 'uint16':
        workingImg = img_as_uint(workingImg)
    elif dtype == 'uint8':
        workingImg = img_as_ubyte(workingImg)
    
    return workingImg



# def saveToZarr(array,zarr_store,block_info=None):
#     block_location = block_info[None]['array-location']
#     d0_start = block_location[0][0]
#     d0_stop = block_location[0][1]
#     d1_start = block_location[1][0]
#     d1_stop = block_location[1][1]
#     d2_start = block_location[2][0]
#     d2_stop = block_location[2][1]
#     d3_start = block_location[3][0]
#     d3_stop = block_location[3][1]
#     d4_start = block_location[4][0]
#     d4_stop = block_location[4][1]
    
#     a = zarr.open(zarr_store)
#     a[
#       d0_start:d0_stop,
#       d1_start:d1_stop,
#       d2_start:d2_stop,
#       d3_start:d3_stop,
#       d4_start:d4_stop
#       ]
#     return np.zeros((1),dtype=bool)


def saveToZarr(array,t,c,zarr_store,block_info=None):
    print('In saveToZarr')
    block_location = block_info[None]['array-location']
    d0_start = block_location[0][0]
    d0_stop = block_location[0][1]
    d1_start = block_location[1][0]
    d1_stop = block_location[1][1]
    d2_start = block_location[2][0]
    d2_stop = block_location[2][1]
    
    print(array.shape)
    a = zarr.open(zarr_store, mode='r+')
    a[t,
      c,
      d0_start:d0_stop,
      d1_start:d1_stop,
      d2_start:d2_stop
      ] = array
    return np.zeros(array.shape,dtype=bool)







for t,c in itertools.product(
        range(stack.shape[0]),
        range(stack.shape[1])
        ):
    
    ## Form downsample series
    downSample = {}
    for r in resolutions:
        print('Staging downsample of resolution {}'.format(r))
        if r == 0:
            single_color_stack = stack[t,c]
            downSample[r] = single_color_stack.rechunk(chunks=(
                                                  processingChunks[0],
                                                  processingChunks[1],
                                                  processingChunks[2]
                                                  # resolutions[r][2][0],
                                                  # resolutions[r][2][1],
                                                  # resolutions[r][2][2]
                                                  ))
        else:
            single_color_stack = downSample[r-1]
            
            previousPixels = resolutions[r-1][1]
            pixels = resolutions[r][1]
            previousDownsampleFactor = resolutions[r-1][3]
            downSampleFactor = resolutions[r][3]
            # downSampleFactor = tuple([x/y for x,y in zip(resolutions[r-1][1],resolutions[r][1])])
            sigma = tuple([(x - 1) / 2 for x in downSampleFactor])
            depth = tuple([math.ceil(2*x) for x in sigma])
            blured = da.map_overlap(smooth, single_color_stack,sigma=sigma,depth=depth,boundary='reflect',trim=True,dtype=sampleImage.dtype)
            
            
            if all([isinstance(x, int) for x in downSampleFactor]):
                print('Single INT index')
                a = blured[1::downSampleFactor[0], 1::downSampleFactor[1], 1::downSampleFactor[2]]
            else:
                with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                    print('First index')
                    if isinstance(downSampleFactor[0],int):
                        z = downSampleFactor[0]
                        a = blured[1::z]
                    else:
                        z = list(np.round(np.linspace(0, previousPixels[0] - 1, pixels[0])).astype(int))
                        a = blured[z]
                    
                    print('Second index')
                    if isinstance(downSampleFactor[1],int):
                        y = downSampleFactor[1]
                        a = a[:,1::y]
                    else:
                        y = list(np.round(np.linspace(0, previousPixels[1] - 1, pixels[1])).astype(int))
                        a = a[:,y]
                    
                    print('Third index')
                    if isinstance(downSampleFactor[2],int):
                        x = downSampleFactor[2]
                        a = a[:,:,1::x]
                    else:
                        x = list(np.round(np.linspace(0, previousPixels[2] - 1, pixels[2])).astype(int))
                        a = a[:,:,x]
                    
                # with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            
            
            
            downSample[r] = a.rechunk(processingChunks)
    
    toMake = []
    for r in resolutions:
        
        location=os.path.join(outputLocation,str(r).zfill(2))
        make = da.map_blocks(saveToZarr,
                      downSample[r],
                      t,
                      c,
                      zarr.NestedDirectoryStore(location), 
                      # chunks=(1,1,1),
                      dtype=bool,
                      meta=np.array((), dtype=bool)
                      )
        make = make.sum()
        toMake.append(make)
    client = Client()
    # client = Client(processes=False)
    print('Computing')
    z = client.compute(toMake)
    # z = client.compute(toMake, scheduler='threads')
    # z = client.compute(toMake, scheduler='processes')
    # z = client.compute(toMake, scheduler='distributed')

    
    z = da.compute(toMake,processes=True)

    break





# ## Create delayed da.to_zarr in a list that can be used to call compute
# zarr_arrays = []
# for r in resolutions:
#     print('Create delayed da.to_zarr resolution {}'.format(str(r).zfill(2)))
#     location=os.path.join(outputLocation,str(r).zfill(2))
#     zarr_arrays.append(
#         da.to_zarr
#         (
#         downSample[r],
#         zarr.NestedDirectoryStore(location),
#         compute=False
#         )
#         )

# da.compute(zarr_arrays)



















