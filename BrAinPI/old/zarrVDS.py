# -*- coding: utf-8 -*-
"""
Created on Tue Nov  9 16:26:33 2021

@author: alpha
"""

import os, glob, zarr, warnings
import numpy as np

from numcodecs import Blosc
compressor = Blosc(cname='zstd', clevel=9, shuffle=Blosc.BITSHUFFLE)

# ## Open fullres fmost CH1
# location = r"/CBI_Hive/globus/pitt/bil/CH1"
# location = r"h:/globus/pitt/bil/CH1"
# fileList = sorted(glob.glob(os.path.join(location,'*_CH1.tif')))

# testImage = io.imread(fileList[0])

# stack = [delayed(io.imread)(x) for x in fileList]
# stack = [da.from_delayed(x,shape=testImage.shape,dtype=testImage.dtype) for x in stack]
# imageStack = da.stack(stack)

class zarrVDS:
    
    def __init__(self, directoryStr, shape=None, dtype=None, chunks=None, compressor=None, ResolutionLevelLock=0):
        
        
        
        assert isinstance(directoryStr,str)
        assert isinstance(shape,tuple)
        assert len(shape) == 5,"All shapes and chunks must be 5 dimentions TCZYX"
        assert len(chunks) == 5,"All shapes and chunks must be 5 dimentions TCZYX"
        
        self.directoryStr = directoryStr
        self.shape = shape
        self.dtype = dtype
        self.ndim = len(self.shape)
        self.chunks = chunks
        
        # Force chunk dims 0,1 to == 1 for time and color
        if self.chunks[0] != 1:
            warnings.warn('Chunk dim 0 must be 1.  Resetting to 1')
            self.chunks = list(self.chunks)
            self.chunks[0] = 1
        if self.chunks[1] != 1:
            warnings.warn('Chunk dim 1 must be 1.  Resetting to 1')
            self.chunks = list(self.chunks)
            self.chunks[1] = 1
        self.chunks = tuple(self.chunks)
        
        self.compressor = compressor
        self.ResolutionLevelLock = ResolutionLevelLock
        
        
        # Set defaults
        if self.dtype is None:
            self.dtype = np.float32
        if self.compressor is None:
            self.compressor = Blosc(cname='zstd', clevel=9, shuffle=Blosc.BITSHUFFLE)
        
        # make location dir
        if os.path.exists(self.directoryStr) == False:
            os.makedirs(self.directoryStr)
            
        # Initialize the first and only required array
        self.initArray(0,0,0)
        
    
    def __getitem__(self, key):
        print(key)
        
        # res = self.ResolutionLock
        
        # if isinstance(key,tuple) and len(key) == 6 and isinstance(key[0], int):
        #     res = key[0]
        #     key = [x for x in key[1::]]
            
        
        # if isinstance(key, int):
        #     key = [slice(key)]
        #     for _ in range(self.ndim-1):
        #         key.append(slice(None))
        #     key = tuple(key)
            
        # if isinstance(key,tuple):
        #     key = [slice(x) if isinstance(x,int) else x for x in key]
        #     while len(key) < self.ndim:
        #         key.append(slice(None))
        #     key = tuple(key)
        
        # print(key)
        
        origionalKey = key
        res = self.ResolutionLevelLock
        
        if isinstance(key,slice) == False and isinstance(key,int) == False and len(key) == 6:
            res = key[0]
            # if res >= self.ResolutionLevels:
            #     raise ValueError('Layer is larger than the number of ResolutionLevels')
            key = tuple([x for x in key[1::]])
        
        ## All slices will be converted to 5 dims and placed into a tuple
        if isinstance(key,slice):
            key = [key]
        
        if isinstance(key, int):
            key = [slice(key)]
        
        ## Convert int/slice mix to a tuple of slices
        elif isinstance(key,tuple):
            key = tuple([slice(x) if isinstance(x,int) else x for x in key])
            
        key = list(key)
        while len(key) < 5:
            key.append(slice(None))
        key = tuple(key)
        
        print(res)
        print(key)
        

        ## Convert slice None to int
        newKey = []
        for num, idx in enumerate(key):
            if isinstance(idx.stop, int) and idx.start is None:
                newKey.append(slice(idx.stop,idx.stop+1,idx.step))
                
                ## Need to throw errors here
                if newKey[-1].stop >= self.shape[num]:
                    newKey[-1] = slice(newKey[-1].start,self.shape[num]-1,newKey[-1].step)
                
                if newKey[-1].start >= self.shape[num]:
                    newKey[-1] = slice(newKey[-1].stop-1,newKey[-1].stop,newKey[-1].step)
                
                if newKey[-1].step is None:
                    newKey[-1] = slice(newKey[-1].start,newKey[-1].stop,1)
            else:
                newKey.append(idx)
        
        
        key = newKey
        print(key)
        
        
        
        # if self.cache == None:
        #     return getSlice(
        #         self, 
        #         r = res if res is not None else 0,
        #         t = sliceFixer(self,key[0],'t',res=res),
        #         c = sliceFixer(self,key[1],'c',res=res),
        #         z = sliceFixer(self,key[2],'z',res=res),
        #         y = sliceFixer(self,key[3],'y',res=res), 
        #         x = sliceFixer(self,key[4],'x',res=res)
        #         )
        # else:
        #     return cache(location=self.cache_location,mem_size=self.mem_size,disk_size=self.disk_size)(getSlice)(
        #         self, 
        #         r = res if res is not None else 0,
        #         t = sliceFixer(self,key[0],'t',res=res),
        #         c = sliceFixer(self,key[1],'c',res=res),
        #         z = sliceFixer(self,key[2],'z',res=res),
        #         y = sliceFixer(self,key[3],'y',res=res), 
        #         x = sliceFixer(self,key[4],'x',res=res)
        #         )
        
        sliceReturned = getSlice(
                self, 
                r = res if res is not None else 0, #Force ResolutionLock of None to be 0 when slicing
                t = sliceFixer(self,key[0],'t',res=res),
                c = sliceFixer(self,key[1],'c',res=res),
                z = sliceFixer(self,key[2],'z',res=res),
                y = sliceFixer(self,key[3],'y',res=res), 
                x = sliceFixer(self,key[4],'x',res=res)
                )
        print('Image Slices Requested: {} / Item shape returned: {} \n'.format(origionalKey,sliceReturned.shape))
        return sliceReturned
        # return getArray(datasetNum=self.datasetNum,res=self.ResolutionLock,key=key)
        
        
        
        
        
        
        

    def location(self,r,t,c):
        return os.path.join(self.directoryStr,'{}.{}.{}.zarr'.format(r,t,c))
        
        
    def initArray(self,r,t,c):
        if os.path.exists(self.location(r,t,c)) == False:
            store = zarr.ZipStore(self.location(r,t,c))
            zarr.zeros(shape=self.shape[-2::], chunks=self.chunks[-2::], store=store, dtype=np.uint16,compressor=compressor)
            store.close()
    



def getSlice(imsClass,r,t,c,z,y,x):
    
    '''
    IMS stores 3D datasets ONLY with Resolution, Time, and Color as 'directory'
    structure witing HDF5.  Thus, data access can only happen accross dims XYZ
    for a specific RTC.  
    '''
    
    # incomingSlices = (r,t,c,z,y,x)
    tSize = list(range(imsClass.TimePoints)[t])
    cSize = list(range(imsClass.Channels)[c])
    zSize = len(range(imsClass.metaData[(r,0,0,'shape')][-3])[z])
    ySize = len(range(imsClass.metaData[(r,0,0,'shape')][-2])[y])
    xSize = len(range(imsClass.metaData[(r,0,0,'shape')][-1])[x])
    
    outputArray = np.zeros((len(tSize),len(cSize),zSize,ySize,xSize))
    # chunkRequested = outputArray.shape
    
    with h5py.File(imsClass.filePathComplete, 'r') as hf:
        for idxt, t in enumerate(tSize):
            for idxc, c in enumerate(cSize):
                # print(t)
                # print(c)
                dSetString = locationGenerator(r,t,c,data='data')
                outputArray[idxt,idxc,:,:,:] = hf[dSetString][z,y,x]
    
    
    ''' Some issues here with the output of these arrays.  Napari sometimes expects
    3-dim arrays and sometimes 5-dim arrays which originates from the dask array input representing
    tczyx dimentions of the imaris file.  When os.environ["NAPARI_ASYNC"] = "1", squeezing
    the array to 3 dimentions works.  When ASYNC is off squeese does not work.
    Napari throws an error because it did not get a 3-dim array.
    
    Am I implementing slicing wrong?  or does napari have some inconsistancy with the 
    dimentions of the arrays that it expects with different loading mechanisms if the 
    arrays have unused single dimentions.
    
    Currently "NAPARI_ASYNC" = '1' is set to one in the image loader
    Currently File/Preferences/Render Images Asynchronously must be turned on for this plugin to work
    '''
    try:
        # if os.environ["NAPARI_ASYNC"] == '1':
        #     while outputArray.shape[0] == 1 and len(outputArray.shape) > 1:
        #         outputArray = outputArray[0,:]
        #     # sliceOutput = outputArray.shape
        #     # print('Incoming Slices: {} / Slice Requested: {} / Slice Output {}'.format(incomingSlices,chunkRequested,sliceOutput))
        #     return outputArray
        
        ## Above code only eliminates low single length dims
        ## Squeeze will eliminate ALL single length dims
        if os.environ["NAPARI_ASYNC"] == '1':
            return np.squeeze(outputArray)
    except KeyError:
        pass
    
    # sliceOutput = outputArray.shape
    # print('Incoming Slices: {} / Slice Requested: {} / Slice Output {}'.format(incomingSlices,chunkRequested,sliceOutput))
    return outputArray


z = zarrVDS(r"Y:\bil", shape=(1,1,100,3000,3000), dtype=np.uint16,chunks=(2,1,1,1000,1000))  























