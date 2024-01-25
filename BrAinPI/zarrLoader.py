# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 14:12:11 2021

@author: alpha
"""

import zarr, os, glob, itertools
import numpy as np

# location = r'H:\globus\pitt\bil'

# images = glob.glob(os.path.join(location,'t00_*.zarr'))
# images = [zarr.NestedDirectoryStore(x) for x in images]



# images = [da.from_zarr(x) for x in images]

# napari.view_image(images,contrast_limits=[0,65534])


class zarrSeries:
    def __init__(self,file, ResolutionLevelLock=None):
        
        
        self.fileSeries = sorted(glob.glob(os.path.join(os.path.split(file)[0],'t[0-9][0-9]_c[0-9][0-9]_*.zarr')))
        self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock
        self.metaData = {}
        
        t = None
        idx = 0
        while t is None:
            if all(['t{}'.format(str(idx).zfill(2)) in x for x in self.fileSeries]):
                print('int')
                idx += 1
            else:
                t='Done'
                
        self.TimePoints = idx
        
        c = None
        idx = 0
        while c is None:
            if all(['c{}'.format(str(idx).zfill(2)) in x for x in self.fileSeries]):
                print('inc')
                idx += 1
            else:
                c='Done'
        
        self.Channels = idx
        
        self.ResolutionLevels = max([int(os.path.splitext(x)[0].split('_')[-1]) for x in self.fileSeries])
        
        zarrStore = zarr.NestedDirectoryStore(self.fileSeries[0])
        zarrFile = zarr.open(zarrStore)
        
        self.shape = (self.TimePoints,self.Channels,zarrFile.shape[0],zarrFile.shape[1],zarrFile.shape[2])
        self.chunks = (1,1,zarrFile.chunks[0],zarrFile.chunks[1],zarrFile.chunks[2])
        self.ndim = len(self.shape)
        self.dtype = zarrFile.dtype
        
        for r,t,c in itertools.product(range(self.ResolutionLevels), range(self.TimePoints), range(self.Channels)):
            
            currentFile = self.locationGenerator(r,t,c)
            # currentFile = os.path.join(os.path.split(self.fileSeries[0])[0],currentFile)
            print(currentFile)
            zarrStore = zarr.NestedDirectoryStore(currentFile)
            zarrFile = zarr.open(zarrStore)
            
            # Collect attribute info
            self.metaData[r,t,c,'shape'] = (t+1,
                                            c+1,
                                            zarrFile.shape[0],
                                            zarrFile.shape[1],
                                            zarrFile.shape[2]
                                       )
            ## Need to extract resolution by some other means.  For now, default to 1,1,1 and divide by 2 for each series
            self.metaData[r,t,c,'resolution'] = tuple([x*(2**r) for x in (1,0.3,0.3)])#(1,1,1)
                     
            # Collect dataset info
            self.metaData[r,t,c,'chunks'] = (1,1,zarrFile.chunks[0],zarrFile.chunks[1],zarrFile.chunks[2])
            self.metaData[r,t,c,'dtype'] = zarrFile.dtype
            self.metaData[r,t,c,'ndim'] = len(self.metaData[r,t,c,'chunks'])
        
        if isinstance(self.ResolutionLevelLock, int):
            self.shape = self.metaData[self.ResolutionLevelLock,t,c,'shape']
            self.ndim = len(self.shape)
            self.chunks = self.metaData[self.ResolutionLevelLock,t,c,'chunks']
            self.resolution = self.metaData[self.ResolutionLevelLock,t,c,'resolution']
            self.dtype = self.metaData[self.ResolutionLevelLock,t,c,'dtype']
        

        
    def __getitem__(self,key):
        
        res = 0 if self.ResolutionLevelLock is None else self.ResolutionLevelLock
        print(key)
        if isinstance(key,slice) == False and isinstance(key,int) == False and len(key) == 6:
            res = key[0]
            if res >= self.ResolutionLevels:
                raise ValueError('Layer is larger than the number of ResolutionLevels')
            key = tuple([x for x in key[1::]])
        print(res)
        print(key)
        
        if isinstance(key, int):
            key = [slice(key,key+1)]
            for _ in range(self.ndim-1):
                key.append(slice(None))
            key = tuple(key)
            
        if isinstance(key,tuple):
            key = [slice(x,x+1) if isinstance(x,int) else x for x in key]
            while len(key) < self.ndim:
                key.append(slice(None))
            key = tuple(key)
        
        print(key)
        newKey = []
        for ss in key:
            if ss.start is None and isinstance(ss.stop,int):
                newKey.append(slice(ss.stop,ss.stop+1,ss.step))
            else:
                newKey.append(ss)
                
        key = tuple(newKey)
        print(key)
        
        
        return self.getSlice(
                        r=res,
                        t = key[0],
                        c = key[1],
                        z = key[2],
                        y = key[3],
                        x = key[4]
                        )
        


    def getSlice(self,r,t,c,z,y,x):
        
        '''
        IMS stores 3D datasets ONLY with Resolution, Time, and Color as 'directory'
        structure witing HDF5.  Thus, data access can only happen across dims XYZ
        for a specific RTC.  
        '''
        
        incomingSlices = (r,t,c,z,y,x)
        print(incomingSlices)
        tSize = list(range(self.TimePoints)[t])
        cSize = list(range(self.Channels)[c])
        zSize = len(range(self.metaData[(r,0,0,'shape')][-3])[z])
        ySize = len(range(self.metaData[(r,0,0,'shape')][-2])[y])
        xSize = len(range(self.metaData[(r,0,0,'shape')][-1])[x])
        
        # Casting zeros to specific dtype significantly speeds up data retrieval
        outputArray = np.zeros((len(tSize),len(cSize),zSize,ySize,xSize), dtype=self.dtype)
        # chunkRequested = outputArray.shape
        
        for idxt, t in enumerate(tSize):
            for idxc, c in enumerate(cSize):
                print('r{},t{},c{}'.format(r,t,c))
                print(self.locationGenerator(r,t,c))
                zarrStore = zarr.NestedDirectoryStore(self.locationGenerator(r,t,c))
                zarrFile = zarr.open(zarrStore)
                print(zarrFile.shape)
                outputArray[idxt,idxc,:,:,:] = zarrFile[z,y,x]
        
        # print('Incoming Slices: {} / Slice Requested: {} / Slice Output {}'.format(incomingSlices,chunkRequested,sliceOutput))
        # return np.squeeze(outputArray)
        return outputArray
    
    
    def locationGenerator(self,r,t,c):
        
        currentFile = 't{}_c{}_{}.zarr'.format(str(t).zfill(2),
                                 str(c).zfill(2),
                                 str(r)
                                 )
        currentFile = os.path.join(os.path.split(self.fileSeries[0])[0],currentFile)
        
        return currentFile    
