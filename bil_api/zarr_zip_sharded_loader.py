# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 15:47:35 2022

@author: alpha
"""
import os
import glob
import natsort
import itertools
import dask
from dask.delayed import delayed
import dask.array as da
import zarr

class zarr_zip_sharded:
    
    def __init__(self, location, ResolutionLevelLock=None, squeeze=True, compute=True):
        
        self.location = location
        self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock
        self.compute = compute
        self.squeeze = squeeze
        self.metaData = {}
        
        self.ResolutionLevels = len(glob.glob(os.path.join(self.location,'[0-9]')))
        
        self.dataset = {}
        for res in range(self.ResolutionLevels):
            print('Assembling Resolution Level {}'.format(res))
            # self.dataset[res] = build_array_res_level(self.location,res) # Works single threaded
            # self.dataset[res] = no_concat(location,res) # Works to build array with only da.stack commands
            self.dataset[res] = delayed(self.build_array_par)(location,res) # Works full parallel assembly
        self.dataset = dask.compute(self.dataset)[0]
            
        
        shape = self.dataset[0].shape
        self.TimePoints = shape[0]
        self.Channels = shape[1]
        
        self.collect_metadata()
        self.change_resolution_lock(self.ResolutionLevelLock)
        
    def collect_metadata(self):
        for r, t, c in itertools.product(range(self.ResolutionLevels), range(self.TimePoints),
                                         range(self.Channels)):
            
            self.metaData[(r,t,c,'chunks')] = self.dataset[r].chunksize
            self.metaData[(r,t,c,'dtype')] = self.dataset[r].dtype
            self.metaData[(r,t,c,'shape')] = self.dataset[r].shape
            self.metaData[(r,t,c,'ndim')] = self.dataset[r].ndim
            self.metaData[(r,t,c,'size')] = self.dataset[r].size
            
    def change_resolution_lock(self,ResolutionLevelLock):
        self.ResolutionLevelLock = ResolutionLevelLock
        self.shape = self.metaData[(self.ResolutionLevelLock,0,0,'shape')]
        self.size = self.metaData[(self.ResolutionLevelLock,0,0,'size')]
        self.ndim = self.metaData[(self.ResolutionLevelLock,0,0,'ndim')]
        self.chunks = self.metaData[(self.ResolutionLevelLock,0,0,'chunks')]
        self.dtype = self.metaData[(self.ResolutionLevelLock,0,0,'dtype')]
        
    def __getitem__(self,key):
        # print(key)
        res = self.ResolutionLevelLock
        
        if isinstance(key,(int,slice)):
            key = (key)
        elif isinstance(key,tuple) and len(key) == 6:
            res = key[0]
            key = key[1:]
        
        newKey = []
        for ii in key:
            if isinstance(ii,slice):
                newKey.append(ii)
            elif isinstance(ii,int):
                newKey.append(slice(ii,ii+1))
            else:
                raise NotImplementedError('Slice must contain an integers or slice objects only')
        
        while len(newKey) < 5:
            newKey = newKey + [slice(None)]
        
        data = self.dataset[res][newKey[0],newKey[1],newKey[2],newKey[3],newKey[4]]
        
        if self.squeeze:
            data = da.squeeze(data)
        
        if self.compute:
            return data.compute()
        else:
            return data
    
    def build_array_par(self,location,res):
        '''
        Build a dask array representation of a specific resolution level
        Always output a 5-dim array (t,c,z,y,x)
        '''
        
        # Determine the number of TimePoints (int)
        TimePoints = len(glob.glob(os.path.join(location,str(res),'[0-9]')))
        
        # Determine the number of Channels (int)
        Channels = len(glob.glob(os.path.join(location,str(res),'0','[0-9]')))
        
        # Build a dask array from underlying zarr ZipStores
        
        stack = []
        for t in range(TimePoints):
            colors = []
            
            for c in range(Channels):
                z_shard_list = natsort.natsorted(glob.glob(os.path.join(location,str(res),str(t),str(c),'*.zip')))
                
                single_color_stack = [delayed(self.make_da_zarr)(file) for file in z_shard_list]
                # single_color_stack = dask.compute(single_color_stack)[0]
                single_color_stack = delayed(da.concatenate)(single_color_stack,axis=0)
                colors.append(single_color_stack)
                
            colors = delayed(da.stack)(colors)
            stack.append(colors)
        stack = delayed(da.stack)(stack)
        
        return stack.compute()
    
    def make_da_zarr(file):
        return da.from_zarr(zarr.ZipStore(file),name=file)


        