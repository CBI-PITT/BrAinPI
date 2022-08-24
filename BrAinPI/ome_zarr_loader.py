# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 14:12:11 2021

@author: alpha
"""

import zarr, os, glob, itertools
import numpy as np

from stack_to_multiscale_ngff.h5_shard_store import H5_Shard_Store

# location = r'H:\globus\pitt\bil'

# images = glob.glob(os.path.join(location,'t00_*.zarr'))
# images = [zarr.NestedDirectoryStore(x) for x in images]



# images = [da.from_zarr(x) for x in images]

# napari.view_image(images,contrast_limits=[0,65534])



class ome_zarr_loader:
    def __init__(self, location, ResolutionLevelLock=None, zarr_store_type=H5_Shard_Store, verbose=None, squeeze=True):
        
        self.location = location
        self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock
        self.zarr_store_type = zarr_store_type
        self.verbose = verbose
        self.squeeze = squeeze
        self.metaData = {}
        
        store = self.zarr_store_type(self.location)
        zgroup = zarr.open(store)
        self.zattrs = zgroup.attrs
        
        assert 'omero' in self.zattrs
        self.omero = zgroup.attrs['omero']
        assert 'multiscales' in self.zattrs
        self.multiscales = zgroup.attrs['multiscales']
        del zgroup
        del store
        
        self.multiscale_datasets = self.multiscales[0]['datasets']
        self.ResolutionLevels = len(self.multiscale_datasets)
        
        self.dataset_paths = []
        self.dataset_scales = []
        for res in range(self.ResolutionLevels):
            self.dataset_paths.append(self.multiscale_datasets[res]['path'])
            self.dataset_scales.append(self.multiscale_datasets[res]['coordinateTransformations'][0]['scale'])
        
        
        for r in range(self.ResolutionLevels):
            array = self.open_array(r)
            if r == 0:
                self.TimePoints = array.shape[0]
                self.Channels = array.shape[1]
                
            for t,c in itertools.product(range(self.TimePoints),range(self.Channels)):
                
                # Collect attribute info
                self.metaData[r,t,c,'shape'] = (t+1,c+1,*array.shape[2:])
                ## Need to extract resolution by some other means.  For now, default to 1,1,1 and divide by 2 for each series
                self.metaData[r,t,c,'resolution'] = self.dataset_scales[r][2:]
                         
                # Collect dataset info
                self.metaData[r,t,c,'chunks'] = array.chunks
                self.metaData[r,t,c,'dtype'] = array.dtype
                self.metaData[r,t,c,'ndim'] = array.ndim
                
                try:
                    self.metaData[r,t,c,'max'] = self.omero['channels'][c]['window']['end']
                    self.metaData[r,t,c,'min'] = self.omero['channels'][c]['window']['start']
                except:
                    pass
        
        self.change_resolution_lock(self.ResolutionLevelLock)
    

    def change_resolution_lock(self,ResolutionLevelLock):
        self.ResolutionLevelLock = ResolutionLevelLock
        self.shape = self.metaData[self.ResolutionLevelLock,0,0,'shape']
        self.ndim = len(self.shape)
        self.chunks = self.metaData[self.ResolutionLevelLock,0,0,'chunks']
        self.resolution = self.metaData[self.ResolutionLevelLock,0,0,'resolution']
        self.dtype = self.metaData[self.ResolutionLevelLock,0,0,'dtype']
    
    
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
        structure witing HDF5.  Thus, data access can only happen accross dims XYZ
        for a specific RTC.  
        '''
        
        incomingSlices = (r,t,c,z,y,x)
        print(incomingSlices)
        
        array = self.open_array(r)[t,c,z,y,x]
        if self.squeeze:
            return np.squeeze(array)
        else:
            return array
    
    
    def locationGenerator(self,res):
        return os.path.join(self.location,self.dataset_paths[res])
    
    def open_array(self,res):
        store = self.zarr_store_type(self.locationGenerator(res))
        return zarr.open(store)
    
    




# class ome_zarr_loader:
#     def __init__(self, location, ResolutionLevelLock=None, zarr_store_type=H5_Shard_Store, verbose=None, squeeze=True):
        
#         self.location = location
#         self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock
#         self.zarr_store_type = zarr_store_type
#         self.verbose = verbose
#         self.squeeze = squeeze
#         self.metaData = {}
        
#         store = self.zarr_store_type(self.location)
#         zgroup = zarr.open(store)
#         self.zattrs = zgroup.attrs
        
#         assert 'omero' in self.zattrs
#         self.omero = zgroup.attrs['omero']
#         assert 'multiscales' in self.zattrs
#         self.multiscales = zgroup.attrs['multiscales']
#         del zgroup
#         del store
        
#         self.multiscale_datasets = self.multiscales[0]['datasets']
#         self.ResolutionLevels = len(self.multiscale_datasets)
        
#         self.dataset_paths = []
#         self.dataset_scales = []
#         for res in range(self.ResolutionLevels):
#             self.dataset_paths.append(self.multiscale_datasets[res]['path'])
#             self.dataset_scales.append(self.multiscale_datasets[res]['coordinateTransformations'][0]['scale'])
        
        
#         for r in range(self.ResolutionLevels):
            
#             array = self.open_array(r)
            
#             if r == 0:
#                 self.TimePoints = array.shape[0]
#                 self.Channels = array.shape[1]
            
#             # Collect attribute info
#             self.metaData[r,'shape'] = array.shape
#             ## Need to extract resolution by some other means.  For now, default to 1,1,1 and divide by 2 for each series
#             self.metaData[r,'resolution'] = self.dataset_scales[r]
                     
#             # Collect dataset info
#             self.metaData[r,'chunks'] = array.chunks
#             self.metaData[r,'dtype'] = array.dtype
#             self.metaData[r,'ndim'] = array.ndim
        
#         self.change_resolution_lock(self.ResolutionLevelLock)
    

#     def change_resolution_lock(self,ResolutionLevelLock):
#         self.ResolutionLevelLock = ResolutionLevelLock
#         self.shape = self.metaData[self.ResolutionLevelLock,'shape']
#         self.ndim = len(self.shape)
#         self.chunks = self.metaData[self.ResolutionLevelLock,'chunks']
#         self.resolution = self.metaData[self.ResolutionLevelLock,'resolution']
#         self.dtype = self.metaData[self.ResolutionLevelLock,'dtype']
    
    
#     def __getitem__(self,key):
        
#         res = 0 if self.ResolutionLevelLock is None else self.ResolutionLevelLock
#         print(key)
#         if isinstance(key,slice) == False and isinstance(key,int) == False and len(key) == 6:
#             res = key[0]
#             if res >= self.ResolutionLevels:
#                 raise ValueError('Layer is larger than the number of ResolutionLevels')
#             key = tuple([x for x in key[1::]])
#         print(res)
#         print(key)
        
#         if isinstance(key, int):
#             key = [slice(key,key+1)]
#             for _ in range(self.ndim-1):
#                 key.append(slice(None))
#             key = tuple(key)
            
#         if isinstance(key,tuple):
#             key = [slice(x,x+1) if isinstance(x,int) else x for x in key]
#             while len(key) < self.ndim:
#                 key.append(slice(None))
#             key = tuple(key)
        
#         print(key)
#         newKey = []
#         for ss in key:
#             if ss.start is None and isinstance(ss.stop,int):
#                 newKey.append(slice(ss.stop,ss.stop+1,ss.step))
#             else:
#                 newKey.append(ss)
                
#         key = tuple(newKey)
#         print(key)
        
        
#         return self.getSlice(
#                         r=res,
#                         t = key[0],
#                         c = key[1],
#                         z = key[2],
#                         y = key[3],
#                         x = key[4]
#                         )
        


#     def getSlice(self,r,t,c,z,y,x):
        
#         '''
#         IMS stores 3D datasets ONLY with Resolution, Time, and Color as 'directory'
#         structure witing HDF5.  Thus, data access can only happen accross dims XYZ
#         for a specific RTC.  
#         '''
        
#         incomingSlices = (r,t,c,z,y,x)
#         print(incomingSlices)
        
#         array = self.open_array(r)[t,c,z,y,x]
#         if self.squeeze:
#             return np.squeeze(array)
#         else:
#             return array
    
    
#     def locationGenerator(self,res):
#         return os.path.join(self.location,self.dataset_paths[res])
    
#     def open_array(self,res):
#         store = self.zarr_store_type(self.locationGenerator(res))
#         return zarr.open(store)
    
    
    
    
    
