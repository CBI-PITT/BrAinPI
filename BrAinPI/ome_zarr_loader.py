# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 14:12:11 2021

@author: alpha
"""

import zarr, os, glob, itertools
import numpy as np

# Import zarr stores
from zarr.storage import NestedDirectoryStore
from zarr_stores.archived_nested_store import Archived_Nested_Store
from zarr_stores.h5_nested_store import H5_Nested_Store


class ome_zarr_loader:
    def __init__(self, location, ResolutionLevelLock=None, zarr_store_type=NestedDirectoryStore, verbose=None, squeeze=True, cache=None):
        
        location = location
        self.location = location
        self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock
        
        if isinstance(zarr_store_type,str):
            if zarr_store_type=='hns':
                self.zarr_store_type = H5_Nested_Store
            elif zarr_store_type == 'oz':
                self.zarr_store_type = NestedDirectoryStore
            elif zarr_store_type=='ans':
                self.zarr_store_type = Archived_Nested_Store

        else:
            self.zarr_store_type = zarr_store_type

        self.verbose = verbose
        self.squeeze = squeeze
        self.cache = cache
        self.metaData = {}
        
        store = self.zarr_store_type(self.location)
        zgroup = zarr.open(store)
        self.zattrs = zgroup.attrs
        
        if 'omero' in self.zattrs:
            self.omero = zgroup.attrs['omero']
        # assert 'omero' in self.zattrs
        # self.omero = zgroup.attrs['omero']
        assert 'multiscales' in self.zattrs
        self.multiscales = zgroup.attrs['multiscales']
        print(self.multiscales)
        del zgroup
        del store
        
        try:
            self.multiscale_datasets = self.multiscales[0]['datasets']
        except:
            self.multiscale_datasets = self.multiscales['datasets']
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
        
        self.arrays = {}
        for res in range(self.ResolutionLevels):
            self.arrays[res] = self.open_array(res)
    

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
        
        
        array = self.getSlice(
                        r=res,
                        t = key[0],
                        c = key[1],
                        z = key[2],
                        y = key[3],
                        x = key[4]
                        )
        
        if self.squeeze:
            return np.squeeze(array)
        else:
            return array
        
    
    def _get_memorize_cache(self, name=None, typed=False, expire=None, tag=None, ignore=()):
        if tag is None: tag = self.location
        return self.cache.memorize(
            name=name,
            typed=typed,
            expire=expire,
            tag=tag,
            ignore=ignore
            ) if self.cache is not None else lambda x: x
    
    def getSlice(self,r,t,c,z,y,x):
        
        '''
        Access the requested slice based on resolution level and 
        5-dimentional (t,c,z,y,x) access to zarr array.
        '''
        
        incomingSlices = (r,t,c,z,y,x)
        print(incomingSlices)
        if self.cache is not None:
            key = f'{self.location}_getSlice_{str(incomingSlices)}'
            # key = self.location + '_getSlice_' + str(incomingSlices)
            result = self.cache.get(key, default=None, retry=True)
            if result is not None:
                print(f'Returned from cache: {incomingSlices}')
                return result
        
        result = self.arrays[r][t,c,z,y,x]

        if self.cache is not None:
            self.cache.set(key, result, expire=None, tag=self.location, retry=True)
            # test = True
            # while test:
            #     # print('Caching slice')
            #     self.cache.set(key, result, expire=None, tag=self.location, retry=True)
            #     if result == self.getSlice(*incomingSlices):
            #         test = False

        
        return result
        # return self.open_array(r)[t,c,z,y,x]
    
    
    def locationGenerator(self,res):
        return os.path.join(self.location,self.dataset_paths[res])
    
    def open_array(self,res):
        store = self.zarr_store_type(self.locationGenerator(res))
        # try:
        #     if self.cache is not None:
        #         store = disk_cache_store(store=store, uuid=self.locationGenerator(res), diskcache_object=self.cache, persist=None, meta_data_expire_min=15)
        # except Exception as e:
        #     print('Caught Exception')
        #     print(e)
        #     pass
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
    
    
    
    
    
