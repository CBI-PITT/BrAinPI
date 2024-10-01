# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 14:12:11 2021

@author: alpha
"""
import io

import zarr, os, itertools
import numpy as np

# # Import zarr stores
from zarr.storage import NestedDirectoryStore
# from zarr_stores.archived_nested_store import Archived_Nested_Store
# from zarr_stores.h5_nested_store import H5_Nested_Store

from collections.abc import MutableMapping
from zarr._storage.store import Store, BaseStore
from typing import Union
Path = Union[str, bytes, None]
StoreLike = Union[BaseStore, Store, MutableMapping]
from logger_tools import logger
# import s3fs

class nifti_zarr_loader:
    def __init__(self, location, ResolutionLevelLock=None, zarr_store_type: StoreLike=NestedDirectoryStore, verbose=None, squeeze=True, cache=None):

        # assert StoreLike is s3fs.S3Map or any([issubclass(zarr_store_type,x) for x in StoreLike.__args__]), 'zarr_store_type is not a zarr storage class'

        self.location = location
        self.s3 = False
        # if 's3://' in location:
        #     self.s3 = s3fs.S3FileSystem(anon=True)
        self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock

        self.verbose = verbose
        self.squeeze = squeeze
        self.cache = cache
        self.metaData = {}

        # Open zarr store
        self.zarr_store = zarr_store_type # Only relevant for non-s3 datasets
        store = self.zarr_store_type(self.location) # opens the store based on whether data are on s3 or local
        zgroup = zarr.open(store)
        self.zattrs = zgroup.attrs
        
        if 'omero' in self.zattrs:
            self.omero = zgroup.attrs['omero']
        # assert 'omero' in self.zattrs
        # self.omero = zgroup.attrs['omero']
        assert 'multiscales' in self.zattrs
        self.multiscales = zgroup.attrs['multiscales']
        try:
            self.axes = self.multiscales[0]['axes']
        except:
            self.axes = self.multiscales['axes']
        # self.axes = self.multiscales[0]['axes']
        if len(self.axes) < 3:
            raise Exception()
        self.dim_pos_dic = {
            't':None,
            'c':None,
            'z':None,
            'y':None,
            'x':None
            }
        for index, axe in enumerate(self.axes):
            self.dim_pos_dic[axe['name']] = index
        logger.info(self.dim_pos_dic)
        logger.info(self.multiscales)

        del zgroup
        del store
        
        try:
            self.multiscale_datasets = self.multiscales[0]['datasets']
        except:
            self.multiscale_datasets = self.multiscales['datasets']

        self.ResolutionLevels = len(self.multiscale_datasets)
        
        self.dataset_paths = []
        self.dataset_scales = []
        self.arrays = {}
        for r in range(self.ResolutionLevels):
            self.dataset_paths.append(self.multiscale_datasets[r]['path'])
            self.dataset_scales.append(self.multiscale_datasets[r]['coordinateTransformations'][0]['scale'])
            array = self.open_array(r)
            if r == 0:
                self.TimePoints = array.shape[self.dim_pos_dic['t']] if self.dim_pos_dic['t'] else 1
                self.Channels = array.shape[self.dim_pos_dic['c']] if self.dim_pos_dic['c'] else 1
            # shape_z = array.shape[self.dim_pos_dic['z']]
            # shape_y = array.shape[self.dim_pos_dic['y']]
            # shape_x = array.shape[self.dim_pos_dic['x']]
            # if shape_z <= 64 and shape_y <= 64 and shape_x <=64:
            #     self.ResolutionLevels = r + 1
            #     break 
            
                
            for t,c in itertools.product(range(self.TimePoints),range(self.Channels)):
                
                # Collect attribute info
                self.metaData[r,t,c,'shape'] = array.shape
                ## Need to extract resolution by some other means.  For now, default to 1,1,1 and divide by 2 for each series
                self.metaData[r,t,c,'resolution'] = self.dataset_scales[r][-3:]
                         
                # Collect dataset info
                self.metaData[r,t,c,'chunks'] = array.chunks[-3:]
                dtype = array.dtype[0]
                if dtype == 'int8':
                    dtype = 'uint8'
                elif dtype == 'int16':
                    dtype ='uint16'
                elif dtype == 'float64':
                    dtype ='float32'
                self.metaData[r,t,c,'dtype'] = dtype
                self.metaData[r,t,c,'ndim'] = array.ndim
                
                try:
                    self.metaData[r,t,c,'max'] = self.omero['channels'][c]['window']['end']
                    self.metaData[r,t,c,'min'] = self.omero['channels'][c]['window']['start']
                except:
                    pass
            self.arrays[r] = array
            shape_z = array.shape[self.dim_pos_dic['z']]
            shape_y = array.shape[self.dim_pos_dic['y']]
            shape_x = array.shape[self.dim_pos_dic['x']]
            if shape_z <= 64 and shape_y <= 64 and shape_x <=64:
                self.ResolutionLevels = r + 1
                break 


        self.change_resolution_lock(self.ResolutionLevelLock)
        logger.info(self.metaData)
        
       
            

    def zarr_store_type(self, path):
        if self.s3:
            pass
            # return s3fs.S3Map(path, s3=self.s3)
        else:
            return self.zarr_store(path)
                                                                       

    def change_resolution_lock(self,ResolutionLevelLock):
        self.ResolutionLevelLock = ResolutionLevelLock
        self.shape = self.metaData[self.ResolutionLevelLock,0,0,'shape']
        self.ndim = len(self.shape)
        self.chunks = self.metaData[self.ResolutionLevelLock,0,0,'chunks']
        self.resolution = self.metaData[self.ResolutionLevelLock,0,0,'resolution']
        self.dtype = self.metaData[self.ResolutionLevelLock,0,0,'dtype']
    


    def __getitem__(self,key):
        
        res = 0 if self.ResolutionLevelLock is None else self.ResolutionLevelLock
        logger.info(key)
        if isinstance(key,slice) == False and isinstance(key,int) == False and len(key) == 6:
            res = key[0]
            if res >= self.ResolutionLevels:
                raise ValueError('Layer is larger than the number of ResolutionLevels')
            key = tuple([x for x in key[1::]])
        logger.info(res)
        logger.info(key)
        
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
        
        logger.info(key)
        newKey = []
        for ss in key:
            if ss.start is None and isinstance(ss.stop,int):
                newKey.append(slice(ss.stop,ss.stop+1,ss.step))
            else:
                newKey.append(ss)
                
        key = tuple(newKey)
        logger.info(key)
        
        
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
        logger.info(incomingSlices)
        if self.cache is not None:
            key = f'{self.location}_getSlice_{str(incomingSlices)}'
            # key = self.location + '_getSlice_' + str(incomingSlices)
            result = self.cache.get(key, default=None, retry=True)
            if result is not None:
                logger.info(f'Returned from cache: {incomingSlices}')
                return result
        list_tp =  [0] * self.ndim
        if self.dim_pos_dic.get('t') != None:
            list_tp[self.dim_pos_dic.get('t')] = t
        if self.dim_pos_dic.get('c') != None:
            list_tp[self.dim_pos_dic.get('c')] = c
        if self.dim_pos_dic.get('z') != None:
            list_tp[self.dim_pos_dic.get('z')] = z
        if self.dim_pos_dic.get('y') != None:
            list_tp[self.dim_pos_dic.get('y')] = y
        if self.dim_pos_dic.get('x') != None:
            list_tp[self.dim_pos_dic.get('x')] = x
        tp = tuple(list_tp)
        # logger.success(tp)
        result = self.arrays[r][tp]
        if len(result.shape) < 4:
            result = np.expand_dims(result, axis=0)
        if self.cache is not None:
            self.cache.set(key, result, expire=None, tag=self.location, retry=True)
            # test = True
            # while test:
            #     # logger.info('Caching slice')
            #     self.cache.set(key, result, expire=None, tag=self.location, retry=True)
            #     if result == self.getSlice(*incomingSlices):
            #         test = False

        result = result.astype(self.dtype)
        
        # if str(result.dtype).startswith('float'):
        #     min_value = np.min(result)
        #     max_value = np.max(result)
        #     logger.success(f'min_Value{min_value}, max_value{max_value}')
        #     if max_value == min_value:
        #     # Handle the constant array case
        #         result = np.ones_like(result)  # Or np.ones_like(array) depending on what you prefer
        #     else:
        #     # Apply min-max scaling
        #         result = (result - min_value) / (max_value - min_value)
        # logger.info(result)
        logger.info(result.shape)
        return result
        # return self.open_array(r)[t,c,z,y,x]
    
    
    def locationGenerator(self,res):
        return os.path.join(self.location,self.dataset_paths[res])
    
    def open_array(self,res):
        store = self.zarr_store_type(self.locationGenerator(res))
        logger.info('OPENING ARRAYS')
        store = self.wrap_store_in_chunk_cache(store)
        # if self.cache is not None:
        #     logger.info('OPENING CHUNK CACHE ARRAYS')
        #     from zarr_stores.zarr_disk_cache import Disk_Cache_Store
        #     store = Disk_Cache_Store(store, unique_id=store.path, diskcache_object=self.cache, persist=False)
        # # try:
        # #     if self.cache is not None:
        # #         store = disk_cache_store(store=store, uuid=self.locationGenerator(res), diskcache_object=self.cache, persist=None, meta_data_expire_min=15)
        # # except Exception as e:
        # #     logger.info('Caught Exception')
        # #     logger.info(e)
        # #     pass
        return zarr.open(store)


    def wrap_store_in_chunk_cache(self, store):
        if self.cache is not None:
            logger.info('OPENING CHUNK CACHE ARRAYS')
            logger.info(store.path)
            from zarr_chunk_cache import disk_cache_store as Disk_Cache_Store
            store = Disk_Cache_Store(store, uuid=store.path, diskcache_object=self.cache, persist=True)
        return store



    
    
#uicontrol bool channel0_visable checkbox(default=true);

#uicontrol invlerp channel0_lut (range=[1.6502681970596313,180.84588623046875],window=[0,180.84588623046875]);

#uicontrol vec3 channel0_color color(default="green");

# vec3 channel0 = vec3(0);


# void main() {

# if (channel0_visable == true)
# channel0 = channel0_color *   channel0_lut();

# vec3 rgb = (channel0);

# vec3 render = min(rgb,vec3(1));

# emitRGB(render);
# }



