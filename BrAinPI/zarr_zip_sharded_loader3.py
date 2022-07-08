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
import json
import math
import numpy as np

location_big = r'H:\\globus\\pitt\\bil\\fmost.z_sharded'
location_small = r'H:\globus\pitt\bil\jp2\zarr.z_sharded'

class zarr_zip_sharded:
    
    def __init__(self, location, ResolutionLevelLock=None, squeeze=True, compute=True):
        
        self.location = location
        
        # Opening JSON file
        with open(os.path.join(self.location,'z_sharded.json')) as f:
            f = f.read()
            self.json = json.loads(f, strict=False)
        
        self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock
        self.compute = compute
        self.squeeze = squeeze
        self.metaData = {}
        
        self.ResolutionLevels = self.json['ResolutionLevels']
        self.TimePoints = self.json['TimePoints']
        self.Channels = self.json['Channels']
        
        self.collect_metadata()
        
        
        # self.dataset = {}
        # for res in range(self.ResolutionLevels):
        #     print('Assembling Resolution Level {}'.format(res))
        #     # self.dataset[res] = build_array_res_level(self.location,res) # Works single threaded
        #     self.dataset[res] = self.build_array_manager(location,res) # Works to build array with only da.stack commands
        #     # self.dataset[res] = self.build_array_manager_par(location,res) # Works to build array with only da.stack commands
        #     # self.dataset[res] = self.build_array(location,res) # Works to build array with only da.stack commands
        #     # self.dataset[res] = delayed(self.build_array_par)(location,res) # Works full parallel assembly
        #     # self.dataset[res] = delayed(self.build_array)(location,res) # Works full parallel assembly
        # self.dataset = dask.compute(self.dataset)[0]
            
        
        # shape = self.dataset[0].shape
        # self.TimePoints = shape[0]
        # self.Channels = shape[1]
        
        # self.collect_metadata()
        # self.change_resolution_lock(self.ResolutionLevelLock)
        
    
    
    
    def collect_metadata(self):
        '''
        Extract and organize metaData from json file
        '''
        for r, t, c in itertools.product(range(self.ResolutionLevels), range(self.TimePoints),
                                         range(self.Channels)):
            
            self.metaData[(r,t,c,'chunks')] = self.json['chunks'][str(r)]
            self.metaData[(r,t,c,'dtype')] = np.dtype(self.json['dtype'])
            self.metaData[(r,t,c,'shape')] = self.json['shape'][str(r)]
            self.metaData[(r,t,c,'ndim')] = 5
            self.metaData[(r,t,c,'size')] = math.prod(self.json['shape'][str(r)])
            ## Need to extract resolution by some other means.  For now, default to 1,1,1 and divide by 2 for each series
            self.metaData[r,t,c,'resolution'] = tuple(self.json['resolution'][str(r)])#(1,1,1)(z,y,x)
            files, z_shape = self.generate_file_names(r,t,c)
            self.metaData[r,t,c,'files'] = files
            self.metaData[r,t,c,'z_shape'] = z_shape
            
    def generate_file_names(self,r,t,c):
        z_layers = self.metaData[(r,t,c,'shape')][2]
        z_chunks = self.metaData[(r,t,c,'chunks')][2]
        
        fileList = []
        z_shape_list = []
        idx = 0
        remaining_z = z_layers
        while idx < z_layers: #Less than number of z-layers
            fileName = os.path.join(
                self.location, 
                str(r),
                str(t),
                str(c),
                str(idx) + '.zip'
                )
            print(fileName)
            fileList.append(fileName)
            
            
            z_shape_list.append(
                z_chunks if z_chunks <= remaining_z else remaining_z
                )
            
            
            idx += z_chunks
            remaining_z -= z_chunks
            
            
        
        return fileList, z_shape_list
    
    
    
    class zip_manager: #Read only
        '''
        Designed to manage reads from individual zip
        zarr stores.  This was necessary to keep da.from_zarr
        from keeping all of the zip files open silmutaniously and causing os errors
        '''
        def __init__(self, z_sharded_class,fileName,r,t,c):
            self.fileName = fileName
            self.dtype = z_sharded_class.metaData[r,t,c,'dtype']
            self.chunks = z_sharded_class.metaData[r,t,c,'chunks'][]
            # self.chunksize = tmp_zarr.chunks
            # self.chunks = tmp_zarr.chunks
            self.shape = z_sharded_class.metaData[r,t,c,'shape']
            self.ndim = z_sharded_class.metaData[r,t,c,'ndim']
            self.size = tmp_zarr.size
            self.nbytes = tmp_zarr.nbytes
            del tmp_zarr
        
        def __getitem__(self,key):
            tmp_zarr = self.open_zip(self.fileName)
            return tmp_zarr[key]
        
        @staticmethod
        def open_zip(fileName):
            # return zarr.open(zarr.ZipStore(fileName))
            return da.from_zarr(zarr.ZipStore(fileName))
    
    # class zip_manager: #Read only
    #     '''
    #     Designed to manage reads from individual zip
    #     zarr stores.  This was necessary to keep da.from_zarr
    #     from keeping all of the zip files open silmutaniously and causing os
    #     '''
    #     def __init__(self, fileName):
    #         print(fileName)
    #         tmp_zarr = self.open_zip(fileName)
    #         self.fileName = fileName
    #         self.dtype = tmp_zarr.dtype
    #         self.chunksize = tmp_zarr.chunksize
    #         # self.chunksize = tmp_zarr.chunks
    #         # self.chunks = tmp_zarr.chunks
    #         self.shape = tmp_zarr.shape
    #         self.ndim = tmp_zarr.ndim
    #         self.size = tmp_zarr.size
    #         self.nbytes = tmp_zarr.nbytes
    #         del tmp_zarr
        
    #     def __getitem__(self,key):
    #         tmp_zarr = self.open_zip(self.fileName)
    #         return tmp_zarr[key]
        
    #     @staticmethod
    #     def open_zip(fileName):
    #         # return zarr.open(zarr.ZipStore(fileName))
    #         return da.from_zarr(zarr.ZipStore(fileName))
    
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
                print(z_shard_list)
                single_color_stack = [delayed(self.make_da_zarr)(file) for file in z_shard_list]
                # single_color_stack = dask.compute(single_color_stack)[0]
                single_color_stack = delayed(da.concatenate)(single_color_stack,axis=0)
                colors.append(single_color_stack)
                
            colors = delayed(da.stack)(colors)
            stack.append(colors)
        stack = delayed(da.stack)(stack)
        
        return stack.compute()
    
    # @staticmethod
    # def open_zip_zarr_store(file):
    #     '''
    #     An attempt to instantiate a zarr.ZipStore and keep it closed
    #     so that too many files are not opened at once.
    #     '''
    #     with zarr.ZipStore(file) as store:
    #         return store
    
    # def make_da_zarr(self,file):
    #     return da.from_zarr(self.open_zip_zarr_store(file),name=file)
    
    @staticmethod
    def make_da_zarr(file):
        return da.from_zarr(zarr.ZipStore(file),name=file)
    
    def build_array(self,location,res):
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
                
                single_color_stack = [da.from_zarr(zarr.ZipStore(file),name=file) for file in z_shard_list]
                single_color_stack = da.concatenate(single_color_stack,axis=0)
                colors.append(single_color_stack)
                
            colors = da.stack(colors)
            stack.append(colors)
        stack = da.stack(stack)
        
        return stack
    
    def build_array_manager(self,location,res):
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
                
                single_color_stack = [self.zip_manager(file) for file in z_shard_list]
                single_color_stack = da.concatenate(single_color_stack,axis=0)
                colors.append(single_color_stack)
                
            colors = da.stack(colors)
            stack.append(colors)
        stack = da.stack(stack)
        
        return stack
    
    def build_array_manager_par(self,location,res):
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
                print(z_shard_list)
                single_color_stack = [delayed(self.zip_manager(file)) for file in z_shard_list]
                # single_color_stack = dask.compute(single_color_stack)[0]
                single_color_stack = delayed(da.concatenate)(single_color_stack,axis=0)
                colors.append(single_color_stack)
                
            colors = delayed(da.stack)(colors)
            stack.append(colors)
        stack = delayed(da.stack)(stack)
        
        return stack.compute()



        