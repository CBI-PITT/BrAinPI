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

import s3fs

class ome_zarr_loader:
    def __init__(self, location, ResolutionLevelLock=None, zarr_store_type: StoreLike=NestedDirectoryStore, verbose=None, squeeze=True, cache=None):

        # assert StoreLike is s3fs.S3Map or any([issubclass(zarr_store_type,x) for x in StoreLike.__args__]), 'zarr_store_type is not a zarr storage class'

        self.location = location
        self.s3 = False
        if 's3://' in location:
            self.s3 = s3fs.S3FileSystem(anon=True)
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

    def zarr_store_type(self, path):
        if self.s3:
            return s3fs.S3Map(path, s3=self.s3)
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
    
    
    
    
    
######################################################################
# Attempt boto3-based s3:// store (READ-ONLY)
# Why? s3fs does not play well with async gunicorn workers
# Want to enable reading s3:// ome.zarr
######################################################################

# -*- coding: utf-8 -*-
# """
# Created on Tue Jul 19 10:29:42 2022
#
# @author: awatson
# """
#
# '''
# A Zarr store that uses boto3 (and not s3fs) to access zarr stores in s3://
# '''
#
# import os
# import errno
# import shutil
# import time
# import numpy as np
# import uuid
# import glob
# import re
#
# from zarr.errors import (
#     MetadataError,
#     BadCompressorError,
#     ContainsArrayError,
#     ContainsGroupError,
#     FSPathExistNotDir,
#     ReadOnlyError,
# )
#
# from numcodecs.abc import Codec
# from numcodecs.compat import (
#     ensure_bytes,
#     ensure_text,
#     ensure_contiguous_ndarray,
#     ensure_contiguous_ndarray_like
# )
#
# # from numcodecs.registry import codec_registry
#
# # from threading import Lock, RLock
# # from filelock import Timeout, FileLock, SoftFileLock
#
# from zarr.util import (buffer_size, json_loads, nolock, normalize_chunks,
#                        normalize_dimension_separator,
#                        normalize_dtype, normalize_fill_value, normalize_order,
#                        normalize_shape, normalize_storage_path, retry_call)
#
# from zarr._storage.absstore import ABSStore  # noqa: F401
#
# from zarr._storage.store import Store, array_meta_key
#
# _prog_number = re.compile(r'^\d+$')
#
# ## BOTO3 Way to do dir and files from s3
# import boto3
# from botocore import UNSIGNED, exceptions
# from botocore.client import Config
# import functools
#
# ####################################
# # HELPER FUNCTIONS
# # Duplicated from utils
# # may integrate into store class
# ####################################
#
#
# def s3_get_bucket_and_path_parts(path):
#     path = s3_clean_path(path)
#     path_split = path.split('/')
#     # print(path_split)
#     if isinstance(path_split, str):
#         path_split = [path_split]
#     bucket = path_split[0]
#     return bucket, path_split
# def s3_clean_path(path):
#     if 's3://' in path.lower():
#         path = path[5:]
#     elif path.startswith('/'):
#         path = path[1:]
#     if path.endswith('/'):
#         path = path[:-1]
#     return path
#
#
# def list_all_contents(path):
#     parent, dirs, files = get_dir_contents(path)
#     dirs = [os.path.join(parent,x) for x in dirs]
#     files = [os.path.join(parent, x) for x in files]
#     return dirs + files
#
#     # if 's3://' in path:
#     #     return s3.glob(os.path.join(path,'*'))
#     # else:
#     #     return glob.glob(os.path.join(path,'*'))
#
# def isdir(path):
#     # if 's3://' in path:
#     #     return s3.isdir(path)
#     if 's3://' in path:
#         return s3_isdir(path)
#     else:
#         return os.path.isdir(path)
#
# def isfile(path):
#     if 's3://' in path:
#         return s3_isfile(path)
#     else:
#         return os.path.isfile(path)
#
# def get_dir_contents(path,skip_s3=False):
#     if 's3://' in path:
#         if skip_s3:
#             return path, [], []
#         parent, dirs, files, _, _ = s3_get_dir_contents(path)
#         return f's3://{parent}', dirs, files
#     else:
#         for parent, dirs, files in os.walk(path):
#             return parent, dirs, files
#
# def get_ttl_hash(self,hours=24):
#     """Return the same value withing `hours` time period"""
#     seconds = hours * 60 * 60
#     return round(time.time() / seconds)
#
# class s3_boto_store(Store):
#     '''
#     READ ONLY
#     '''
#
#     def __init__(self, path, normalize_keys=False, dimension_separator='/', s3_cred='anon', mode='r'
#                  ):
#
#         # guard conditions
#         self.raw_path = path
#         self.bucket, path_split = self.s3_get_bucket_and_path_parts(self.raw_path)
#         if len(path_split) > 1:
#             self.zarr_dir = '/'.join(path_split[1:])
#         else:
#             self.zarr_dir = ''
#
#         # if os.path.exists(path) and not os.path.isdir(path):
#         #     raise FSPathExistNotDir(path)
#
#         self.normalize_keys = normalize_keys
#         if dimension_separator is None:
#             dimension_separator = "/"
#         elif dimension_separator != "/":
#             raise ValueError(
#                 "s3_boto_store only supports '/' as dimension_separator")
#         self._dimension_separator = dimension_separator
#         self.mode = mode
#         assert self.mode == 'r', "s3_boto_store only supports read_only mode (mode='r')"
#
#         # Form client
#         assert s3_cred.lower() == 'anon', 'Currently only anonymous connections to s3 are supported'
#         self.client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
#         self.paginator = self.client.get_paginator('list_objects_v2')
#
#     @functools.lru_cache(maxsize=10000)
#     def s3_get_dir_contents(self, path, recursive=False, ttl_hash=get_ttl_hash(hours=24)):
#         bucket, path_split = self.s3_get_bucket_and_path_parts(path)
#         # print(bucket)
#         if len(path_split) > 1:
#             prefix = '/'.join(path_split[1:]) + '/'  # Make sure you provide / in the end
#             root = f'{bucket}/{prefix}'
#         else:
#             prefix = ''  # Root prefix
#             root = f'{bucket}'
#         # client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
#         # paginator = client.get_paginator('list_objects_v2')
#         if recursive:
#             pages = self.paginator.paginate(Bucket=bucket, MaxKeys=1000)
#         else:
#             pages = self.paginator.paginate(Bucket=bucket, MaxKeys=1000, Prefix=prefix, Delimiter='/')
#
#         dirs = ()
#         files = ()
#         files_sizes = ()
#         files_modified = ()
#         for page in pages:
#             if 'CommonPrefixes' in page:
#                 dirs += tuple([x.get('Prefix')[:-1] for x in page.get('CommonPrefixes')])
#             if 'Contents' in page:
#                 files += tuple((x.get('Key') for x in page.get('Contents')))
#                 files_sizes += tuple((x.get('Size') for x in page.get('Contents')))
#                 files_modified += tuple((x.get('LastModified') for x in page.get('Contents')))  # datetime objects
#         r = root.replace(bucket + '/', '')
#         dirs = (x.replace(r,'') for x in dirs)
#         files = (x.replace(r,'') for x in files)
#         return root, tuple(dirs), tuple(files), tuple(files_sizes), tuple(files_modified)
#
#     def s3_get_bucket_and_path_parts(self, path):
#         path = self.s3_clean_path(path)
#         path_split = path.split('/')
#         # print(path_split)
#         if isinstance(path_split, str):
#             path_split = [path_split]
#         bucket = path_split[0]
#         return bucket, path_split
#
#     def s3_clean_path(self, path):
#         if 's3://' in path.lower():
#             path = path[5:]
#         elif path.startswith('/'):
#             path = path[1:]
#         if path.endswith('/'):
#             path = path[:-1]
#         return path
#
#     def s3_path_split(self, path):
#         path = self.s3_clean_path(path)
#         p, f = os.path.split(path)
#         if p == '':
#             return f, p
#         else:
#             return p, f
#
#     def s3_isfile(self, path):
#         # print(path)
#         p, f = self.s3_path_split(path)
#         _, _, files, _, _ = self.s3_get_dir_contents(p)
#         # print(f in files)
#         # print(f'''
#         # ##########################################
#         # ISFILE {f in files}
#         # #########################################
#         # ''')
#         return f in files
#
#     def s3_isdir(self, path):
#         # print(path)
#         p, f = self.s3_path_split(path)
#         if f == '':
#             return True
#         _, dirs, _, _, _ = self.s3_get_dir_contents(p)
#         # print(f in dirs)
#         # print(f'''
#         # ##########################################
#         # ISDIR {f in dirs}
#         # #########################################
#         # ''')
#         return f in dirs
#
#     def get_file_size(self, path):
#         if self.s3_isfile(path):
#             p, f = self.s3_path_split(path)
#             parent, _, files, files_sizes, _ = self.s3_get_dir_contents(p)
#             idx = files.index(f)
#             return files_sizes[idx]
#         else:
#             return 0
#
#     def num_dirs_files(self, path, skip_s3=True):
#         # skip_s3 if passed to get_dir_contents will ignore contents
#         # Getting this information from large remote s3 stores can be very slow on the first try
#         # however after caching, it is much faster.
#         _, dirs, files = self.get_dir_contents(path, skip_s3=skip_s3)
#         return len(dirs), len(files)
#
#     def get_mod_time(self, path):
#         if self.s3_isfile(path):
#             p, f = self.s3_path_split(path)
#             parent, _, files, _, files_modified = self.s3_get_dir_contents(p)
#             idx = files.index(f)
#             return files_modified[idx]
#         else:
#             return datetime.datetime.now()
#
#     def __del__(self):
#         pass
#
#     def __getstate__(self):
#         pass
#         # return (self.path, self.normalize_keys, self._dimension_separator, self.swmr, self.container_ext,
#         #         self._write_direct, self.distribuited, self.distribuited_lock, self._consolidate_depth,
#         #         self.auto_verify_write)
#
#     def __setstate__(self, state):
#         pass
#         # (self.path, self.normalize_keys, self._dimension_separator, self.swmr, self.container_ext,
#         #  self._write_direct, self.distribuited, self.distribuited_lock, self._consolidate_depth,
#         #  self.auto_verify_write) = state
#         #
#         # self.uuid = uuid.uuid1()
#         # self._setup_dist_lock()
#
#     def _normalize_key(self, key):
#         return key.lower() if self.normalize_keys else key
#
#
#     def get_full_path_from_key(self, key):
#         if key[0] == '/':
#             return f'{self.bucket}/{self.zarr_dir}{key}'
#         return f'{self.bucket}/{self.zarr_dir}/{key}'
#
#     def __getitem__(self, key):
#         # print('In Get Item')
#         # key = self._normalize_key(key)
#         filepath = self.get_full_path_from_key(key)
#
#         if self.s3_isfile(filepath):
#             try:
#                 return self._fromfile(filepath)
#             except:
#                 raise KeyError(key)
#
#     def _fromfile(self, filepath):
#         filepath = self.s3_clean_path(filepath)
#         object_name = filepath.replace(self.bucket + '/','')
#         with io.BytesIO() as f:
#             s3.download_fileobj(self.bucket, object_name, f)
#             return f
#
#     def __setitem__(self, key, value):
#         pass
#
#     def __delitem__(self, key):
#         pass
#
#     def __contains__(self, key):
#         filepath = self.get_full_path_from_key(key)
#         if self.s3_isfile(filepath):
#             return True
#         return False
#
#     def __eq__(self, other):
#         return isinstance(other, s3_boto_store) and \
#                 self.bucket == other.bucket and \
#                 self.zarr_dir == other.zarr_dir
#
#     def keys(self):
#         if os.path.exists(self.path):
#             yield from self._keys_fast()
#
#     def _keys_fast(self, walker=os.walk):
#         for dirpath, _, filenames in walker(self.path):
#             dirpath = os.path.relpath(dirpath, self.path)
#             if dirpath == os.curdir:
#                 for f in filenames:
#                     yield f
#             else:
#                 # dirpath = dirpath.replace("\\", "/")
#                 for f in filenames:
#                     basefile, ext = os.path.splitext(f)
#                     if ext == self.container_ext:
#                         names = self._get_zip_keys(os.path.join(self.path, dirpath, f))
#                         # Keys are stored in h5 with '.' separator, replace with appropriate separator
#                         names = (x.replace('.', os.path.sep) for x in tuple(names)[0])
#                         names = (os.path.sep.join((dirpath, basefile, x)) for x in names)
#                         yield from names
#                     # elif ext == '.tmp' and os.path.splitext(basefile)[-1] == self.container_ext:
#                     #     basefile, ext = os.path.splitext(basefile)
#                     #     names = self._get_zip_keys(f)
#                     #     names = ("/".join((dirpath, basefile,x)) for x in names)
#                     #     yield from names
#                     else:
#                         yield os.path.sep.join((dirpath, f))
#
#     def __iter__(self):
#         return self.keys()
#
#     def __len__(self):
#         return sum(1 for _ in self.keys())
#
#     def dir_path(self, path=None):
#         store_path = normalize_storage_path(path)
#         dir_path = self.path
#         if store_path:
#             dir_path = os.path.join(dir_path, store_path)
#         return dir_path
#
#     def listdir(self, path=None):
#         return self._nested_listdir(path) if self._dimension_separator == "/" else \
#             self._flat_listdir(path)
#
#     def _flat_listdir(self, path=None):
#         dir_path = self.dir_path(path)
#         if os.path.isdir(dir_path):
#             return sorted(os.listdir(dir_path))
#         else:
#             return []
#
#     def _nested_listdir(self, path=None):
#         children = self._flat_listdir(path=path)
#         if array_meta_key in children:
#             # special handling of directories containing an array to map nested chunk
#             # keys back to standard chunk keys
#             new_children = []
#             root_path = self.dir_path(path)
#             for entry in children:
#                 entry_path = os.path.join(root_path, entry)
#                 if _prog_number.match(entry) and os.path.isdir(entry_path):
#                     for dir_path, _, file_names in os.walk(entry_path):
#                         for file_name in file_names:
#                             file_path = os.path.join(dir_path, file_name)
#                             rel_path = file_path.split(root_path + os.path.sep)[1]
#                             new_children.append(rel_path.replace(os.path.sep, '.'))
#                 else:
#                     new_children.append(entry)
#             return sorted(new_children)
#         else:
#             return children
#
#     def rename(self, src_path, dst_path):
#         store_src_path = normalize_storage_path(src_path)
#         store_dst_path = normalize_storage_path(dst_path)
#
#         dir_path = self.path
#
#         src_path = os.path.join(dir_path, store_src_path)
#         dst_path = os.path.join(dir_path, store_dst_path)
#
#         os.renames(src_path, dst_path)
#
#     def rmdir(self, path=None):
#         store_path = normalize_storage_path(path)
#         dir_path = self.path
#         if store_path:
#             dir_path = os.path.join(dir_path, store_path)
#         if os.path.isdir(dir_path):
#             shutil.rmtree(dir_path)
#
#     def getsize(self, path=None):
#         store_path = normalize_storage_path(path)
#         fs_path = self.path
#         if store_path:
#             fs_path = os.path.join(fs_path, store_path)
#         if os.path.isfile(fs_path):
#             return os.path.getsize(fs_path)
#         elif os.path.isdir(fs_path):
#             size = 0
#             for child in scandir(fs_path):
#                 if child.is_file():
#                     size += child.stat().st_size
#             return size
#         else:
#             return 0
#
#     def clear(self):
#         shutil.rmtree(self.path)
#
#     def atexit_rmtree(path,
#                       isdir=os.path.isdir,
#                       rmtree=shutil.rmtree):  # pragma: no cover
#         """Ensure directory removal at interpreter exit."""
#         if isdir(path):
#             rmtree(path)
#
#     # noinspection PyShadowingNames
#     def atexit_rmglob(path,
#                       glob=glob.glob,
#                       isdir=os.path.isdir,
#                       isfile=os.path.isfile,
#                       remove=os.remove,
#                       rmtree=shutil.rmtree):  # pragma: no cover
#         """Ensure removal of multiple files at interpreter exit."""
#         for p in glob(path):
#             if isfile(p):
#                 remove(p)
#             elif isdir(p):
#                 rmtree(p)
#
