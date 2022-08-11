# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 10:29:42 2022

@author: awatson
"""

'''
Attempt at creating a custom HDF5 zarr data store (similar to ZIP store)
but implemented in a directory store-like structure to limit the number of files
per directory

WORKS WITH MULTISCALE_SPATIAL_IMAGE PACKAGE
'''


import os
import zarr
import h5py
import numpy as np
import shutil
import errno
import time

from zarr.errors import (
    MetadataError,
    BadCompressorError,
    ContainsArrayError,
    ContainsGroupError,
    FSPathExistNotDir,
    ReadOnlyError,
)

from numcodecs.abc import Codec
from numcodecs.compat import (
    ensure_bytes,
    ensure_text,
    ensure_contiguous_ndarray
)
# from numcodecs.registry import codec_registry


from zarr.meta import encode_array_metadata, encode_group_metadata
from zarr.util import (buffer_size, json_loads, nolock, normalize_chunks,
                       normalize_dimension_separator,
                       normalize_dtype, normalize_fill_value, normalize_order,
                       normalize_shape, normalize_storage_path, retry_call)

from zarr._storage.absstore import ABSStore  # noqa: F401

from zarr._storage.store import Store, BaseStore


# file = r'Z:\testData\test.h5'

# # Wrap bytes object in np.void()
# data = np.void(np.zeros((1024,1024),dtype=np.dtype('uint16')).tobytes())

# # Save each chunk as a dataset
# with h5py.File(file,'a') as f:
#     f.create_dataset("0.0", data=data)

# with h5py.File(file,'a') as f:
#     f.create_dataset("0.1", data=data)
    
# # Extract Bytes from h5py
# with h5py.File(file,'a') as f:
#     q = f['0.1'][()].tobytes()

# #Delete datasets
# with h5py.File(file,'a') as f:
#     del f["0.0"]
    

# for _ in range(1000):
#     with h5py.File(file,'a') as f:
#         print('Create')
#         f.create_dataset("0.0", data=data)
#         print('Delete')
#         del f['0.0']from numcodecs import Blosc

# from numcodecs import Blosc
# compressor=Blosc(cname='zstd', clevel=8, shuffle=Blosc.BITSHUFFLE)

# store = H5Store(r'Z:\testData\test_h5_store2')
# z = zarr.zeros((1, 2, 11500, 20000, 20000), chunks=(1,1,256,256,256), store=store, overwrite=True, compressor=compressor)
# z[0,0,7,0:2000,0:5000] = 42

class H5Store(Store):
    """
    Storage class the uses HDF5 files to shard chunks accross axis [-3]
    
    Currently, the number of axes in the zarr array must be len(zarr_array) >= 4
    """

    def __init__(self, path, normalize_keys=True, dimension_separator='.',swmr=True,verbose=False):

        # guard conditions
        path = os.path.abspath(path)
        if os.path.exists(path) and not os.path.isdir(path):
            raise FSPathExistNotDir(path)

        self.path = path
        self.normalize_keys = normalize_keys
        self._dimension_separator = dimension_separator
        self.chunk_depth = self._chunk_depth()
        self.swmr=swmr
        self.verbose = verbose
        self._files = ['.zarray','.zgroup','.zattrs','.zmetadata']

    def _normalize_key(self, key):
        return key.lower() if self.normalize_keys else key
    
    # def _dset_from_dirStoreFilePath(self,filepath):
    #     base, dset = os.path.split(filepath)
    #     h5_file = os.path.join(base,self._h5_name)
    #     return h5_file,dset

    def _fromfile(self,file,dset):
        """ Read data from a file
        Parameters
        ----------
        fn : str
            Filepath to open and read from.
        Notes
        -----
        Subclasses should overload this method to specify any custom
        file reading logic.
        """
        # Extract Bytes from h5py
        trys = 0
        while True:
            try:
                with h5py.File(file,'r',libver='latest', swmr=self.swmr) as f:
                    if dset in f:
                        return f[dset][()].tobytes()
                    else:
                        raise KeyError(dset)
                break
            except KeyError:
                raise
            except:
                trys += 1
                if self.verbose == 2:
                    print('READ Failed for key {}, try #{} : Pausing 0.1 sec'.format(dset, trys))
                time.sleep(0.1)
                if trys == 500:
                    raise

    def _tofile(self,key, data, file):
        """ Write data to a file
        Parameters
        ----------
        a : array-like
            Data to write into the file.
        fn : str
            Filepath to open and write to.
        Notes
        -----
        Subclasses should overload this method to specify any custom
        file writing logic.
        """
        trys = 0
        while True:
            try:
                # with h5py.File(file,'a',libver='latest',locking=True) as f:
                with h5py.File(file,'a',libver='latest') as f:
                    f.swmr_mode = self.swmr
                    if key in f:
                        if self.verbose == 2:
                            print('Deleting existing dataset before writing new data : {}'.format(key))
                        del f[key]
                    f.create_dataset(key, data=data)
                break
            except:
                trys += 1
                if self.verbose == 2:
                    print('WRITE Failed for key {}, try #{} : Pausing 0.1 sec'.format(key, trys))
                time.sleep(0.1)
                if trys == 500:
                    raise
                
    
    def _chunk_depth(self):
        return 3
        chunks = list(self.chunks)
        while chunks[0] == 1:
            del chunks[0]
        return len(chunks)
    
    def _dset_from_dirStoreFilePath(self,key):
        '''
        filepath will include self.path + key ('0.1.2.3.4')
        
        '''
        
        dirs, key = os.path.split(key)
        dirs = os.path.split(dirs)
        
        key = self._normalize_key(key)
        # dirs = tuple([self._normalize_key(x) for x in dirs])
        if key in self._files:
            return os.path.join(self.path,*dirs,key), None
        
        
        
        try:
            dirs_2 = key.split('.')[:-3]
            dirs = dirs + tuple(dirs_2)
            fname = key.split('.')[-3]
            
            dset = key.split('.')[-2:]
            
            h5_file = os.path.join(self.path,*dirs,fname + '.h5')
            dset = '.'.join(dset)
            
            # print(h5_file)
            return h5_file, dset
        except:
            return os.path.join(self.path,*dirs,key), None
    
    
    def __getitem__(self, key):
        
        if self.verbose:
            print('GET : {}'.format(key))
        
        # #Special case for .zarray file which should be in file system
        # if key == '.zarray' or key == '.zgroup' or key == '.zattrs':
        #     fn = os.path.join(self.path,key)
        #     with open(fn, mode='rb') as f:
        #         return f.read()
        
        file, dset = self._dset_from_dirStoreFilePath(key)
        # print(file)
        # print(dset)
        
        if dset is None:
            # print('Im HEre1')
            try:
                with open(file, mode='rb') as f:
                    # print('Im HEre2')
                    return f.read()
            except:
                # print('Im HEre3')
                if os.path.exists(os.path.join(os.path.split(file)[0],'.zgroup')) and \
                    os.path.exists(os.path.join(os.path.split(file)[0],'.zattrs')):
                        try:
                            # print('Im HEre4')
                            with open(os.path.join(os.path.split(file)[0],'.zattrs'), mode='rb') as f:
                                return f.read()
                        except:
                            pass
                raise KeyError(key)
        
        
        if os.path.exists(file):
            return self._fromfile(file,dset)
        
        # Must raise KeyError when key does not exist for zarr to load defult 'fill' values
        else:
            raise KeyError(key)
                
        raise KeyError(key)

    def __setitem__(self, key, value):
        
        # key = self._normalize_key(key)
        
        if self.verbose:
            print('SET : {}'.format(key))
            # print('SET VALUE : {}'.format(value))
        
        # #Special case for .zarray file which should be in file system
        # if key == '.zarray' or key == '.zgroup' or key == '.zattrs':
        #     if not os.path.exists(self.path):
        #         os.makedirs(self.path)
        #     fn = os.path.join(self.path,key)
        #     with open(fn, mode='wb') as f:
        #         f.write(value)
        #     return
        
        file, dset = self._dset_from_dirStoreFilePath(key)
        # print(file)
        
        if dset is None:
            basePath = os.path.split(file)[0]
            if not os.path.exists(basePath):
                os.makedirs(basePath)
            with open(file, mode='wb') as f:
                f.write(value)
            return
        
        # coerce to flat, contiguous array (ideally without copying)
        value = ensure_contiguous_ndarray(value)

        try:
            # destination path for key
            # print(h5_file)
            # print(dset)
            # print(h5_file,dset)
    
            # ensure there is no directory in the way
            if os.path.isdir(file):
                shutil.rmtree(file)
    
            # ensure containing directory exists
            dir_path, _ = os.path.split(file)
            if os.path.isfile(dir_path):
                raise KeyError(key)
                
            os.makedirs(dir_path,exist_ok=True)
    
            #Write to h5 file
            try:
                self._tofile(dset, value, file)
            except:
                pass
        except:
            # basePath = os.path.join(self.path,os.path.split(key)[0])
            # if not os.path.exists(basePath):
            #     os.makedirs(basePath)
            # fullPath = os.path.join(self.path,key)
            # with open(fullPath, mode='wb') as f:
            #     f.write(value)
            return

    def __delitem__(self, key):
        
        '''
        Does not yet handle situation where directorystore path is provided
        as the key.
        '''
        
        
        if self.verbose == 2:
            print('__delitem__')
            print('DEL : {}'.format(key))
        
        file, dset = self._dset_from_dirStoreFilePath(key)
        
        if os.path.exists(key):
            os.remove(key)
        elif dset is None:
            if os.path.exists(file):
                os.remove(file)
        # elif '.h5' in file:
        #     with h5py.File(file,'a',libver='latest', swmr=self.swmr) as f:
        #         del f[dset]
        else:
            with h5py.File(file,'a',libver='latest', swmr=self.swmr) as f:
                del f[dset]

    def __contains__(self, key):
        
        if self.verbose == 2:
            print('__contains__')
            print('CON : {}'.format(key))
        
        file, dset = self._dset_from_dirStoreFilePath(key)
        
        # print(file)
        
        if os.path.exists(file):
            # print('HERE')
            return True
        try:
            if os.path.exists(file):
                with h5py.File(file,'r',libver='latest', swmr=self.swmr) as f:
                    return dset in f
        except:
            pass
                
        print('Not Here')
        return False
    
    def __enter__(self):
        return self



    def __eq__(self, other):
        if self.verbose == 2:
            print('eq')
        return (
            isinstance(other, H5Store) and
            self.path == other.path
        )

    def keys(self):
        if self.verbose == 2:
            print('keys')
        if os.path.exists(self.path):
            yield from self._keys_fast(self.path)


    def _keys_fast(self,path, walker=os.walk):
        '''
        This will inspect each h5 file and yield keys in the form of paths.
        
        The paths must be translated into h5_file, key using the function:
            self._dset_from_dirStoreFilePath
        
        Only returns relative paths to store
        '''
        if self.verbose == 2:
            print('_keys_fast')
        for dirpath, _, filenames in walker(path):
            relpath = os.path.relpath(dirpath, path)
            # print(dirpath)
            for f in filenames:
                # print(f)
                if '.h5' in f:
                    h5_file = os.path.join(dirpath,f)
                    # print(h5_file)
                    # h5_file = os.path.join(dirpath,file_path)
                    with h5py.File(h5_file,'r',libver='latest', swmr=self.swmr) as f:
                        dset_list =  list(f.keys())
                    
                else:
                    dset_list = [f]
                
                if relpath == os.curdir:
                    dset_list = [os.path.join(relpath,x) for x in dset_list]
                else:
                    relpath = relpath.replace("\\", "/")
                    dset_list = ["/".join((relpath, x)) for x in dset_list]
                    # dset_list = [os.path.join(dirpath,x) for x in dset_list]
                dset_list = [x[2:] if x[0:2] == './' else x for x in dset_list]
                dset_list = [x[3:] if x[0:3] == '../' else x for x in dset_list]
                yield from dset_list
                    # yield os.path.join(dirpath,f)

    def _keys_num_estimate(self,walker=os.walk):
        '''
        For only the first h5 file, count number of keys and extrapolate to all h5 files
        -a form of cheating to stop every h5 file from being inspected.  
        This can improve performance by many fold for very large datasets.
        
        Estimates could be off if all keys are not written.  Perhaps a method
        to estimate total keys based on shape + chunks would be better.
        '''
        if self.verbose == 2:
            print('_keys_num_estimate')
        idx = True
        for dirpath, _, filenames in walker(self.path):
            # dirpath = os.path.relpath(dirpath, path)
            # print(dirpath)
            for f in filenames:
                # print(f)
                if idx and '.h5' in f:
                    h5_file = os.path.join(dirpath,f)
                    # print(h5_file)
                    # h5_file = os.path.join(dirpath,file_path)
                    with h5py.File(h5_file,'r',libver='latest', swmr=self.swmr) as f:
                        dset_list =  list(f.keys())
                    h5_key_num = len(dset_list)
                    # print('len == {}'.format(h5_key_num))
                    idx = False
                    
                if 'h5_key_num' in locals() and '.h5' in f:
                    yield from range(h5_key_num)
                else:
                    yield 1

    def __iter__(self):
        if self.verbose == 2:
            print('__iter__')
        return self.keys()

    def __len__(self):
        if self.verbose == 2:
            print('__len__')
        return sum((1 for _ in self._keys_num_estimate()))

#     def dir_path(self, path=None):
#         store_path = normalize_storage_path(path)
#         dir_path = self.path
#         if store_path:
#             dir_path = os.path.join(dir_path, store_path)
#         return dir_path

#     def listdir(self, path=None):
#         return self._nested_listdir(path) if self._dimension_separator == "/" else \
#             self._flat_listdir(path)

#     def _flat_listdir(self, path=None):
#         dir_path = self.dir_path(path)
#         if os.path.isdir(dir_path):
#             return sorted(os.listdir(dir_path))
#         else:
#             return []

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

#     def rename(self, src_path, dst_path):
#         store_src_path = normalize_storage_path(src_path)
#         store_dst_path = normalize_storage_path(dst_path)

#         dir_path = self.path

#         src_path = os.path.join(dir_path, store_src_path)
#         dst_path = os.path.join(dir_path, store_dst_path)

#         os.renames(src_path, dst_path)

#     def rmdir(self, path=None):
#         store_path = normalize_storage_path(path)
#         dir_path = self.path
#         if store_path:
#             dir_path = os.path.join(dir_path, store_path)
#         if os.path.isdir(dir_path):
#             shutil.rmtree(dir_path)

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

#     def clear(self):
#         shutil.rmtree(self.path)


# def atexit_rmtree(path,
#                   isdir=os.path.isdir,
#                   rmtree=shutil.rmtree):  # pragma: no cover
#     """Ensure directory removal at interpreter exit."""
#     if isdir(path):
#         rmtree(path)


# # noinspection PyShadowingNames
# def atexit_rmglob(path,
#                   glob=glob.glob,
#                   isdir=os.path.isdir,
#                   isfile=os.path.isfile,
#                   remove=os.remove,
#                   rmtree=shutil.rmtree):  # pragma: no cover
#     """Ensure removal of multiple files at interpreter exit."""
#     for p in glob(path):
#         if isfile(p):
#             remove(p)
#         elif isdir(p):
#             rmtree(p)
