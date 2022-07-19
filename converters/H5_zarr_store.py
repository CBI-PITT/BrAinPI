# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 10:29:42 2022

@author: awatson
"""

'''
Attempt at creating a custom HDF5 zarr data store (similar to ZIP store)
but implemented in a directory store-like structure to limit the number of files
per directory
'''

import os
import zarr
import h5py
import numpy as np
import shutil
import errno

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

from zarr._storage.store import (
                                 array_meta_key,
                                 Store
                                 )


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
#         del f['0.0']

class H5Store(Store):
    """Storage class using directories and files on a standard file system.
    Parameters
    ----------
    path : string
        Location of directory to use as the root of the storage hierarchy.
    normalize_keys : bool, optional
        If True, all store keys will be normalized to use lower case characters
        (e.g. 'foo' and 'FOO' will be treated as equivalent). This can be
        useful to avoid potential discrepancies between case-sensitive and
        case-insensitive file system. Default value is False.
    dimension_separator : {'.', '/'}, optional
        Separator placed between the dimensions of a chunk.
    Examples
    --------
    Store a single array::
        >>> import zarr
        >>> store = zarr.DirectoryStore('data/array.zarr')
        >>> z = zarr.zeros((10, 10), chunks=(5, 5), store=store, overwrite=True)
        >>> z[...] = 42
    Each chunk of the array is stored as a separate file on the file system,
    i.e.::
        >>> import os
        >>> sorted(os.listdir('data/array.zarr'))
        ['.zarray', '0.0', '0.1', '1.0', '1.1']
    Store a group::
        >>> store = zarr.DirectoryStore('data/group.zarr')
        >>> root = zarr.group(store=store, overwrite=True)
        >>> foo = root.create_group('foo')
        >>> bar = foo.zeros('bar', shape=(10, 10), chunks=(5, 5))
        >>> bar[...] = 42
    When storing a group, levels in the group hierarchy will correspond to
    directories on the file system, i.e.::
        >>> sorted(os.listdir('data/group.zarr'))
        ['.zgroup', 'foo']
        >>> sorted(os.listdir('data/group.zarr/foo'))
        ['.zgroup', 'bar']
        >>> sorted(os.listdir('data/group.zarr/foo/bar'))
        ['.zarray', '0.0', '0.1', '1.0', '1.1']
    Notes
    -----
    Atomic writes are used, which means that data are first written to a
    temporary file, then moved into place when the write is successfully
    completed. Files are only held open while they are being read or written and are
    closed immediately afterwards, so there is no need to manually close any files.
    Safe to write in multiple threads or processes.
    """

    def __init__(self, path, normalize_keys=True, dimension_separator='.',h5_name='xy_chunks.h5'):

        # guard conditions
        path = os.path.abspath(path)
        if os.path.exists(path) and not os.path.isdir(path):
            raise FSPathExistNotDir(path)

        self.path = path
        self.normalize_keys = normalize_keys
        self._dimension_separator = dimension_separator
        self._h5_name = h5_name
        self.chunk_depth = self._chunk_depth()

    def _normalize_key(self, key):
        return key.lower() if self.normalize_keys else key
    
    # def _dset_from_dirStoreFilePath(self,filepath):
    #     base, dset = os.path.split(filepath)
    #     h5_file = os.path.join(base,self._h5_name)
    #     return h5_file,dset

    @staticmethod
    def _fromfile(file,dset):
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
        with h5py.File(file,'a') as f:
            if dset in f:
                return f[dset][()].tobytes()
            else:
                raise KeyError(dset)

    @staticmethod
    def _tofile(key, data, file):
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
        with h5py.File(file,'a') as f:
            f.create_dataset(key, data=data)
    
    def _chunk_depth(self):
        return 2
        chunks = list(self.chunks)
        while chunks[0] == 1:
            del chunks[0]
        return len(chunks)
    
    def _dset_from_dirStoreFilePath(self,filepath):
        base, key = os.path.split(filepath)
        dset = key.split('.')[-3:]
        dirs = key.split('.')[:-2]
        
        
        # dirs = key.split('.')[:-self.chunk_depth]
        # dset = key.split('.')[-self.chunk_depth:]
        dset = '.'.join(dset)
        
        
        h5_file = os.path.join(base,*dirs,self._h5_name)
        # print(h5_file)
        return h5_file,dset
    
    
    
    def __getitem__(self, key):
        
        print('key : {}'.format(key))
        
        #Special case for .zarray file which should be in file system
        if key == '.zarray':
            fn = os.path.join(self.path,key)
            with open(fn, mode='rb') as f:
                return f.read()
        
        key = self._normalize_key(key)
        filepath = os.path.join(self.path, key)
        h5_file, dset = self._dset_from_dirStoreFilePath(filepath)
        
        return self._fromfile(h5_file,dset)

    def __setitem__(self, key, value):
        
        key = self._normalize_key(key)
        
        print('key : {}'.format(key))
        
        #Special case for .zarray file which should be in file system
        if key == '.zarray':
            if not os.path.exists(self.path):
                os.makedirs(self.path)
            fn = os.path.join(self.path,key)
            with open(fn, mode='wb') as f:
                f.write(value)
            return
        
        # coerce to flat, contiguous array (ideally without copying)
        value = ensure_contiguous_ndarray(value)

        # destination path for key
        file_path = os.path.join(self.path, key)
        print(file_path)
        h5_file, dset = self._dset_from_dirStoreFilePath(file_path)
        print(h5_file)
        print(dset)
        # print(h5_file,dset)

        # ensure there is no directory in the way
        if os.path.isdir(h5_file):
            shutil.rmtree(h5_file)

        # ensure containing directory exists
        dir_path, _ = os.path.split(h5_file)
        if os.path.isfile(dir_path):
            raise KeyError(key)
        os.makedirs(dir_path,exist_ok=True)
        # if not os.path.exists(dir_path):
        #     try:
        #         os.makedirs(dir_path)
        #     except OSError as e:
        #         if e.errno != errno.EEXIST:
        #             raise KeyError(key)

        #Write to h5 file
        try:
            self._tofile(dset, value, h5_file)

            # move temporary file into place;
            # make several attempts at writing the temporary file to get past
            # potential antivirus file locking issues
            # retry_call(os.replace, (temp_path, file_path), exceptions=(PermissionError,))

        finally:
            # clean up if temp file still exists for whatever reason
            # if os.path.exists(temp_path):  # pragma: no cover
            #     os.remove(temp_path)
            pass

    def __delitem__(self, key):
        key = self._normalize_key(key)
        filepath = os.path.join(self.path, key)
        h5_file, dset = self._dset_from_dirStoreFilePath(filepath)
        
        #Delete datasets
        with h5py.File(h5_file,'a') as f:
            del f[dset]

    def __contains__(self, key):
        key = self._normalize_key(key)
        filepath = os.path.join(self.path, key)
        h5_file, dset = self._dset_from_dirStoreFilePath(filepath)
        
        with h5py.File(h5_file,'a') as f:
            return dset in f


    def __eq__(self, other):
        return (
            isinstance(other, H5Store) and
            self.path == other.path
        )

    def keys(self):
        if os.path.exists(self.path):
            yield from self._keys_fast(self.path)


    def _keys_fast(self,path, walker=os.walk):
        '''
        For each h5 file, 
        '''
        for dirpath, _, filenames in walker(path):
            dirpath = os.path.relpath(dirpath, path)
            if dirpath == os.curdir:
                for f in filenames:
                    file_path = os.path.join(path,f)
                    h5_file, dset = self._dset_from_dirStoreFilePath(file_path)
                    with h5py.File(h5_file,'a') as f:
                        dset_list =  list(f.keys())
                    dset_list = [os.path.join(dirpath,x) for x in dset_list]
                    yield from dset_list
            else:
                dirpath = dirpath.replace("\\", "/")
                for f in filenames:
                    file_path = os.path.join(path,f)
                    h5_file, dset = self._dset_from_dirStoreFilePath(file_path)
                    with h5py.File(h5_file,'a') as f:
                        dset_list =  list(f.keys())
                    dset_list = [os.path.join(dirpath,x) for x in dset_list]
                    yield from dset_list

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return sum(1 for _ in self.keys())

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
