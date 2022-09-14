# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 10:29:42 2022

@author: awatson
"""

'''
A Zarr store that uses HDF5 as a containiner to shard chunks accross a single
axis.  The store is implemented similar to a directory store 
but on axis[-3] HDF5 files are written which contain
chunks cooresponding to the remainining axes.  If the shape of the 
the array are less than 3 axdes, the shards will be accross axis0

Example:
    array.shape = (1,1,200,10000,10000)
    /root/of/array/.zarray
    #Sharded h5 container at axis[-3]
    /root/of/array/0/0/4.hf
    
    4.hf contents:
        key:value
        0.0:bit-string
        0.1:bit-string
        4.6:bit-string
        ...
        ...
'''


import os
import sys
import h5py
import shutil
import time
import psutil
import numpy as np

from collections import OrderedDict
from collections.abc import MutableMapping
from threading import Lock, RLock
from typing import Optional, Union, List, Tuple, Dict, Any

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


from zarr.util import (buffer_size, json_loads, nolock, normalize_chunks,
                       normalize_dimension_separator,
                       normalize_dtype, normalize_fill_value, normalize_order,
                       normalize_shape, normalize_storage_path, retry_call)

from zarr._storage.absstore import ABSStore  # noqa: F401

from zarr._storage.store import Store, BaseStore
from zarr.storage import listdir, getsize

Path = Union[str, bytes, None]
StoreLike = Union[BaseStore, MutableMapping]

class disk_cache_store(Store):
    """Storage class that implements a DiskCache layer over
    some other store. Intended primarily for use with stores that may be on slow
    spinning storage to be cached to fast flash storage.
    Parameters
    ----------
    store : Store
        The store containing the actual data to be cached.
    max_size : int
        The maximum size that the cache may grow to, in number of bytes. Provide `None`
        if you would like the cache to have unlimited size.
    Examples
    --------
    The example below wraps an S3 store with an LRU cache::
        >>> import s3fs
        >>> import zarr
        >>> s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name='eu-west-2'))
        >>> store = s3fs.S3Map(root='zarr-demo/store', s3=s3, check=False)
        >>> cache = zarr.LRUStoreCache(store, max_size=2**28)
        >>> root = zarr.group(store=cache)  # doctest: +REMOTE_DATA
        >>> z = root['foo/bar/baz']  # doctest: +REMOTE_DATA
        >>> from timeit import timeit
        >>> # first data access is relatively slow, retrieved from store
        ... timeit('print(z[:].tobytes())', number=1, globals=globals())  # doctest: +SKIP
        b'Hello from the cloud!'
        0.1081731989979744
        >>> # second data access is faster, uses cache
        ... timeit('print(z[:].tobytes())', number=1, globals=globals())  # doctest: +SKIP
        b'Hello from the cloud!'
        0.0009490990014455747
    """

    def __init__(self, store: StoreLike, uuid: str=None, diskcache_object=None, persist=None, meta_data_expire_min=15):
        '''
        Designating a uuid will allow the cache to persist accross instances
        '''
        self._store: BaseStore = BaseStore._ensure_store(store)
        self._diskcache_object = diskcache_object
        # self._current_size = 0
        self._meta_data_expire = meta_data_expire_min * 60
        self._mutex = Lock()
        self._metadata_keys = ['.zarray','.zgroup','.zattrs','.zmetadata']
        if uuid is None:
            self.persist = False
            import uuid
            self.uuid = str(uuid.uuid4())
        else:
            self.persist = True if persist is None else persist
            self.uuid = uuid
        
        self._cache_key_prefix = self.uuid + '_'
        
        print(self._store)
        print(self._diskcache_object)
        print(self._meta_data_expire)
        print(self._metadata_keys)
        print(self.uuid)
        print(self._cache_key_prefix)
        

    # def __getstate__(self):
    #     return (self._store, self._max_size, self._current_size, self._keys_cache,
    #             self._contains_cache, self._listdir_cache, self._values_cache, self.hits,
    #             self.misses)

    # def __setstate__(self, state):
    #     (self._store, self._max_size, self._current_size, self._keys_cache,
    #      self._contains_cache, self._listdir_cache, self._values_cache, self.hits,
    #      self.misses) = state
    #     self._mutex = Lock()

    def __len__(self):
        print('__len__')
        return len(self._keys())

    def __iter__(self):
        print('__iter__')
        return self.keys()

    def __contains__(self, key):
        print('__contains__')
        print(key)
        cache_key = self.cache_key('CONTAINS_'+ key)
        if cache_key not in self._diskcache_object:
            self._diskcache_object.set(cache_key, self._store.__contains__(key), expire=self._meta_data_expire, tag=self.uuid + '_CONTAINS')
        return self._diskcache_object.get(cache_key)

    # def clear(self):
    #     self._store.clear()
    #     self.invalidate()

    def keys(self):
        print('keys')
        with self._mutex:
            return iter(self._keys())

    def _keys(self):
        print('_keys')
        cache_key = self.cache_key('KEYS')
        if cache_key not in self._diskcache_object:
            print('Setting Keys from Cache')
            self._diskcache_object.set(cache_key, list(self._store.keys()),tag=self.uuid)
        return self._diskcache_object.get(cache_key)

    # def listdir(self, path: Path = None):
    #     with self._mutex:
    #         try:
    #             return self._listdir_cache[path]
    #         except KeyError:
    #             listing = listdir(self._store, path)
    #             self._listdir_cache[path] = listing
    #             return listing

    def cache_key(self,key):
        print('cache_key')
        return self._cache_key_prefix+key

    def __getitem__(self, key):
        print('__getitem__')
        cache_key = self.cache_key(key)
        try:
            # first try to obtain the value from the cache
            value = self._diskcache_object[cache_key]
            print('GOT FROM CACHE: {}'.format(cache_key))
            # treat the end as most recently used
            if key in self._metadata_keys:
                self._diskcache_object.touch(cache_key, expire=self._meta_data_expire)

        except KeyError:
            # cache miss, retrieve value from the store
            value = self._store[key]
            if key in self._metadata_keys:
                # Metadata expires after awhile
                self._diskcache_object.add(cache_key, value, tag=self.uuid)
                self._diskcache_object.touch(cache_key, expire=self._meta_data_expire)
            else:
                # Chunks do not expire
                self._diskcache_object.add(cache_key, value, tag=self.uuid)
            print('SET TO CACHE: {}'.format(cache_key))

        return value

    
    def _invalidate(self):
        print('_invalidate')
        self._invalidate_keys()
        self._invalidate_listdir()
        self._invalidate_contains()
        
    def _invalidate_keys(self):
        cache_key = self.cache_key('KEYS')
        if cache_key in self._diskcache_object:
            del self._diskcache_object[cache_key]
    
    def _invalidate_listdir(self):
        cache_key = self.cache_key('LISTDIR')
        if cache_key in self._diskcache_object:
            del self._diskcache_object[cache_key]
    
    def _invalidate_contains(self):
        self._diskcache_object.evict(self.uuid + '_CONTAINS')
        
        
    def __setitem__(self, key, value):
        print('__setitem__')
        print(key)
        cache_key = self.cache_key(key)
        self._store[key] = value
        if key in self._metadata_keys:
            self._diskcache_object.set(cache_key, value, tag=self.uuid, expire=self._meta_data_expire)
        else:
            self._diskcache_object.set(cache_key, value, tag=self.uuid)
        
        self._invalidate()

    def __delitem__(self, key):
        del self._store[key]
        del self._diskcache_object[self.cache_key(key)]
        self._invalidate()
    
    def __del__(self):
        if self.persist == False:
            print('Cleaning up cache for uuid: {}'.format(self.uuid))
            self._invalidate()
            self._diskcache_object.evict(self.uuid)
    
    
    
    
class LRUStoreCache_HeadSpace(Store):
    """BASED ON ZARR LRUStoreCache
    Storage class that implements a least-recently-used (LRU) cache layer over
    some other store. Intended primarily for use with stores that can be slow to
    access, e.g., remote stores that require network communication to store and
    retrieve data.
    Parameters
    ----------
    store : Store
        The store containing the actual data to be cached.
    head_space_GB : int
        The number of GB that will remain free in RAM. The cache will stop growing
        until when this amount of free space remains in RAM.
    Examples
    --------
    The example below wraps an S3 store with an LRU cache::
        >>> import s3fs
        >>> import zarr
        >>> s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name='eu-west-2'))
        >>> store = s3fs.S3Map(root='zarr-demo/store', s3=s3, check=False)
        >>> cache = zarr.LRUStoreCache(store, max_size=2**28)
        >>> root = zarr.group(store=cache)  # doctest: +REMOTE_DATA
        >>> z = root['foo/bar/baz']  # doctest: +REMOTE_DATA
        >>> from timeit import timeit
        >>> # first data access is relatively slow, retrieved from store
        ... timeit('print(z[:].tobytes())', number=1, globals=globals())  # doctest: +SKIP
        b'Hello from the cloud!'
        0.1081731989979744
        >>> # second data access is faster, uses cache
        ... timeit('print(z[:].tobytes())', number=1, globals=globals())  # doctest: +SKIP
        b'Hello from the cloud!'
        0.0009490990014455747
    """

    def __init__(self, store: StoreLike, head_space_GB: int = 8):
        self._store: BaseStore = BaseStore._ensure_store(store)
        self._head_space_GB = head_space_GB
        # self._current_size = 0
        self._keys_cache = None
        self._contains_cache = None
        self._listdir_cache: Dict[Path, Any] = dict()
        self._values_cache: Dict[Path, Any] = OrderedDict()
        self._mutex = Lock()
        self.hits = self.misses = 0

    def __getstate__(self):
        return (self._store, self._max_size, self._keys_cache,
                self._contains_cache, self._listdir_cache, self._values_cache, self.hits,
                self.misses)

    def __setstate__(self, state):
        (self._store, self._max_size, self._keys_cache,
         self._contains_cache, self._listdir_cache, self._values_cache, self.hits,
         self.misses) = state
        self._mutex = Lock()

    def __len__(self):
        return len(self._keys())

    def __iter__(self):
        return self.keys()

    def __contains__(self, key):
        with self._mutex:
            if self._contains_cache is None:
                self._contains_cache = set(self._keys())
            return key in self._contains_cache

    def clear(self):
        self._store.clear()
        self.invalidate()

    def keys(self):
        with self._mutex:
            return iter(self._keys())

    def _keys(self):
        if self._keys_cache is None:
            self._keys_cache = list(self._store.keys())
        return self._keys_cache

    def listdir(self, path: Path = None):
        with self._mutex:
            try:
                return self._listdir_cache[path]
            except KeyError:
                listing = listdir(self._store, path)
                self._listdir_cache[path] = listing
                return listing

    def getsize(self, path=None) -> int:
        return getsize(self._store, path=path)

    
    def _remaining_space_GB(self):
        return psutil.virtual_memory().free/1024**3
    
    def _pop_value(self):
        # remove the first value from the cache, as this will be the least recently
        # used value
        print(len(self._values_cache))
        _, v = self._values_cache.popitem(last=False)
        return v

    def _remove_value(self):
        print(len(self._values_cache))
        _, v = self._values_cache.popitem(last=False)
        return v
    
    def _accommodate_value(self, value_size):
        print('_accommodate_value')
        # ensure there is enough space in the cache for a new value
        while self._remaining_space_GB() - (value_size/1024**3) < self._head_space_GB:
            self._remove_value()
            # self._pop_value()
            print('Pop Value')
            

    def _cache_value(self, key: Path, value):
        # cache a value
        value_size = buffer_size(value)
        # check size of the value against max size, as if the value itself exceeds max
        # size then we are never going to cache it
        # if self._max_size is None or value_size <= self._max_size:
        #     self._accommodate_value(value_size)
        #     self._values_cache[key] = value
        #     # self._current_size += value_size
        self._accommodate_value(value_size)
        self._values_cache[key] = value

    def invalidate(self):
        """Completely clear the cache."""
        with self._mutex:
            self._values_cache.clear()
            self._invalidate_keys()
            # self._current_size = 0

    def invalidate_values(self):
        """Clear the values cache."""
        with self._mutex:
            self._values_cache.clear()

    def invalidate_keys(self):
        """Clear the keys cache."""
        with self._mutex:
            self._invalidate_keys()

    def _invalidate_keys(self):
        self._keys_cache = None
        self._contains_cache = None
        self._listdir_cache.clear()

    def _invalidate_value(self, key):
        if key in self._values_cache:
            del self._values_cache[key]
            # value = self._values_cache.pop(key)
            # self._current_size -= buffer_size(value)

    def __getitem__(self, key):
        try:
            # first try to obtain the value from the cache
            with self._mutex:
                value = self._values_cache[key]
                # cache hit if no KeyError is raised
                self.hits += 1
                # treat the end as most recently used
                self._values_cache.move_to_end(key)

        except KeyError:
            # cache miss, retrieve value from the store
            value = self._store[key]
            with self._mutex:
                self.misses += 1
                # need to check if key is not in the cache, as it may have been cached
                # while we were retrieving the value from the store
                if key not in self._values_cache:
                    self._cache_value(key, value)

        return value

    def __setitem__(self, key, value):
        self._store[key] = value
        with self._mutex:
            self._invalidate_keys()
            self._invalidate_value(key)
            self._cache_value(key, value)

    def __delitem__(self, key):
        del self._store[key]
        with self._mutex:
            self._invalidate_keys()
            self._invalidate_value(key)
    

