

from diskcache import FanoutCache
import os
import config_tools

settings = config_tools.get_config()
def get_cache():
    ## Setup cache location based on OS type
    ## Optional situations like machine name can be used to customize
    if os.name == 'nt':
        cacheLocation = settings.get('disk_cache', 'location_win')
    else:
        cacheLocation = settings.get('disk_cache', 'location_unix')
        # cacheLocation = None

    if cacheLocation is not None:
        # Instantiate class that will manage all open datasets
        # This will remain in the global env and be accessed by multiple route methods
        cacheLocation=cacheLocation
        cacheSizeGB=settings.getint('disk_cache', 'cacheSizeGB')
        cacheSizeBytes = cacheSizeGB * (1024 ** 3)
        evictionPolicy=settings.get('disk_cache', 'evictionPolicy')
        shards=settings.getint('disk_cache', 'shards')
        timeout=settings.getfloat('disk_cache', 'timeout')

        return FanoutCache(cacheLocation, shards=shards, timeout=timeout,
                                     size_limit=cacheSizeBytes, eviction_policy=evictionPolicy)

import sys
from psutil import virtual_memory
from collections import OrderedDict
import uuid
class cache_head_space:

    def __init__(self, free_ram_gb=20):
        self.head_space = free_ram_gb * (1024**3)
        self.cache = OrderedDict()
        self.update_available_space()
        self.uuid = str(uuid)

    def update_available_space(self):
        self.available_space = virtual_memory().available
        print(f'Available GB: {self.available_space / 1024**3}')

    @staticmethod
    def get_size_object(object):
        return sys.getsizeof(object)

    def __getitem__(self, key):
        result = self.cache.get(key)
        if result is not None:
            self.cache.move_to_end(key)
            print('Got from RAM CACHE')
        return result

    def __setitem__(self, key, value):
        while True:
            lock_value = self.cache.get('lock')
            if lock_value is None:
                self.cache['lock'] = self.uuid
            else:
                continue

            if self.cache.get('lock') == self.uuid:
                break

        try:
            new_obj_size = self.get_size_object(value) + self.get_size_object(key)
            space = self.trim_cache(extra_space=new_obj_size)
            if space:
                self.cache[key] = value
                print('SET TO RAM CACHE')
        finally:
            if self.cache.get('lock') == self.uuid:
                self.cache.pop('lock')

    def trim_cache(self, extra_space=0):
        '''
        Trim cache to fit head_space
        extra_space will trim further to allow for object insertion

        Return True if trim was successful, False if cache is empty and space is not available
        '''
        self.update_available_space()
        while self.available_space < self.head_space + extra_space:
            try:
                self.cache.popitem(last=False)
            except KeyError: # If dict is empty thus cache can not be reduced
                return False
            self.update_available_space()
        return True

    def __del__(self):
        for key in self.cache.keys():
            self.cache[key] = None

def test(head_space=20):
    import numpy as np
    a = cache_head_space(head_space)
    for ii in range(100000):
        a[ii] = np.ones((100,100,100))
