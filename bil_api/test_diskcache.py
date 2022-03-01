# -*- coding: utf-8 -*-
"""
Created on Mon Feb 28 15:44:01 2022

@author: awatson
"""

from diskcache import Cache
from diskcache import FanoutCache
import numpy as np
import time

location = r'Z:\tmpCache\bil_api'

size_limit = 1073741824000
evict_policy = "least-recently-stored" #R only
evict_policy = "least-recently-used"  #R/W

cache = FanoutCache(location,shards=16)
# cache = FanoutCache(location,shards=16,timeout=0.100, size_limit = 1073741824000)
cache.close()


test = np.zeros((100,100,100))

with cache as reference:

    for ii in range(100): 
        print(ii)
        reference.set(str(ii), np.zeros((100,100,100)))


start = time.time()
for ii in range(100):
    
    print(ii)
    with FanoutCache(location) as cache:
        out = cache.get('test_np')

print(time.time()-start)