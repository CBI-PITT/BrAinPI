# -*- coding: utf-8 -*-
"""
Created on Mon Nov 22 19:08:56 2021

@author: alpha
"""

import dask.array as da
import numpy as np

a = np.zeros((100,100,100), dtype=np.uint16)

b = da.from_array(a,chunks=(10,15,20))
b = b[None,None,...]

def test(image, ok, block_info=None):
    print(block_info[None]['array-location'])
    print(ok)
    return image
    

c = da.map_blocks(test, b,'testOK',dtype=np.uint16)
z = c.compute()