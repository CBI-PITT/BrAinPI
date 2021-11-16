# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 11:44:46 2021

@author: alpha
"""

import numpy as np
import io, zlib, requests
import urllib.request, json 

import utils

# import urllib.request, json 
# with urllib.request.urlopen('http://127.0.0.1:5000/api/fmost?zstart=0&zstop=1&ystart=0&ystop=1&xstart=0&xstop=1') as url:
#     data = np.array(json.loads(url.read().decode()))



## Request a specific numpy chunk
zstart = 1
zstop = 2
ystart = 7000
ystop = 7200
xstart = 5000
xstop = 5200

location = 'http://127.0.0.1:5000/api/fmostCompress?zstart={}&zstop={}&ystart={}&ystop={}&xstart={}&xstop={}'.format(zstart,zstop,ystart,ystop,xstart,xstop)

with urllib.request.urlopen(location) as url:
    test = utils.uncompress_np(url.read())



##retrieve metadata in a dict form
location = 'http://127.0.0.1:5000/api/fmost/meta'


with urllib.request.urlopen(location) as url:
    data = dict(json.loads(url.read().decode()))