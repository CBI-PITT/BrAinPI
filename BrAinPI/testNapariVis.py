# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 20:24:42 2021

@author: alpha
"""

import numpy as np
import os
import io
import zlib
import requests
import urllib
import json
import copy
import functools
import urllib3

# bil_api imports
import utils
from remote_array import dataWrapper

# Vis and helper imports
import napari
import dask.array as da

# Benchmarking imports
import cProfile, pstats
from pstats import SortKey

'''
Run: python -i  z:\cbiPythonTools\bil_api\bil_api\testAPIClient.py
'''

# baseURL = 'http://127.0.0.1:5000/api/'
# baseURL = 'http://awatson.duckdns.org:5000/api/'
# baseURL = 'http://136.142.29.160:5000/api/'
# baseURL = 'http://c00.bil.psc.edu:5001/api/'
# baseURL = 'https://brain-api.cbi.pitt.edu/api/'
baseURL = 'https://brain-api.cbi.pitt.edu/array/'

os.environ["NAPARI_ASYNC"] = "1"
# os.environ["NAPARI_OCTREE"] = "1"

selectedDataset = '/CBI_Hive/globus/pitt/bil/t00_c00_0.zarr'
# selectedDataset = '/CBI_Hive/Public/world/BrainA.ims'
selectedDataset = '/CBI_Hive/Public/world/BrainA_25_atlas.ims'

selectedDataset = '/CBI_Hive/Public/world/BrainA_25_atlas_ch1.ims'


# # Find datasets, print, and allow 1 to be selected
# print('Available datasets \n')
# available = utils.get('available-datasets',baseURL)
# utils.prettyPrintDict(available)
# selectedDataset = input('Enter the number of the dataset do you wish to view? \n')

# Take the selected dataset and describe it as a numpy-like array
data = dataWrapper(baseURL,selectedDataset)

# Build a list of the datasets for each resolution level from Highest to Lowest
imagePyramid = []
channel_axis = 1
for ii in range(data.ResolutionLevels):
    imagePyramid.append(data.makeNewArray(ResolutionLock=ii))
    imagePyramid[-1] = da.from_array(imagePyramid[-1],chunks=imagePyramid[-1].chunks,fancy=False)
print(imagePyramid)


# print(imagePyramid)

napari.view_image(imagePyramid,contrast_limits=[0,2000],channel_axis=channel_axis,scale=tuple(data.metadata[(0,0,0,'resolution')]))
# napari.view_image(imagePyramid,contrast_limits=[0,65534],channel_axis=0)


# if __name__ == "__main__":
#     run()
    










































