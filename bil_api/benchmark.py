# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 19:11:09 2022

@author: alpha
"""
import numpy as np
import os
import io
import zlib
import time
import requests
import urllib
import json
import copy
import functools
import urllib3
import itertools
import random

import dask
from dask.delayed import delayed
from distributed import Client

# bil_api imports
import utils
from bil_api.remote_array import dataWrapper

# Vis and helper imports
import napari
import dask.array as da

# Benchmarking imports
import cProfile, pstats
from pstats import SortKey

baseURL = 'http://c00.bil.psc.edu:5001/api/'

# # Find datasets, print, and allow 1 to be selected
# print('Available datasets \n')
# available = utils.get('available-datasets',baseURL)
# utils.prettyPrintDict(available)
# selectedDataset = input('Enter the number of the dataset do you wish to view? \n')


selectedDataset = 1

# Take the selected dataset and describe it as a numpy-like array
data = dataWrapper(baseURL,selectedDataset)

data_dask = da.from_array(data,chunks=data.chunks,fancy=False)

chunkMap = (
 range(0,data_dask.shape[0],data_dask.chunksize[0]),
 range(0,data_dask.shape[1],data_dask.chunksize[1]),
 range(0,data_dask.shape[2],data_dask.chunksize[2]),
 range(0,data_dask.shape[3],data_dask.chunksize[3]),
 range(0,data_dask.shape[4],data_dask.chunksize[4])
 )

allChunks = [x for x in itertools.product(*chunkMap)]

def getIt(choice):
    a = data[
        slice(choice[0],choice[0]+data_dask.chunksize[0]),
        slice(choice[1],choice[1]+data_dask.chunksize[1]),
        slice(choice[2],choice[2]+data_dask.chunksize[2]),
        slice(choice[3],choice[3]+data_dask.chunksize[3]),
        slice(choice[4],choice[4]+data_dask.chunksize[4])
        ]
    return a

delay = True
random.seed(42)

if delay == True:
    # client = Client()
    pass

toProcess = []
start = time.time()
for ii in range(1000):
    choice = random.choice(allChunks)
    while all([x+data_dask.chunksize[idx]<=data.shape[idx] for idx,x in enumerate(choice)]) == False:
        choice = random.choice(allChunks)
    
    if delay == False:
        a = getIt(choice)
    elif delay == True:
        a = delayed(getIt)(choice)
        # a = client.compute(a)
        toProcess.append(a)
        del a
    print('Working on request {}'.format(ii))

if delay == True:
    print('Waiting for jobs to complete')
    start = time.time()
    toProcess = dask.compute(toProcess)
    # toProcess = client.gather(toProcess)
    # client.close()
stop = time.time()
print('{} minutes to complete {} requests'.format((stop-start)/60,ii))
