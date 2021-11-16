# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 20:24:42 2021

@author: alpha
"""

import numpy as np
import os,io, zlib, requests
import urllib, json 

import utils
import napari
import dask.array as da
import functools


baseURL = 'http://127.0.0.1:5000/api/'
# baseURL = 'http://awatson.duckdns.org:5000/api/'
# baseURL = 'http://136.142.29.160:5000/api/'

os.environ["NAPARI_ASYNC"] = "1"
os.environ["NAPARI_OCTREE"] = "1"

class dataWrapper:
    '''
    A wrapper to describe a specific dataset at a specific resolution level as
    a sliceable array-like object.  It is self describing after receving metadata
    from the bil-api.
    
    Slicing calls the api and requests a specific slice at the stated 
    resolution level.
    
    The only optional parameter is 'ResolutionLock' which forces the class to adopt
    the metadata spefigic that resoiltion level and always request data from 
    that specoific resoltution level
    '''
    def __init__(self,selectedDataset,ResolutionLock=0):
        
        print('Loading dataset: {}'.format(selectedDataset))
        self.datasetNum = selectedDataset
        self.metadata = get('metadata?id={}'.format(selectedDataset))
        self.metadata = utils.convertMetaDataDict(self.metadata)
        
        self.ResolutionLevels = int(self.metadata['ResolutionLevels'])
        self.TimePoints = int(self.metadata['TimePoints'])
        self.Channels = int(self.metadata['Channels'])
        
        self.ResolutionLock=ResolutionLock
        
        self.shape = tuple([self.TimePoints,self.Channels] + list(self.metadata[(self.ResolutionLock,0,0,'shape')][-3::]))
        self.chunks = tuple(self.metadata[(self.ResolutionLock,0,0,'chunks')])
        self.dtype = np.dtype(self.metadata[(self.ResolutionLock,0,0,'dtype')])
        self.ndim = len(self.metadata[(self.ResolutionLock,0,0,'shape')])
        
    # @functools.lru_cache(maxsize=128, typed=False)   
    def __getitem__(self,key):
        
        if isinstance(key, int):
            key = [slice(key)]
            for _ in range(self.ndim-1):
                key.append(slice(None))
            key = tuple(key)
            
        if isinstance(key,tuple):
            key = [slice(x) if isinstance(x,int) else x for x in key]
            while len(key) < self.ndim:
                key.append(slice(None))
            key = tuple(key)
        
        print(key)
        
        
        return getArray(datasetNum=self.datasetNum,res=self.ResolutionLock,key=key)
        
    
@functools.lru_cache(maxsize=10000, typed=False)
def getArrayFromAPI(url):
    with urllib.request.urlopen(url) as url:
        return utils.uncompress_np(url.read())

# @functools.cache
# @functools.lru_cache(maxsize=128, typed=False)
def getArray(datasetNum,res,key):
    
    '''
    axes = (t,c,z,y,x)
    '''
    
    location = baseURL + 'fmostCompress?dset={}&res={}&tstart={}&tstop={}&tstep={}&cstart={}&cstop={}&cstep={}&zstart={}&zstop={}&zstep={}&ystart={}&ystop={}&ystep={}&xstart={}&xstop={}&xstep={}'.format(
    datasetNum,
    res,
    key[0].start,key[0].stop,key[0].step,
    key[1].start,key[1].stop,key[1].step,
    key[2].start,key[2].stop,key[2].step,
    key[3].start,key[3].stop,key[3].step,
    key[4].start,key[4].stop,key[4].step
    )


    
    test = getArrayFromAPI(location)
    # with urllib.request.urlopen(location) as url:
    #     test = utils.uncompress_np(url.read())
    
    return test

def get(location):
    with urllib.request.urlopen(baseURL + location, timeout=5) as url:
        data = dict(json.loads(url.read().decode()))
    return data

print('Available datasets \n')
available = get('available-datasets')
print(available)

selectedDataset = input('Enter the number of the dataset do you wish to view? \n')

data = dataWrapper(selectedDataset)

imagePyramid = []
channel_axis = 1
for ii in range(data.ResolutionLevels):
    imagePyramid.append(dataWrapper(selectedDataset=selectedDataset,ResolutionLock=ii))
    imagePyramid[-1] = da.from_array(imagePyramid[-1],chunks=imagePyramid[-1].chunks,fancy=False)
    # if os.environ["NAPARI_ASYNC"] == '1' or os.environ["NAPARI_OCTREE"] == "1":
    #     while imagePyramid[-1].shape[0] == 1:
    #         imagePyramid[-1] = imagePyramid[-1][0]
    # print(imagePyramid[-1].shape)
    # imagePyramid[-1] = imagePyramid[-1]*100
    # imagePyramid[-1] = da.from_array(imagePyramid[-1],chunks=(1,1,1,1000,1000),fancy=False)
print(imagePyramid)

# imagePyramid = imagePyramid[0:-4]




napari.view_image(imagePyramid,contrast_limits=[0,65534],channel_axis=1)
# napari.view_image(imagePyramid,contrast_limits=[0,65534],channel_axis=0)


# if __name__ == "__main__":
#     run()
    










































