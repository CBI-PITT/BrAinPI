# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 15:05:35 2022

@author: alpha
"""

import utils
import urllib, urllib3
import json
import copy
import ast
import math
import numpy as np


class dataWrapper:
    '''
    A wrapper to describe a specific dataset at a specific resolution level as
    a sliceable array-like object.  It is self describing after receiving metadata
    from the BrAinPI.  
    
    baseURL specifies the bil_api address:port:
        example: http://brainimagelibrary.org/api:5000  <-- only an example
    
    Slicing calls the api and requests a specific slice at the stated 
    resolution level.
    
    The only optional parameter is 'ResolutionLock' which forces the class to adopt
    the metadata specific to that resolution level and forces the class to always 
    request data from that specific resoltution level.
    '''
    def __init__(self,URL,ResolutionLock=0,get_metadata='/metadata/',get_array='/array/'):
        
        print('Loading dataset: {}'.format(URL))
        self.URL = URL if URL[-1] != '/' else URL[-1]
        self.get_metadata = get_metadata
        self.get_array = get_array
        self.metadata = self.get(self.URL.replace(get_array,get_metadata,1))
        self.metadata = self.convertMetaDataDict(self.metadata)
        
        self.ResolutionLevels = int(self.metadata['ResolutionLevels'])
        self.TimePoints = int(self.metadata['TimePoints'])
        self.Channels = int(self.metadata['Channels'])
        
        self.changeResolutionLock(ResolutionLock)
        
    def changeResolutionLock(self, ResolutionLock=0):
        self.ResolutionLock=ResolutionLock
        self.shape = tuple([self.TimePoints,self.Channels] + list(self.metadata[(self.ResolutionLock,0,0,'shape')][-3::]))
        self.chunks = tuple(self.metadata[(self.ResolutionLock,0,0,'chunks')])
        self.dtype = np.dtype(self.metadata[(self.ResolutionLock,0,0,'dtype')])
        self.ndim = len(self.metadata[(self.ResolutionLock,0,0,'shape')])
        
        if self.dtype == np.dtype('uint8'):
            self.nbytes = int(math.prod(self.shape))
        elif self.dtype == np.dtype('uint16'):
            self.nbytes = int(math.prod(self.shape)*16/8)
        elif self.dtype == np.dtype('float32'):
            self.nbytes = int(math.prod(self.shape)*32/8)
        elif self.dtype == np.dtype('float64'):
            self.nbytes = int(math.prod(self.shape)*64/8)
        
        print(self.shape)
        
    def makeNewArray(self, ResolutionLock=None):
        ResolutionLock = self.ResolutionLock if ResolutionLock is None else ResolutionLock
        newArray = copy.deepcopy(self)
        newArray.changeResolutionLock(ResolutionLock)
        print(self.chunks)
        return newArray

        
        
    # @profile
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
        
        
        return self.getArray(res=self.ResolutionLock,key=key)
    
    
    @staticmethod
    # @functools.lru_cache(maxsize=10000, typed=False)
    def getArrayFromAPI(url):
        http = urllib3.PoolManager()
        # r = http.request('GET', url)
        r = http.request('GET', url,headers={'Connection':'close'})
        # print(r.data)
        return utils.uncompress_np(r.data)
    
    
    # @functools.cache
    # @functools.lru_cache(maxsize=128, typed=False)
    def getArray(self,res,key):
        
        '''
        axes = (t,c,z,y,x)
        '''
        
        location = '{}?res={}&tstart={}&tstop={}&tstep={}&cstart={}&cstop={}&cstep={}&zstart={}&zstop={}&zstep={}&ystart={}&ystop={}&ystep={}&xstart={}&xstop={}&xstep={}'.format(
        self.URL,
        res,
        key[0].start,key[0].stop,key[0].step,
        key[1].start,key[1].stop,key[1].step,
        key[2].start,key[2].stop,key[2].step,
        key[3].start,key[3].stop,key[3].step,
        key[4].start,key[4].stop,key[4].step
        )
        print(location)
        # location = f'{baseURL}fmostCompress?dset={datasetNum}&res={res}&tstart={key[0].start}&tstop={key[0].stop}&tstep={key[0].step}&cstart={key[1].start}&cstop={key[1].stop}&cstep={key[1].step}&zstart={key[2].start}&zstop={key[2].stop}&zstep={key[2].step}&ystart={key[3].start}&ystop={key[3].stop}&ystep={key[3].step}&xstart={key[4].start}&xstop={key[4].stop}&xstep={key[4].step}'
        array = self.getArrayFromAPI(location)
        # print('Shape of returned array = {}'.format(array.shape))
        # print(array)
        return array
    

    def get(self,location):
        with urllib.request.urlopen(location, timeout=5) as url:
            data = dict(json.loads(url.read().decode()))
        return data
    
    def convertMetaDataDict(self,meta):
        
        '''
        json serialized dict can not have tuple as key.
        This assumes that any key value that 'literal_eval's to tuple 
        will be converted.  Tuples are used to designate (r,t,c) information.
        
        Example: a key for the shape of an array at resolution level 2, 
        timepoint 3, channel 4 = (2,3,4,'shape')
        '''
        
        newMeta = {}
        for idx in meta:
            
            try:
                if isinstance(ast.literal_eval(idx),tuple):
                    newMeta[ast.literal_eval(idx)] = meta[idx]
                else:
                    newMeta[idx] = meta[idx]
            
            except ValueError:
                newMeta[idx] = meta[idx]
        
        return newMeta










