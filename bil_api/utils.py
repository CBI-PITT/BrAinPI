# -*- coding: utf-8 -*-
"""
Created on Thu Nov  4 10:05:34 2021

@author: alpha
"""


import numpy as np
import io
import ast
import os
import urllib
import json

import imaris_ims_file_reader as ims
import zarr
from bil_api.dataset_info import dataset_info
from bil_api import zarrLoader
# from bil_api import config
from numcodecs import Blosc

from diskcache import FanoutCache


def get(location,baseURL):
    with urllib.request.urlopen(baseURL + location, timeout=5) as url:
        data = dict(json.loads(url.read().decode()))
    return data

def compress_np(nparr):
    """
    Receives a numpy array,
    Returns a compressed bytestring, uncompressed and the compressed byte size.
    """
    
    comp = Blosc(cname='zstd', clevel=1, shuffle=Blosc.SHUFFLE)
    bytestream = io.BytesIO()
    np.save(bytestream, nparr)
    uncompressed = bytestream.getvalue()
    compressed = comp.encode(uncompressed)
    return compressed, len(uncompressed), len(compressed)


def uncompress_np(bytestring):
    """
    Receives a compressed bytestring,
    Returns a numpy array.
    """
    
    comp = Blosc(cname='zstd', clevel=1, shuffle=Blosc.SHUFFLE)
    array = comp.decode(bytestring)
    array = io.BytesIO(array)
    
    # sequeeze = False
    # if "NAPARI_ASYNC" in os.environ or "NAPARI_OCTREE" in os.environ:
    #     sequeeze = False
    # if sequeeze == True and (os.environ["NAPARI_ASYNC"] == '1' or os.environ["NAPARI_OCTREE"] == "1"):
    #     return np.squeeze(np.load(array))
    # else:
    return np.load(array)


class config:
    '''
    This class will be used to manage open datasets and persistant cache
    '''
    def __init__(self, 
                 cacheLocation=None, 
                 cacheSizeGB=100, 
                 evictionPolicy='least-recently-used',
                 timeout=0.100, 
                 shards=16
                 ):
        '''
        evictionPolicy Options:
            "least-recently-stored" #R only
            "least-recently-used"  #R/W (maybe a performace hit but probably best cache option)
        '''
        self.opendata = {}
        self.cacheLocation = cacheLocation
        self.cacheSizeGB = cacheSizeGB
        self.evictionPolicy = evictionPolicy
        self.shards = shards
        self.timeout = timeout
        
        self.cacheSizeBytes = self.cacheSizeGB * (1024**3)
        
        if self.cacheLocation is not None:
            # Init cache
            # self.cache = FanoutCache(self.cacheLocation,shards=16)
            self.cache = FanoutCache(self.cacheLocation, shards=self.shards, timeout=self.timeout, size_limit=self.cacheSizeBytes)
            ## Consider removing this and always leaving open to improve performance
            self.cache.close()
        else:
            self.cache = None

    
    def loadDataset(self, selection: int):
    
        dataPath = dataset_info()[selection][1]
        print(dataPath)
        
        if dataPath in self.opendata:
            return dataPath
        
        if os.path.splitext(dataPath)[-1] == '.ims':
            print('Is IMS')
            
            print('Creating ims object')
            self.opendata[dataPath] = ims.ims(dataPath)
            
            if self.opendata[dataPath].hf is None or self.opendata[dataPath].dataset is None:
                print('opening ims object')
                self.opendata[dataPath].open()
        
        
        elif os.path.splitext(dataPath)[-1] == '.zarr':
            print('Is Zarr')
            print('Creating zarrSeries object')
            self.opendata[dataPath] = zarrLoader.zarrSeries(dataPath)
            
        return dataPath
        
    
    
def prettyPrintDict(aDict):
    print('{}{}{}'.format('Number'.ljust(10),'Name'.ljust(20),'File'))
    for k,v in aDict.items():
        print('{}{}{}'.format(k.ljust(10),v[0].ljust(20),v[1]))
    
    
def metaDataExtraction(numpy_like_object,strKey=False):
    '''
    Function take a 5D numpy_like_object that includes the parameters
    'chunks','ResolutionLevels','TimePoints','Channels','metaData'
    
    metaData is a dict with tuple keys of types (int,int,int,str) 
    specifying (resolution_level,TimePoint,Channel,information_type)
    '''
    metadata = {
        'shape':numpy_like_object.shape,
        'chunks':numpy_like_object.chunks,
        'dtype':str(numpy_like_object.dtype),
        'ndim':numpy_like_object.ndim,
        'ResolutionLevels':numpy_like_object.ResolutionLevels,
        'TimePoints':numpy_like_object.TimePoints,
        'Channels':numpy_like_object.Channels
        }
    
    try:
        newMetaDict = {}
        for key in numpy_like_object.metaData:
            if strKey == False:
                newMetaDict[key] = numpy_like_object.metaData[key] \
                    if isinstance(numpy_like_object.metaData[key],np.dtype) == False \
                        else str(numpy_like_object.metaData[key])
            else:
                newMetaDict[str(key)] = numpy_like_object.metaData[key] \
                    if isinstance(numpy_like_object.metaData[key],np.dtype) == False \
                        else str(numpy_like_object.metaData[key])
        print(newMetaDict)
        metadata.update(newMetaDict)
    
    except Exception:
        pass
    
    return metadata
    
#################################
## Depreciated code?  ###########
#################################

def mountDataset(name,storeType):
    
    dataSets = {
        'fmost':(r'H:\globus\pitt\bil\c01_0.zarr','zarrNested'),
        }
    
    if dataSets[name][1] == 'zarrNested':
        store = zarr.NestedDirectoryStore(dataSets[name][0])
        return zarr.open(store, mode='r')

    

def profile(func):
    def wrapper(*args, **kwargs):
        pr = cProfile.Profile()
        pr.enable()
        retval = func(*args, **kwargs)
        pr.disable()
        s = io.StringIO()
        sortby = SortKey.CUMULATIVE  # 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        return retval

    return wrapper
    
    
    
    