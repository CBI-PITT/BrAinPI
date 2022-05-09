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
import sys
import math

import imaris_ims_file_reader as ims
import zarr
from bil_api.dataset_info import dataset_info
from bil_api import zarrLoader
from bil_api import zarr_zip_sharded_loader
# from bil_api import config
from numcodecs import Blosc

from diskcache import FanoutCache


def get_file_size(in_bytes):
    '''
    returns a tuple (number, suffix, sortindex) eg (900,GB,2) 
    the table hack will sort by the sort index then the number otherwise
    3 GB will be 'smaller' than 5 kB
    '''
    suffixes = ('B','kB','MB','GB','TB')
    a = 0
    while in_bytes > 1000:
        a += 1 #This will go up the suffixes tuple with each division
        in_bytes = in_bytes / 1000
    return math.ceil(in_bytes), suffixes[a], a   

def num_dirs_files(path):
    for _, dirs, files in os.walk(path):
        return len(dirs), len(files)

def get_config(file='settings.ini',allow_no_value=True):
    import configparser
    # file = os.path.join(os.path.split(os.path.abspath(__file__))[0],file)
    file = os.path.join(sys.path[0], file)
    config = configparser.ConfigParser(allow_no_value=allow_no_value)
    config.read(file)
    return config
    

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

def split_html(req_path):
    html_path = req_path.split('/')
    return tuple((x for x in html_path if x != '' ))


def is_file_type(file_type, path):
    '''
    file_type is file extension starting with '.'
    Examples: '.ims', '.tiff', '.nd2'
    
    if file_type is a list of types return True if even 1 match ['.ims','.tif','.nd2']
    
    return bool
    '''
    
    #orig_path = path
    if isinstance(file_type,str):
        file_type = [file_type]
    if path[-1] == '/':
        path = path[:-1]
    terminal_path_ext = os.path.splitext('a'+ path)[-1]
    
    return any( [ x.lower() == terminal_path_ext.lower() for x in file_type ] ) #+ \
        #[os.path.exists(os.path.join(orig_path,x)) for x in file_type] )
    # return file_type.lower() == os.path.splitext('a'+ path)[-1].lower()

def from_html_to_path(req_path, path_map):
    # print('UTIL line 101: {}'.format(req_path))
    html_path = split_html(req_path)
    # print('UTIL line 103: {}'.format(html_path))
    return os.path.join(
        path_map[html_path[1]], # returns the true FS path
        *html_path[2:]) # returns a unpacked list of all subpaths from html_path[1]

# def from_html_to_path(req_path, path_map):
#     for key in path_map:
#         if key in req_path:
#             request = [x for x in req_path[1:].split(key) if x != '' ]
#             request = [x for x in request if x != '/' ]
#             print(request)
#             break
#     return os.path.join(
#         path_map[key], # returns the true FS path
#         *request) # returns a unpacked list of all subpaths from html_path[1]

def from_path_to_html(path, path_map, req_path, entry_point):
    html_path = split_html(req_path)
    if len(html_path) == 1:
        return path.replace(path_map[html_path[0]],entry_point)
    else:
        return path.replace(path_map[html_path[1]],entry_point + html_path[1])

def get_base_paths(settings_config_parser_object,user_authenticated=False):
    '''
    Returns a list of directories that users are authorized to see
    '''
    ## Grab anon paths from settings file
    paths = []
    for ii in settings_config_parser_object['dir_anon']:
        paths.append(ii)
    
    if not user_authenticated:
        return paths
   
    ## Grab auth paths from settings file
    for ii in settings_config_parser_object['dir_auth']:
        paths.append(ii)
    
    return paths
    
def get_path_map(settings_config_parser_object,user_authenticated=False):
    '''
    Returns a dict where key=path_common_name and value=actual_file_system_path
    '''
    path_map = {}
    ## Collect anon paths
    for ii in settings_config_parser_object['dir_anon']:
        path_map[ii] = settings_config_parser_object['dir_anon'][ii]

    if not user_authenticated:
        return path_map
    
    for ii in settings_config_parser_object['dir_auth']:
        path_map[ii] = settings_config_parser_object['dir_auth'][ii]
    return path_map




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

    
    def loadDataset(self, dataPath:str):
        
        '''
        Given the filesystem path to a file, open that file with the appropriate
        reader and store it in the opendata attribute with the dataPath as 
        the key
        
        If the key exists return
        Always return the name of the dataPath
        '''
        
        print(dataPath)
        
        if dataPath in self.opendata:
            pass
        
        elif os.path.splitext(dataPath)[-1] == '.ims':
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
        
        elif os.path.splitext(dataPath)[1] == '.z_sharded':
            self.opendata[dataPath] = zarr_zip_sharded_loader.zarr_zip_sharded(dataPath,squeeze=False)
            
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


def getFromDataset(dataset,res,t,c,z,y,x):
    '''
    This is designed to be imported from another module and decorated for
    diskcache.  config.opendata must be in the global space
    '''
    return config.opendata[dataset][res,t,c,z,y,x]

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
    
    
    
    