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
import gzip
from skimage import img_as_float32, img_as_float64, img_as_uint, img_as_ubyte

import imaris_ims_file_reader as ims
import zarr
# from bil_api.dataset_info import dataset_info
import zarrLoader
import zarr_zip_sharded_loader4 as zarr_zip_sharded_loader
from ome_zarr_loader import ome_zarr_loader

from flask import (
    render_template,
    request,
    send_file,
    redirect,
    jsonify,
    url_for
    )


# from BrAinPI import config
# from numcodecs import Blosc
import blosc

from diskcache import FanoutCache


def get_file_size(in_bytes):
    '''
    returns a tuple (number, suffix, sortindex) eg (900,GB,2) 
    the table hack will sort by the sort index then the number otherwise
    3 GB will be 'smaller' than 5 kB
    '''
    suffixes = ('B','KB','MB','GB','TB','PB')
    a = 0
    while in_bytes > 1024:
        a += 1 #This will go up the suffixes tuple with each division
        in_bytes = in_bytes / 1024
    return round(in_bytes,2), suffixes[a], a   

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

def conv_np_dtypes(array,tdtype):
    if array.dtype == tdtype:
        return array
    if tdtype == 'uint8' or tdtype == np.dtype('uint8'):
        return img_as_ubyte(array)
    if tdtype == 'uint16' or tdtype == np.dtype('uint16'):
        return img_as_uint(array)
    if tdtype == 'float32' or tdtype == np.dtype('float32'):
        return img_as_float32(array)
    if tdtype == float or tdtype == 'float64' or tdtype == np.dtype('float64'):
        return img_as_float64(array)

def compress_np(nparr):
    """
    Receives a numpy array,
    Returns a compressed bytestring, uncompressed and the compressed byte size.
    """
    
    # comp = Blosc(cname='zstd', clevel=5, shuffle=Blosc.BITSHUFFLE,typesize=8)
    bytestream = io.BytesIO()
    np.save(bytestream, nparr)
    uncompressed = bytestream.getvalue()
    # compressed = comp.encode(uncompressed)
    compressed = blosc.compress(uncompressed, typesize=6, clevel=1,cname='zstd', shuffle=blosc.BITSHUFFLE)
    return compressed, len(uncompressed), len(compressed)


def uncompress_np(bytestring):
    """
    Receives a compressed bytestring,
    Returns a numpy array.
    """
    
    # comp = Blosc(cname='zstd', clevel=5, shuffle=Blosc.BITSHUFFLE,typesize=8)
    # array = comp.decode(bytestring)
    array = blosc.decompress(bytestring)
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

def dict_key_value_match(a_dict,key_or_value,specific=True):
    '''
    Searches both key and values in dict and return the cooresponding value
    Key --> value
    value --> key
    '''

    if key_or_value in a_dict:
        return a_dict[key_or_value]
    for key,value in a_dict.items():
        if key_or_value.lower() == value.lower():
            return key
        if value.lower() == key_or_value.lower():
            return value

    if not specific:
        'Behavior can be hard to predict'
        for key,value in a_dict.items():
            if key_or_value.lower() in value.lower():
                return key
            if value.lower() in key_or_value.lower():
                return value


def strip_leading_trailing_slash(string):
    assert isinstance(string,str), 'Must pass a string'
    if string[-1] == '/':
        string = string[:-1]
    if string[0] == '/':
        string = string[1:]
    return string


import difflib
def from_path_to_browser_html(path, path_map, html_base):
    '''
    Take a file system path and return a html browser location
    '''
    matches = {}
    for key, value in path_map.items():
        if value in path:
            matches[value] = key
    if len(matches) == 0:
        return
    print(matches)
    match = list(difflib.get_close_matches(path,matches,cutoff=0.01))
    print(match)
    best_match = match if len(match) == 0 else match[0]
    print(best_match)
    end = path.replace(best_match,matches[best_match])
    print(end)
    end = strip_leading_trailing_slash(end)

    main = f'{url_for("browse_fs")}/{end}'.replace('//','/')
    main = strip_leading_trailing_slash(main)

    html_base = strip_leading_trailing_slash(html_base)


    html_path = f'{html_base}/{main}'
    print(html_path)
    return html_path

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

def get_html_split_and_associated_file_path(config,request):
    settings = config.settings
    path_map = get_path_map(settings,user_authenticated=True) #<-- Force user_auth=True to get all possible paths, in this way all ng links will be shareable to anyone
    datapath = from_html_to_path(request.path, path_map)
    
    path_split = split_html(request.path)
    return path_split, datapath


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
            # self.cache.close()
        else:
            self.cache = None

        def __del__(self):
            if self.cache is not None:
                self.cache.close()


    
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
            return dataPath
        
        elif os.path.splitext(dataPath)[-1] == '.ims':
            print('Is IMS')
            
            print('Creating ims object')
            self.opendata[dataPath] = ims.ims(dataPath,squeeze_output=False)
            
            if self.opendata[dataPath].hf is None or self.opendata[dataPath].dataset is None:
                print('opening ims object')
                self.opendata[dataPath].open()
        
        
        elif dataPath.endswith('.ome.zarr'):
            self.opendata[dataPath] = ome_zarr_loader(dataPath, squeeze=False, zarr_store_type='oz', cache=self.cache)

        elif '.omezarr' in os.path.split(dataPath)[-1]:
            self.opendata[dataPath] = ome_zarr_loader(dataPath,squeeze=False,zarr_store_type='hss',cache=self.cache)
        
        elif '.omezans' in os.path.split(dataPath)[-1]:
            self.opendata[dataPath] = ome_zarr_loader(dataPath,squeeze=False,zarr_store_type='ans',cache=self.cache)

        elif '.omehans' in os.path.split(dataPath)[-1]:
            self.opendata[dataPath] = ome_zarr_loader(dataPath,squeeze=False,zarr_store_type='hns',cache=self.cache)
            
        elif os.path.splitext(dataPath)[-1] == '.zarr':
            print('Is Zarr')
            print('Creating zarrSeries object')
            self.opendata[dataPath] = zarrLoader.zarrSeries(dataPath)
        
        elif os.path.splitext(dataPath)[1] == '.z_sharded':
            self.opendata[dataPath] = zarr_zip_sharded_loader.zarr_zip_sharded(dataPath,squeeze=False)
            
        ## Append extracted metadata as attribute to open dataset
        try:
            self.opendata[dataPath].metadata = metaDataExtraction(self.opendata[dataPath])
        except Exception:
            pass
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
        # print(newMetaDict)
        metadata.update(newMetaDict)
    
    except Exception:
        pass
    
    return metadata

def fix_special_characters_in_html(html_string):
    # Replace space with %20 (' ')
    tmp_string = html_string.replace(' ', '%20')
    return tmp_string

def strip_trailing_new_line(string):
    while string[-1] == '\n':
        string = string[:-1]
    return string

def compress_flask_response(response, request, compression_level=6):

    if response.direct_passthrough:
        return response

    request_headers = request.headers
    if 'Accept-Encoding' in request_headers and 'gzip' in request_headers['Accept-Encoding']:
        # Compress json
        out = gzip.compress(response.data, compression_level)
        response.data = out
        response.headers.add('Content-Encoding', 'gzip')
        response.headers.add('Content-length', len(out))
    return response

    
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


# class dataset_projection:
#     '''
#     Takes a multi-scale loaded dataset with dims (t,c,z,y,x) and presents it as a max, min or mean intensity projection
#     of itself in the 3 spatial dimensions (z,y,x). Projection_dict is the final resolution of z,y,x in microns for the projection.
#     '''
#     def __init__(self,reference_dataset, projection_dict={'z':100, 'y':None, 'x':None}, projection_type='max', ResolutionLevelLock=0):
#         # Store reference dataset
#         self.reference_dataset = reference_dataset
#         self.reference_dataset_full_res = self.reference_dataset.metaData[0, t, c, 'resolution']
#
#         # Store desired projection
#         self.projection_dict = projection_dict
#         self.projection_type = projection_type
#         self.projection_resolution = []
#         for axis,calib in projection_dict.items():
#             if axis.lower() == 'z':
#                 if calib is not None:
#                     self.projection_resolution.append(calib)
#                 else:
#                     self.projection_resolution.append(self.reference_dataset_full_res[0])
#             if axis.lower() == 'y':
#                 if calib is not None:
#                     self.projection_resolution.append(calib)
#                 else:
#                     self.projection_resolution.append(self.reference_dataset_full_res[1])
#             if axis.lower() == 'x':
#                 if calib is not None:
#                     self.projection_resolution.append(calib)
#                 else:
#                     self.projection_resolution.append(self.reference_dataset_full_res[2])
#
#         self.projection_resolution = tuple(self.projection_resolution)
#
#         self.ResolutionLevels = self.reference_dataset.ResolutionLevels
#         self.ResolutionLevelLock = ResolutionLevelLock
#
#         self.shape = self.reference_dataset.metaData[0, 0, 0, 'shape']
#         self.ndim = len(self.shape)
#         self.chunks = self.reference_dataset.metaData[0, 0, 0, 'chunks']
#         self.resolution = self.reference_dataset.metaData[0, 0, 0, 'resolution']
#         self.dtype = self.reference_dataset.metaData[0, 0, 0, 'dtype']
#
#         self.metaData = {}
#         for r in range(self.ResolutionLevels):
#             if r == 0:
#                 self.TimePoints = self.reference_dataset.TimePoints
#                 self.Channels = self.reference_dataset.Channels
#                 resolution_proportion_of_full = (1,1,1)
#             else:
#                 resolution_proportion_of_full = [x/y for x,y in
#                                                  zip(self.reference_dataset.metaData[r, t, c, 'resolution'],
#                                                      self.reference_dataset_full_res)]
#
#             for t, c in itertools.product(range(self.TimePoints), range(self.Channels)):
#
#                 # Determine resolution for specific projection multiscale based on proportional change of multiscale in
#                 # origional dataset
#                 out_resolution = [x*y for x,y in zip(self.projection_resolution,resolution_proportion_of_full)]
#                 new_shape = self.three_d_shape_to_projection_shape(self.reference_dataset.metaData[r, t, c, 'shape'],
#                                                                    self.reference_dataset.metaData[r, t, c, 'resolution'], out_resolution)
#                 self.metaData[r, t, c, 'shape'] = new_shape
#                 self.metaData[r, t, c, 'resolution'] = out_resolution
#
#                 # Collect dataset info
#                 self.metaData[r, t, c, 'chunks'] = self.reference_dataset.metaData[r, t, c, 'chunks'] # May need to adjust chunks later for now this is ok
#                 self.metaData[r, t, c, 'dtype'] = self.reference_dataset.metaData[r, t, c, 'dtype']
#                 self.metaData[r, t, c, 'ndim'] = self.reference_dataset.metaData[r, t, c, 'ndim']
#
#                 try:
#                     self.metaData[r, t, c, 'max'] = self.metaData[r, t, c, 'max']
#                     self.metaData[r, t, c, 'min'] = self.metaData[r, t, c, 'min']
#                 except:
#                     pass
#
#         self.change_resolution_lock(self.ResolutionLevelLock)
#
#     def change_resolution_lock(self,ResolutionLevelLock):
#         self.ResolutionLevelLock = ResolutionLevelLock
#         self.shape = self.metaData[self.ResolutionLevelLock,0,0,'shape']
#         self.ndim = len(self.shape)
#         self.chunks = self.metaData[self.ResolutionLevelLock,0,0,'chunks']
#         self.resolution = self.metaData[self.ResolutionLevelLock,0,0,'resolution']
#         self.dtype = self.metaData[self.ResolutionLevelLock,0,0,'dtype']
#
#     @staticmethod
#     def three_d_shape_to_projection_shape(in_shape, in_resolution, out_resolution):
#         '''
#         Takes 3D shape (z,y,z) and resolution in microns for that shape and outputs the projection shape
#         '''
#
#         # Ensure that projection out_resolution(s) are lower (higher value) than in_resolution(s)
#         assert in_resolution[0] < out_resolution[0], 'projection must be from a higher resolution to a lower resolution''
#         assert in_resolution[1] < out_resolution[1], 'projection must be from a higher resolution to a lower resolution''
#         assert in_resolution[2] < out_resolution[2], 'projection must be from a higher resolution to a lower resolution''
#
#         # Calculate proportion change for each dimension based on resolution
#         z_change = in_resolution[0] / out_resolution[0] if out_resolution[0] is not None else 1
#         y_change = in_resolution[1] / out_resolution[1] if out_resolution[1] is not None else 1
#         x_change = in_resolution[2] / out_resolution[2] if out_resolution[2] is not None else 1
#
#         # Calculate new dimension shape. Always use floor function
#         z = math.floor(in_shape[0] * z_change)
#         y = math.floor(in_shape[1] * y_change)
#         x = math.floor(in_shape[2] * x_change)
#
#         return (z,y,x)
#
#     @staticmethod
#     def slice_resizer(key, in_shape, out_shape):
#         '''
#         Given a slice key for in_shape
#         return a slice key fit to out_shape
#         '''
#         # All shape math must use floor function to create integer
#         offset_factor = [y/x for x,y in zip(in_shape,out_shape)]
#         new_key = []
#         for k in key:
#             pass
#
#
#



