# -*- coding: utf-8 -*-
"""
Created on Thu Nov  4 10:05:34 2021

@author: alpha
"""


import numpy as np
import io,zlib,ast,os

import imaris_ims_file_reader as ims
from bil_api.dataset_info import dataset_info
from bil_api import zarrLoader
from bil_api import config
from numcodecs import Blosc


# def compress_np(nparr):
#     """
#     Receives a numpy array,
#     Returns a compressed bytestring, uncompressed and the compressed byte size.
#     """
#     bytestream = io.BytesIO()
#     np.save(bytestream, nparr)
#     uncompressed = bytestream.getvalue()
#     compressed = zlib.compress(uncompressed)
#     return compressed, len(uncompressed), len(compressed)

# def uncompress_np(bytestring):
#     """
#     Receives a compressed bytestring,
#     Returns a numpy array.
#     """
#     array = zlib.decompress(bytestring)
#     array = io.BytesIO(array)
    
    # return np.load(array)




def compress_np(nparr):
    """
    Receives a numpy array,
    Returns a compressed bytestring, uncompressed and the compressed byte size.
    """
    
    comp = Blosc(cname='zstd', clevel=9, shuffle=Blosc.SHUFFLE)
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
    
    comp = Blosc(cname='zstd', clevel=9, shuffle=Blosc.SHUFFLE)
    array = comp.decode(bytestring)
    array = io.BytesIO(array)
    
    sequeeze = False
    if "NAPARI_ASYNC" in os.environ or "NAPARI_OCTREE" in os.environ:
        sequeeze = True
    if sequeeze == True and (os.environ["NAPARI_ASYNC"] == '1' or os.environ["NAPARI_OCTREE"] == "1"):
        return np.squeeze(np.load(array))
    else:
        return np.load(array)




# a = np.zeros((10,10,10), dtype=np.uint16)

# bytestream = io.BytesIO()
# np.save(bytestream, a)
# uncompressed = bytestream.getvalue()


# comp = Blosc(cname='zstd', clevel=1, shuffle=Blosc.SHUFFLE)

# x = comp.encode(uncompressed)

# c = comp.decode(x)
# array = io.BytesIO(c)
# np.load(array)

# a = np.zeros((10,10,10), dtype=np.uint16)
# z,_,_ = compress_np(a)
# x = uncompress_np(z)















def convertMetaDataDict(meta):
    
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

# def loadDataset(selection: int):
    
#     dataPath = dataset_info()[selection][1]
       
#     if os.path.splitext(dataPath)[1] == '.ims':
        
#         if dataPath in globals() == False:
#             globals()[dataPath] = ims.ims(dataPath)
        
#         if globals()[dataPath].hf is None or globals()[dataPath].dataset is None:
#             globals()[dataPath].open()
        
#         return dataPath
    
    
def loadDataset(selection: int):

    
    dataPath = dataset_info()[selection][1]
    print(dataPath)
    
    if 'config' in globals() and hasattr(globals()['config'], 'opendata') == False:
        print('Creating opendata')
        globals()['config'].opendata = {}
    
    
    if os.path.splitext(dataPath)[-1] == '.ims':
        print('Is IMS')
        
        if (dataPath in globals()['config'].opendata) == False:
            print('Creating ims object')
            globals()['config'].opendata[dataPath] = ims.ims(dataPath)
        
        if globals()['config'].opendata[dataPath].hf is None or globals()['config'].opendata[dataPath].dataset is None:
            print('opening ims object')
            globals()['config'].opendata[dataPath].open()
    
    
    elif os.path.splitext(dataPath)[-1] == '.zarr':
        print('Is Zarr')
        
        if (dataPath in globals()['config'].opendata) == False:
            for _ in range(100):
                print('Creating zarrSeries object')
            globals()['config'].opendata[dataPath] = zarrLoader.zarrSeries(dataPath)
        
    return dataPath
        
    
    
def prettyPrintDict(aDict):
    print('{}{}{}'.format('Number'.ljust(10),'Name'.ljust(20),'File'))
    for k,v in aDict.items():
        print('{}{}{}'.format(k.ljust(10),v[0].ljust(20),v[1]))
    
    
    
    
    
    
    
    
    
    
    