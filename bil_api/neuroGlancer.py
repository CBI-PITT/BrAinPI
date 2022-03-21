# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 16:39:55 2022

@author: awatson
"""
# bil_api imports
import utils
from itertools import product
import io
import json
from neuroglancer_scripts.chunk_encoding import RawChunkEncoder


def encode_ng_file(numpy_array,channels):
    encoder = RawChunkEncoder(numpy_array.dtype, channels)
    img_ram = io.BytesIO()
    img_ram.write(encoder.encode(numpy_array))
    img_ram.seek(0)


    # # Write numpy to NG raw chunk to IO buffer
    # img_ram = io.BytesIO()
    # img_ram.write(encoder.encode(numpy_array))
    # img_ram.seek(0)
    return img_ram




#metadata extracted from datasetimport json


# 

## Build neuroglancer json

def ng_json(numpy_like_object,file=None):
    '''
    Save a json from a 5d numpy like volume
    file = None saves to a BytesIO buffer
    file == str saves to that file name
    '''

    metadata = utils.metaDataExtraction(numpy_like_object,strKey=False)
    
    neuro_info = {}
    neuro_info['data_type'] = metadata['dtype']
    neuro_info['num_channels'] = metadata['Channels']
    
    scales = []
    current_scale = {}
    for res in range(metadata['ResolutionLevels']):
        current_scale["chunk_sizes"] = [
            list(
                reversed(
                    list(metadata[(res,0,0,'chunks')][-3:])
                    )
                )
            ]
        
        current_scale["encoding"] = 'raw'
        current_scale["key"] = str(res)
        current_scale["resolution"] = [x*1000 for x in list(
            reversed(
                list(metadata[(res,0,0,'resolution')])
                )
            )
            ]
        current_scale["size"] = list(
            reversed(
                list(metadata[(res,0,0,'shape')][-3:])
                )
            )
        current_scale["voxel_offset"] = [0, 0, 0]
        
        scales.append(current_scale)
        current_scale = {}
        
    
    neuro_info['scales'] = scales
    neuro_info['type'] = 'image'
    
    if file is None:
        b = io.BytesIO()
        b.write(json.dumps(neuro_info).encode())
        b.seek(0)
        return b
    elif file == 'str':
        return json.dumps(neuro_info)
    elif file == 'dict':
        return neuro_info
    else:
        with open(file,'w') as f:
            f.write(json.dumps(neuro_info))
        return


'''
File name convention by chunk = [x,y,z] <-- note: opposite from numpy (z,y,x)
chunks == [10,15,2]
size == [18,35,1]

Files:
    0-10_0-15_0-1
    0-10_15-30_0-1
    0-10_30-35_0-1
    10-18_0-15_0-1
    10-18_15-30_0-1
    10-18_30-35_0-1
    
'''

def ng_files(numpy_like_object):
    '''
    Takes numpy_like_object representing a supported filetype
    and produces a dict where keys are int == resolution level and objects are 
    compreshensive lists of filenames representing each chunk of structure:
        xstart-xstop_ystart-ystop_zstart-zstop
    '''
    
    metadata = utils.metaDataExtraction(numpy_like_object,strKey=False)
    
    name_template = '{}-{}_{}-{}_{}-{}'
    fileLists = {}
    ## Make file list
    for res in range(metadata['ResolutionLevels']):
        chunks = metadata[(res,0,0,'chunks')]
        shape = metadata[(res,0,0,'shape')]
        
        fileLists[res] = []
        for x,y,z in product(
                range(0,shape[-1],chunks[-1]), #X-axis
                range(0,shape[-2],chunks[-2]), #Y-axis
                range(0,shape[-3],chunks[-3])  #Z-axis
                ):
            
            
            currentName = name_template.format(
                x,
                x + chunks[-1] if x + chunks[-1] <= shape[-1] else shape[-1],
                y,
                y + chunks[-2] if y + chunks[-2] <= shape[-2] else shape[-2],
                z,
                z + chunks[-3] if z + chunks[-3] <= shape[-3] else shape[-3],
                
                )
            fileLists[res].append(currentName)
            print(currentName)
    return fileLists




# metadata = {
#  'shape': (1, 2, 3, 27670, 19441),
#  'chunks': (1, 1, 4, 256, 256),
#  'dtype': 'uint16',
#  'ndim': 5,
#  'ResolutionLevels': 7,
#  'TimePoints': 1,
#  'Channels': 2,
#  (0, 0, 0, 'shape'): (1, 1, 3, 27670, 19441),
#  (0, 0, 0, 'resolution'): (10.0, 0.498, 0.498),
#  (0, 0, 0, 'HistogramMax'): 64623,
#  (0, 0, 0, 'HistogramMin'): 0,
#  (0, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (0, 0, 0, 'shapeH5Array'): (4, 27904, 19456),
#  (0, 0, 0, 'dtype'): 'uint16',
#  (0, 0, 1, 'shape'): (1, 1, 3, 27670, 19441),
#  (0, 0, 1, 'resolution'): (10.0, 0.498, 0.498),
#  (0, 0, 1, 'HistogramMax'): 65535,
#  (0, 0, 1, 'HistogramMin'): 0,
#  (0, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (0, 0, 1, 'shapeH5Array'): (4, 27904, 19456),
#  (0, 0, 1, 'dtype'): 'uint16',
#  (1, 0, 0, 'shape'): (1, 1, 3, 13835, 9720),
#  (1, 0, 0, 'resolution'): (10.0, 0.996, 0.996),
#  (1, 0, 0, 'HistogramMax'): 41136,
#  (1, 0, 0, 'HistogramMin'): 0,
#  (1, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (1, 0, 0, 'shapeH5Array'): (4, 14080, 9728),
#  (1, 0, 0, 'dtype'): 'uint16',
#  (1, 0, 1, 'shape'): (1, 1, 3, 13835, 9720),
#  (1, 0, 1, 'resolution'): (10.0, 0.996, 0.996),
#  (1, 0, 1, 'HistogramMax'): 65535,
#  (1, 0, 1, 'HistogramMin'): 0,
#  (1, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (1, 0, 1, 'shapeH5Array'): (4, 14080, 9728),
#  (1, 0, 1, 'dtype'): 'uint16',
#  (2, 0, 0, 'shape'): (1, 1, 3, 6917, 4860),
#  (2, 0, 0, 'resolution'): (10.0, 1.992, 1.992),
#  (2, 0, 0, 'HistogramMax'): 28705,
#  (2, 0, 0, 'HistogramMin'): 0,
#  (2, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (2, 0, 0, 'shapeH5Array'): (4, 7168, 4864),
#  (2, 0, 0, 'dtype'): 'uint16',
#  (2, 0, 1, 'shape'): (1, 1, 3, 6917, 4860),
#  (2, 0, 1, 'resolution'): (10.0, 1.992, 1.992),
#  (2, 0, 1, 'HistogramMax'): 65535,
#  (2, 0, 1, 'HistogramMin'): 0,
#  (2, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (2, 0, 1, 'shapeH5Array'): (4, 7168, 4864),
#  (2, 0, 1, 'dtype'): 'uint16',
#  (3, 0, 0, 'shape'): (1, 1, 3, 3458, 2430),
#  (3, 0, 0, 'resolution'): (10.0, 3.985, 3.984),
#  (3, 0, 0, 'HistogramMax'): 21381,
#  (3, 0, 0, 'HistogramMin'): 0,
#  (3, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (3, 0, 0, 'shapeH5Array'): (4, 3584, 2560),
#  (3, 0, 0, 'dtype'): 'uint16',
#  (3, 0, 1, 'shape'): (1, 1, 3, 3458, 2430),
#  (3, 0, 1, 'resolution'): (10.0, 3.985, 3.984),
#  (3, 0, 1, 'HistogramMax'): 65535,
#  (3, 0, 1, 'HistogramMin'): 0,
#  (3, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (3, 0, 1, 'shapeH5Array'): (4, 3584, 2560),
#  (3, 0, 1, 'dtype'): 'uint16',
#  (4, 0, 0, 'shape'): (1, 1, 3, 1729, 1215),
#  (4, 0, 0, 'resolution'): (10.0, 7.97, 7.968),
#  (4, 0, 0, 'HistogramMax'): 16536,
#  (4, 0, 0, 'HistogramMin'): 0,
#  (4, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (4, 0, 0, 'shapeH5Array'): (4, 1792, 1280),
#  (4, 0, 0, 'dtype'): 'uint16',
#  (4, 0, 1, 'shape'): (1, 1, 3, 1729, 1215),
#  (4, 0, 1, 'resolution'): (10.0, 7.97, 7.968),
#  (4, 0, 1, 'HistogramMax'): 65531,
#  (4, 0, 1, 'HistogramMin'): 0,
#  (4, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (4, 0, 1, 'shapeH5Array'): (4, 1792, 1280),
#  (4, 0, 1, 'dtype'): 'uint16',
#  (5, 0, 0, 'shape'): (1, 1, 3, 864, 607),
#  (5, 0, 0, 'resolution'): (10.0, 15.949, 15.95),
#  (5, 0, 0, 'HistogramMax'): 12182,
#  (5, 0, 0, 'HistogramMin'): 0,
#  (5, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (5, 0, 0, 'shapeH5Array'): (4, 1024, 768),
#  (5, 0, 0, 'dtype'): 'uint16',
#  (5, 0, 1, 'shape'): (1, 1, 3, 864, 607),
#  (5, 0, 1, 'resolution'): (10.0, 15.949, 15.95),
#  (5, 0, 1, 'HistogramMax'): 50946,
#  (5, 0, 1, 'HistogramMin'): 0,
#  (5, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (5, 0, 1, 'shapeH5Array'): (4, 1024, 768),
#  (5, 0, 1, 'dtype'): 'uint16',
#  (6, 0, 0, 'shape'): (1, 1, 3, 432, 303),
#  (6, 0, 0, 'resolution'): (10.0, 31.897, 31.953),
#  (6, 0, 0, 'HistogramMax'): 8203,
#  (6, 0, 0, 'HistogramMin'): 0,
#  (6, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (6, 0, 0, 'shapeH5Array'): (4, 512, 512),
#  (6, 0, 0, 'dtype'): 'uint16',
#  (6, 0, 1, 'shape'): (1, 1, 3, 432, 303),
#  (6, 0, 1, 'resolution'): (10.0, 31.897, 31.953),
#  (6, 0, 1, 'HistogramMax'): 41553,
#  (6, 0, 1, 'HistogramMin'): 0,
#  (6, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (6, 0, 1, 'shapeH5Array'): (4, 512, 512),
#  (6, 0, 1, 'dtype'): 'uint16'
#  }


# # Neuroglancer metadata file example

# example_json_neuro_info = {
#  "data_type": "uint8",
#  "num_channels": 1,
#  "scales": [{"chunk_sizes": [[64, 64, 64]],
#    "encoding": "jpeg",
#    "key": "8_8_8",
#    "resolution": [8, 8, 8],
#    "size": [6446, 6643, 8090],
#    "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#    "encoding": "jpeg",
#    "key": "16_16_16",
#    "resolution": [16, 16, 16],
#    "size": [3223, 3321, 4045],
#    "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#    "encoding": "jpeg",
#    "key": "32_32_32",
#    "resolution": [32, 32, 32],
#    "size": [1611, 1660, 2022],
#    "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#    "encoding": "jpeg",
#    "key": "64_64_64",
#    "resolution": [64, 64, 64],
#    "size": [805, 830, 1011],
#    "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#    "encoding": "jpeg",
#    "key": "128_128_128",
#    "resolution": [128, 128, 128],
#    "size": [402, 415, 505],
#    "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#    "encoding": "jpeg",
#    "key": "256_256_256",
#    "resolution": [256, 256, 256],
#    "size": [201, 207, 252],
#    "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#    "encoding": "jpeg",
#    "key": "512_512_512",
#    "resolution": [512, 512, 512],
#    "size": [100, 103, 126],
#    "voxel_offset": [0, 0, 0]}],
#  "type": "image"}