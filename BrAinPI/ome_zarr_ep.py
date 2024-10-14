# -*- coding: utf-8 -*-
"""
Created on Mon May 16 22:48:44 2022

@author: awatson
"""

# import zarr
from logger_tools import logger
import numpy as np
import numcodecs
from numcodecs import Blosc
try:
    from imagecodecs.numcodecs import JpegXl
    numcodecs.register_codec(JpegXl)
    # logger.info('Imported JpegXl')
except:
    pass
try:
    from imagecodecs.numcodecs import Jpegxl
    numcodecs.register_codec(Jpegxl)
    # logger.info('Imported Jpegxl')
except:
    pass
import io
import re
import os
import math
from flask import (
    render_template,
    request,
    send_file,
    redirect,
    jsonify,
    abort,
    Response
    )

from flask_login import login_required
from flask_cors import cross_origin


import utils



def where_is_that_chunk(chunk_name='0.0.1.3.14', dataset_shape=(1,1,2,23857,14623), chunk_size=(1,1,1,1000,1000)):
    '''
    Given a chunk file name, what is the pixel coordinates for that chunk in a larger datasets
    '''
    t,c,z,y,x = (int(x) for x in chunk_name.split('.'))
    
    location = {}
    
    location['tStart'] = t * chunk_size[0]
    t = location['tStart'] + chunk_size[0]
    location['tStop'] = t if t <= dataset_shape[0] else None
    
    location['cStart'] = c * chunk_size[1]
    c = location['cStart'] + chunk_size[1]
    location['cStop'] = c if c <= dataset_shape[1] else None
    
    location['zStart'] = z * chunk_size[2]
    z = location['zStart'] + chunk_size[2]
    location['zStop'] = z if z <= dataset_shape[2] else None
    
    location['yStart'] = y * chunk_size[3]
    y = location['yStart'] + chunk_size[3]
    location['yStop'] = y if y <= dataset_shape[3] else None
    
    location['xStart'] = x * chunk_size[4]
    x = location['xStart'] + chunk_size[4]
    location['xStop'] = x if x <= dataset_shape[4] else None
    
    return location


def get_chunk(locationDict,res,dataset, chunk_size):

    return dataset[
        res,
        slice(locationDict['tStart'],locationDict['tStop']),
        slice(locationDict['cStart'],locationDict['cStop']),
        slice(locationDict['zStart'],locationDict['zStop']),
        slice(locationDict['yStart'],locationDict['yStop']),
        slice(locationDict['xStart'],locationDict['xStop'])
        ]


def pad_chunk(chunk, chunk_size):
    if chunk.shape == chunk_size:
        return chunk
    
    canvas = np.zeros(chunk_size,dtype=chunk.dtype)
    if len(chunk.shape)==5:
        canvas[0:chunk.shape[0],0:chunk.shape[1],0:chunk.shape[2],0:chunk.shape[3],0:chunk.shape[4]] = chunk
    elif len(chunk.shape)==4:
        canvas[0:chunk.shape[0],0:chunk.shape[1],0:chunk.shape[2],0:chunk.shape[3]] = chunk
    elif len(chunk.shape)==3:
        canvas[0:chunk.shape[0],0:chunk.shape[1],0:chunk.shape[2]] = chunk
    elif len(chunk.shape) == 2:
        canvas[0:chunk.shape[0], 0:chunk.shape[1]] = chunk
    return canvas


def get_compressor():
    # return Blosc(cname='lz4',clevel=3)
    # return Blosc(cname='lz4', clevel=3, shuffle=Blosc.SHUFFLE, blocksize=0)
    return Blosc(cname='zstd', clevel=5, shuffle=Blosc.SHUFFLE, blocksize=0)


# def compress_zarr_chunk(np_array,compressor=get_compressor()):
#     bytestream = io.BytesIO()
#     np.save(bytestream, np_array)
#     uncompressed = bytestream.getvalue()
#     return compressor.encode(uncompressed)

# def compress_zarr_chunk(np_array,compressor=get_compressor()):
#     bytestream = io.BytesIO()
#     np.save(bytestream, np_array)
#     bytestream.seek(0)
#     uncompressed = bytestream.getvalue()
#     compressed = compressor.encode(uncompressed)
#     return compressed

# def compress_zarr_chunk(np_array,compressor=get_compressor()):
#     compressed = compressor.encode(np_array.tobytes())
#     return compressed

def compress_zarr_chunk(np_array,compressor=get_compressor()):
    # buf = np.asarray(np_array).astype(np_array.dtype, casting="safe")
    buf = np_array.tobytes('C')
    if compressor is not None:
        buf = compressor.encode(buf)
    img_ram = io.BytesIO()
    img_ram.write(buf)
    img_ram.seek(0)

    return img_ram

def chunks_combine_channels(metadata,resolution_level):
    '''
    Assumes 5 dimensions and replaces channel dim (t,c,z,y,x) with the number of channels.
    This is good for neuroglancer compatibility because it enables all channels to be delivered with each chunk
    enabling a shader to mix all channels into RGB representation

    return new chunk_size tuple
    '''
    chunks = list(metadata[(resolution_level, 0, 0, 'chunks')])
    chunks[1] = metadata['Channels']
    return tuple(chunks)

def get_zarray_file(numpy_like_dataset,resolution_level,combine_channels=False,force8Bit=False):

    metadata = utils.metaDataExtraction(numpy_like_dataset,strKey=False)

    zarray = {}
    if combine_channels:
        zarray['chunks'] = chunks_combine_channels(metadata,resolution_level)
    else:
        zarray['chunks'] = metadata[(resolution_level,0,0,'chunks')]

    compressor = get_compressor()
    zarray['compressor'] = {}
    zarray['compressor']['blocksize'] = compressor.blocksize
    zarray['compressor']['clevel'] = compressor.clevel
    zarray['compressor']['cname'] = compressor.cname
    zarray['compressor']['id'] = compressor.codec_id
    zarray['compressor']['shuffle'] = compressor.shuffle

    # zarray['dimension_separator'] = '.'
    zarray['dimension_separator'] = '/' #<-- required for compatibility with ome-zarr 4.0
    if force8Bit:
        zarray['dtype'] = encoding_values['uint8']
    else:
        zarray['dtype'] = encoding_values[metadata[(0, 0, 0, 'dtype')]]#'<u2' if numpy_like_dataset.dtype == np.uint16 else '<u2'  ## Need to figure out other dtype opts for uint8, float et al
    zarray['fill_value'] = 0
    zarray['filters'] = None
    zarray['order'] = 'C'
    zarray['shape'] = metadata['TimePoints'],metadata['Channels'],*metadata[(resolution_level,0,0,'shape')][-3:]
    zarray['zarr_format'] = 2

    return zarray


colors = [
    '00FF00', #green
    'FF0000', #red
    'FF00FF', #purple
    '0000FF', #blue
    'FFFF00'  #yellow
    ]

values = {
    np.dtype('uint8'):(0,255),
    np.dtype('uint16'):(0,65535),
    np.dtype('float'):(0,1),
    np.dtype('float32'):(0,1),
    np.dtype('float64'):(0,1),
    'uint8':(0,255),
    'uint16':(0,65535),
    'float':(0,1),
    'float32':(0,1),
    'float64':(0,1)
    }

value_types = {
    np.dtype('uint8'):'uint8',
    np.dtype('uint16'):'uint16',
    np.dtype('float'):float,
    np.dtype('float32'):float,
    np.dtype('float64'):float,
    'uint8':'uint8',
    'uint16':'uint16',
    'float':float,
    'float32':float,
    'float64':float
    }

encoding_values = {
    np.dtype('uint8'):'|u1',
    np.dtype('uint16'):'<u2',
    np.dtype('float'):'<f4',
    np.dtype('float32'):'<f4',
    np.dtype('float64'):'<f8',
    'uint8':'|u1',
    'uint16':'<u2',
    'float':'<f4',
    'float32':'<f4',
    'float64':'<f8'
    }

max_values = {
    np.dtype('uint8'):254,
    np.dtype('uint16'):65534,
    np.dtype('float'):1,
    np.dtype('float32'):1,
    np.dtype('float64'):1,
    'uint8':254,
    'uint16':65534,
    'float':1,
    'float32':1,
    'float64':1
}

# def conv_np_dtypes(array,tdtype):
#     if array.dtype == tdtype:
#         return array
#     if tdtype == 'uint8' or tdtype == np.dtype('uint8'):
#         return img_as_ubyte(array)
#     if tdtype == 'uint16' or tdtype == np.dtype('uint16'):
#         return img_as_uint(array)
#     if tdtype == 'float32' or tdtype == np.dtype('float32'):
#         return img_as_float32(array)
#     if tdtype == float or tdtype == 'float64' or tdtype == np.dtype('float64'):
#         return img_as_float64(array)

def conv_dtype_value(value,fdtype,tdtype):
    ratio = max_values[tdtype]/max_values[fdtype]
    return round(ratio * value)

#####################################
### OME-NGFF 0.4 Complient .zattr ###
#####################################

def get_zattr_file(numpy_like_dataset,force8Bit=False):

    metadata = utils.metaDataExtraction(numpy_like_dataset,strKey=False)
    # metadata = metaDataExtraction(numpy_like_dataset,strKey=False)

    zattr = {}

    ### Build creator info ###
    zattr['_creator'] = {
        'name':'BrAinPI',
        'version':'0.3.0'
        }

    ###################
    ### MULTISCALES ###
    ###################
    axes = [
        {'name':'t',
         'type':'time',
         'unit':'second'
            },
        {'name':'c',
         'type':'channel'
            },
        {'name':'z',
         'type':'space',
         'unit':'micrometer'
            },
        {'name':'y',
         'type':'space',
         'unit':'micrometer'
            },
        {'name':'x',
         'type':'space',
         'unit':'micrometer'
            }
        ]

    datasets = []
    for res in range(metadata['ResolutionLevels']):
        if res == 0:
            translation = (0,0,0,0,0)
        else:
            # Proportional difference in spacing between current and previous scale
            translation = [x/y for x,y in zip(metadata[(res, 0, 0, 'resolution')],metadata[(res-1, 0, 0, 'resolution')])]
            # Determine micron shift in origin by dividing current resoluton by proportional difference
            translation = [x/y if y!=1 else 0 for x,y in zip(metadata[(res, 0, 0, 'resolution')], translation)]
            # translation = [x if x!=1 else 0 for x in translation]
            translation = (0,0,*translation)

            # # I think we can just pass the previous resolution here (need to test carefully)
            # translation = metadata[(res-1, 0, 0, 'resolution')]
            # translation = (0, 0, *translation)
        level = {
            'path':str(res),
            'coordinateTransformations':[
                {
                    'scale':(1,1,*metadata[(res,0,0,'resolution')]),
                    'type':'scale'
                    },
                {
                    'translation': translation, # Spatial units, same as scale
                    'type': 'translation'
                }
                ]
            }
        datasets.append(level)

    zattr['multiscales'] = [
        {
        'axes':axes,
        'datasets':datasets,
        'version':'0.4',
        'type':'gaussian',
        'metadata':{
            'description':'Describe how multiscale was created',
            'method': 'The function used to create',
            'version':'version of method',
            'other':'stuff',
            'other2':'stuff2'
            }
        }
        ]

    colors * math.ceil(metadata['Channels']/len(colors))


    # lowest_res_level = numpy_like_dataset[metadata['ResolutionLevels']-1,0,0,:,:,:]


    #############
    ### OMERO ###
    #############
    channels = []
    for ch in range(metadata['Channels']):
        current_channel_data = numpy_like_dataset[metadata['ResolutionLevels']-1,0,ch,:,:,:]
        dtype = current_channel_data.dtype

        end = int(current_channel_data.max()) if not value_types[dtype] == float else float(current_channel_data.max())
        start = int(current_channel_data.min()) if not value_types[dtype] == float else float(current_channel_data.min())

        if force8Bit:
            end = conv_dtype_value(end, dtype, 'uint8')
            start = conv_dtype_value(start, dtype, 'uint8')

        if value_types[dtype] == float and \
            ((end > 1 or end < 1) or \
                (start < 1 or start > 1)): #<---  May need to look at this more closely

            if end > 1:
                max_window = end*2
            if end < 1:
                max_window = end/2
            if start > 1:
                min_window = start / 2
            if start <= 0:
                min_window = start*2

        else:
            max_window = end*2 if end*2 <= values[dtype][1] else values[dtype][1]
            min_window = start//2 if start//2 >= values[dtype][0] else values[dtype][0]

            if force8Bit:
                max_window = conv_dtype_value(max_window, dtype, 'uint8')
                min_window = conv_dtype_value(min_window, dtype, 'uint8')

        channel = {
            'active':True,
            'coefficient': 1.0,
            'color':colors[ch],
            'family':'linear',
            'inverted':False,
            'label': 'Channel_{}'.format(ch),
            'window':{
                'end': end,
                'max': max_window,
                'min':min_window,
                'start':start
                }
            }

        channels.append(channel)


    zattr['omero'] = {
        'id':1,
        'name':"Need to add file name.ext",
        'version': '0.5-dev',
        'channels':channels,
        'rdefs':{
            'defaultT':0, #Default timepoint to display
            'defaultZ':metadata[(0, 0, 0, 'shape')][2]//2, #Default z-layer to display
            'model':'color' #'color' or 'greyscale'
            }
        }


    return zattr




def open_omezarr_dataset(config,datapath):

    datapath = config.loadDataset(datapath,datapath)

    # if not hasattr(config.opendata[datapath],'ng_json'):
        # or not hasattr(config.opendata[datapath],'ng_files'):

            ## Forms a comrehensive file list for all chunks
            ## Not necessary for neuroglancer to function and take a long time
            # config.opendata[datapath].ng_files = \
            #     neuroGlancer.ng_files(config.opendata[datapath])

            ## Temp ignoring of ng_files
            ## Add attribute so this constantly repeated
            # config.opendata[datapath].ng_files = True

            # config.opendata[datapath].ng_json = \
            #     ng_json(config.opendata[datapath],file='dict')

    return datapath


file_name_template = '{}.{}.{}.{}.{}'
file_pattern = file_name_template.format('[0-9]+','[0-9]+','[0-9]+','[0-9]+','[0-9]+','[0-9]+')

# OME-Zarr extensions to enable arbitrary use of extension for ome zarr compatibility in various apps
# NOTE: .ng.ome.zarr causes a change in the represented chunk size by combining channels into each chunk using func chunks_combine_channels
exts = ['.ng.ome.zarr','.ome.zarr','.omezarr','.ome.ngff','.ngff']


def setup_omezarr(app, config):

    chunks_size_pattern = '^.*[0-9]+x[0-9]+x[0-9]+.*$'  # desired chunk size designated as .##x##x##. where ## are int
    # designated as axes (z,y,x)

    # Establish highly used functions as objects to improve speed
    get_html_split_and_associated_file_path = utils.get_html_split_and_associated_file_path
    match = re.match
    Match_class = re.Match

    def omezarr_entry(req_path):

        path_split, datapath = get_html_split_and_associated_file_path(config,request)

        # logger.info(path_split)
        # logger.info(datapath)
        ##  HAck if ignores '.' as dimension_sperator
        try:
            new_path = path_split[-5:]
            # logger.info(new_path)
            if all([isinstance(int(x),int) for x in new_path]):
                # logger.info('in if')
                new_path = '.'.join(new_path)
                # logger.info(new_path)
                request.path = '/' + os.path.join(*path_split[:-5],new_path)
                # logger.info(request.path)
                path_split, datapath = get_html_split_and_associated_file_path(config,request)
            else:
                pass
        except Exception:
            pass

        # Flag to deliver 8bit data to NG
        force8Bit = False
        if '.8.' in datapath or '.8bit.' in datapath:
            force8Bit = True
            datapath = datapath.replace('.8.', '.')
            datapath = datapath.replace('.8bit.', '.')

        # Attempt to enable ome.zarr ext for compatibility with omezarr utils
        isNeuroGlancer = False
        for ext in exts:
            if len(datapath.split(ext)) > 1:
                datapath = datapath.replace(ext,'')
                if ext == '.ng.ome.zarr': isNeuroGlancer = True
                # logger.info(f'DATAPATH MINUS EXT: {datapath}')
                break

        # Define chunk size if specified in the path otherwise keep None
        chunk_size = None
        if match(chunks_size_pattern, datapath):
            # Modified chunk size must be designated as an extension that comes before any other extenstions
            # designated as .##x##x##. where ## is pixels numbers in axes (Z,Y,X)
            logger.info('INSIDE THE PATTERN ######################################')
            logger.info(datapath)
            reverse_split = datapath.split('.')[::-1]
            any_matches = tuple(
                (
                    x for x in reverse_split if isinstance(
                    match(chunks_size_pattern, x), Match_class
                )
                )
            )

            if len(any_matches) > 0:
                for ii in any_matches:
                    chunk = ii.split('x')
                    logger.info(f'CHUNKS ######## {chunk}')
                    chunk_size = tuple(
                        (int(x) for x in chunk)
                    )
                    # Force chunk_size to 5-dims
                    if len(chunk_size) == 3:
                        chunk_size = (1, 1, *chunk_size)
                    elif len(chunk_size) == 4:
                        chunk_size = (1, *chunk_size)
                    elif len(chunk_size) == 2:
                        chunk_size = (1, 1, 1, *chunk_size)
                    logger.info(f'CHUNK SHAPE {chunk_size}')

                    datapath = datapath.replace(f'.{ii}.', '.')
                    logger.info(f'DATAPATH ###### {datapath}')
                    logger.info(f'CHUNK SIZE ###### {chunk_size}')

                    ####  SWITCH TO None to negate the chunk setting block and default to default chunks ####
                    # chunk_Size = None
                    # Only use the first match
                    break

            logger.info(path_split)

        logger.info(path_split)
        # Find the file system path to the dataset
        # Assumptions are neuroglancer only requests 'info' file or chunkfiles
        # If only the file name is requested this will redirect to a
        if isinstance(match(file_pattern,path_split[-1]),Match_class):
            # Find requested chunk name
            chunk_name = path_split[-1]
            resolution = int(datapath.split('/')[-2])

            # Open dataset
            datapath = os.path.split(datapath)[0]
            datapath = os.path.split(datapath)[0]
            # datapath = '/' + os.path.join(*datapath.split('/')[:-2])
            datapath = open_omezarr_dataset(config,datapath)
            config.opendata[datapath].metadata

            if isNeuroGlancer:
                # logger.info(451)
                chunk_size = chunks_combine_channels(config.opendata[datapath].metadata,resolution)
                # logger.info(453)
                # logger.info(chunk_size)
            else:
                chunk_size = config.opendata[datapath].metadata[(resolution,0,0,'chunks')]
                # logger.info(456)

            # Determine where the chunk is in the actual dataset
            dataset_shape = config.opendata[datapath].metadata[(resolution, 0, 0, 'shape')]
            # logger.info(dataset_shape)
            locationDict = where_is_that_chunk(chunk_name=chunk_name, dataset_shape=dataset_shape, chunk_size=chunk_size)
            # logger.info('Chunk is here:')
            # logger.info(locationDict)

            chunk = None
            if config.cache is not None:
                key = f'omezarr_{locationDict}-{resolution}-{chunk_size}-{isNeuroGlancer}-{force8Bit}'
                chunk = config.cache.get(key, default=None, retry=True)

            if chunk is None:
                chunk = get_chunk(locationDict,resolution,config.opendata[datapath],chunk_size)
                # logger.info(chunk.shape)
                chunk = pad_chunk(chunk, chunk_size)
                if force8Bit:
                    chunk = utils.conv_np_dtypes(chunk, 'uint8')
                # logger.info(chunk.shape)
                # chunk = np.squeeze(chunk)
                # logger.info(chunk.shape)
                chunk = compress_zarr_chunk(chunk,compressor=get_compressor())

                # Add to cache
                if config.cache is not None:
                    # a = dask.delayed(config.cache.set)(key, chunk, expire=None, tag=datapath, retry=True)

                    config.cache.set(key, chunk, expire=None, tag=datapath, retry=True)

                # Flask return of bytesIO as file

            return Response(response=chunk, status=200,
                            mimetype="application/octet_stream")


        elif path_split[-1] == 'labels':
            abort(404)
        elif path_split[-1] == '.zarray':
            if path_split[-2] == 'labels':
                abort(404)
            try:
                resolution = int(datapath.split('/')[-2])
                datapath = os.path.split(datapath)[0]
                datapath = os.path.split(datapath)[0]
                # datapath = '/' + os.path.join(*datapath.split('/')[:-2])
                datapath = open_omezarr_dataset(config,datapath)
                # return Response(response=get_zarray_file(config.opendata[datapath],resolution), status=200,
                #                 mimetype="application/octet_stream")
                return jsonify(get_zarray_file(config.opendata[datapath],resolution,combine_channels=isNeuroGlancer,force8Bit=force8Bit))
            except:
                abort(404)
        elif path_split[-1] == '.zattrs':
            if path_split[-2] == 'labels':
                abort(404)
            datapath = os.path.split(datapath)[0]
            datapath = open_omezarr_dataset(config,datapath)
            # return Response(response=get_zattr_file(config.opendata[datapath]), status=200,
            #                 mimetype="application/octet_stream")
            return jsonify(get_zattr_file(config.opendata[datapath],force8Bit=force8Bit))
        elif path_split[-1] == '.zgroup':
            # return Response(response={'zarr_format':2}, status=200,
            #                 mimetype="application/octet_stream")
            return jsonify(
                {'zarr_format':2}
                )

        # files = ['.zattrs','.zgroup']
        # path_join = '/'.join(path_split)
        # tmp = '<html><body><pre>'
        # file_template = '''<a href="{}">{}</a> '''
        # for ii in files:
        #     # tmp_join = '/'.join([path_join, ii])
        #     tmp = tmp + file_template.format(ii,ii)
        # tmp = tmp + '</pre></body></html>'

        # return Response(tmp)
        abort(404)

        #####  FUNCTION END  ######



    zarrpath = '/omezarr/' #<--- final slash is required for proper navigation through dir tree

    # Decorating neuro_glancer_entry to allow caching ##
    # if config.cache is not None:
    #     logger.info('Caching setup')
    #     omezarr_entry = config.cache.memoize()(omezarr_entry)
    #     logger.info(omezarr_entry)
    # neuro_glancer_entry = login_required(neuro_glancer_entry)

    omezarr_entry = cross_origin(allow_headers=['Content-Type'])(omezarr_entry)
    # omezarr_entry = login_required(omezarr_entry)
    omezarr_entry = app.route(zarrpath + '<path:req_path>')(omezarr_entry)
    omezarr_entry = app.route(zarrpath, defaults={'req_path': ''})(omezarr_entry)
    
    return app
    
    









'''
OME-NGFF Examples

Multi channel, multiscale, Cells:
    napari --plugin napari-ome-zarr https://uk1s3.embassy.ebi.ac.uk/idr/share/gbi2022/6001237/idr.zarr
    https://uk1s3.embassy.ebi.ac.uk/idr/share/gbi2022/6001237/idr.zarr/.zattrs
    https://uk1s3.embassy.ebi.ac.uk/idr/share/gbi2022/6001237/idr.zarr/0/.zarray
    
Single channel, multiscale, EM:
    napari --plugin napari-ome-zarr https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.1/4495402.zarr
    https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.1/4495402.zarr/.zattrs
    https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.1/4495402.zarr/0/.zarray
    
'''








# '''
# Below is a recipe for creating a zarr array and reading / decoding 
# individual chunks
# '''
# from itertools import product
# import math
# import zarr

# compressor = Blosc(cname='zstd', clevel=1, shuffle=Blosc.BITSHUFFLE,blocksize=0)
# # compressor = None

# shape = (1,1,2,10010,10010)
# dtype = np.uint16
# chunks=(1,1,1,1000,1000)

# z1 = zarr.open('Z:/toTest/testZarr', mode='w', shape=shape,
#                 chunks=chunks, dtype=np.uint16,compressor=compressor)

# z1[:] = 42

# # file = 'Z:/toTest/testZarr/0.0.1.10.10'
# compressor = get_compressor()
# with open(file,'rb') as f:
#     z = f.read()
#     if compressor:
#         z = compressor().decode(z)
#     z = np.frombuffer(z, dtype=np.uint16)
#     z = z.reshape(chunks)


# '''
# For an array of a given size, determine how to 'make' a virtual zarr array
# '''

# shape = (1,2,512,40265,30564)
# chunks = (1,1,1,1000,1000)

# dtype = np.uint16
# compressor = Blosc(cname='zstd', clevel=1, shuffle=Blosc.BITSHUFFLE,blocksize=0)

# ## Build virtual zarr chunks
# chunk_range = []
# chunk_mod = []
# for sh,ch in zip(shape,chunks):
#     chunk_range.append(math.ceil(sh/ch))
#     chunk_mod.append(sh%ch)

# chunk_template = '{}.{}.{}.{}.{}'
# chunks_list = []
# for t,c,z,y,x in product(
#         range(chunk_range[0]),
#         range(chunk_range[1]),
#         range(chunk_range[2]),
#         range(chunk_range[3]),
#         range(chunk_range[4])
#         ):
#     tmp = chunk_template.format(t,c,z,y,x)
#     logger.info(tmp)
#     chunks_list.append(tmp)
    





