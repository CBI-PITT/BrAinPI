# -*- coding: utf-8 -*-
"""Virtual OME-Zarr v2 endpoint over BrAinPI dataset loaders.

The endpoint exposes a strict five-dimensional TCZYX hierarchy without
materializing a new Zarr store. It generates root OME attributes, consolidated
metadata, per-resolution ``.zarray`` documents, and compressed chunks on
demand. Missing metadata resources and out-of-grid chunks use HTTP 404; source
loader failures are reported separately as HTTP 502.
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
    """
    Compute the pixel coordinate ranges for a given chunk in a larger dataset.

    The chunk name is assumed to be in the format 't.c.z.y.x', and each component
    is multiplied by the corresponding dimension of chunk_size to determine start and stop indices.

    Parameters:
        chunk_name (str): A dot-separated string indicating the chunk indices for each dimension.
        dataset_shape (tuple): The shape of the full dataset as (t, c, z, y, x).
        chunk_size (tuple): The size of each chunk as (t, c, z, y, x).

    Returns:
        dict: A dictionary with keys 'tStart', 'tStop', 'cStart', 'cStop', 'zStart', 'zStop', 'yStart', 'yStop', 'xStart', 'xStop'
              representing the start and stop coordinates for each respective dimension. If the stop index exceeds
              the dataset dimension, the corresponding value is set to None.
    """
    chunk_coordinates = parse_chunk_name(chunk_name)
    if not chunk_coordinates_in_bounds(
            chunk_coordinates, dataset_shape, chunk_size):
        raise IndexError(
            f'Chunk {chunk_name} is outside array grid '
            f'{get_chunk_grid_shape(dataset_shape, chunk_size)}'
        )

    t,c,z,y,x = chunk_coordinates
    
    location = {}
    
    location['tStart'] = t * chunk_size[0]
    t = location['tStart'] + chunk_size[0]
    location['tStop'] = t if t <= dataset_shape[0] else dataset_shape[0]
    
    location['cStart'] = c * chunk_size[1]
    c = location['cStart'] + chunk_size[1]
    location['cStop'] = c if c <= dataset_shape[1] else dataset_shape[1]
    
    location['zStart'] = z * chunk_size[2]
    z = location['zStart'] + chunk_size[2]
    location['zStop'] = z if z <= dataset_shape[2] else dataset_shape[2]
    
    location['yStart'] = y * chunk_size[3]
    y = location['yStart'] + chunk_size[3]
    location['yStop'] = y if y <= dataset_shape[3] else dataset_shape[3]
    
    location['xStart'] = x * chunk_size[4]
    x = location['xStart'] + chunk_size[4]
    location['xStop'] = x if x <= dataset_shape[4] else dataset_shape[4]
    
    return location


def get_chunk(locationDict,res,dataset, chunk_size):
    """
    Extract a chunk of data from the dataset using the coordinate ranges provided.

    Parameters:
        locationDict (dict): A dictionary containing coordinate ranges for each dimension.
        res (int): The resolution level at which to retrieve the data.
        dataset (numpy.ndarray or similar): The source dataset from which to extract the chunk.
        chunk_size (tuple): The size of the chunk in each dimension.

    Returns:
        numpy.ndarray: The extracted chunk of data.
    """
    return dataset[
        res,
        slice(locationDict['tStart'],locationDict['tStop']),
        slice(locationDict['cStart'],locationDict['cStop']),
        slice(locationDict['zStart'],locationDict['zStop']),
        slice(locationDict['yStart'],locationDict['yStop']),
        slice(locationDict['xStart'],locationDict['xStop'])
        ]


def pad_chunk(chunk, chunk_size):
    """
    Pad a chunk of data with zeros if it does not match the specified chunk size.

    This function creates a new numpy array with the given chunk_size, fills it with zeros,
    and then copies the content of the original chunk into the top-left (or equivalent) corner
    of the new array.

    Parameters:
        chunk (numpy.ndarray): The data chunk to pad.
        chunk_size (tuple): The desired shape for the padded chunk.

    Returns:
        numpy.ndarray: The padded chunk, guaranteed to have the shape specified by chunk_size.
    """
    chunk_size = tuple(chunk_size)
    if chunk.ndim != len(chunk_size):
        raise ValueError(
            f"OME-Zarr chunks must have {len(chunk_size)} dimensions; "
            f"loader returned shape {chunk.shape}"
        )
    if any(actual > expected for actual, expected in zip(chunk.shape, chunk_size)):
        raise ValueError(
            f"Loader chunk shape {chunk.shape} exceeds advertised chunk shape {chunk_size}"
        )
    if chunk.shape == chunk_size:
        return chunk

    canvas = np.zeros(chunk_size,dtype=chunk.dtype)
    destination = tuple(slice(0, size) for size in chunk.shape)
    canvas[destination] = chunk
    return canvas


def get_compressor():
    """
    Create and return a compressor instance for encoding data.

    Returns:
        numcodecs.Blosc: A configured Blosc compressor using zstd codec with clevel=5,
                          enabled shuffle, and blocksize set to 0.
    """
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
    """
    Compress a numpy array chunk into a BytesIO buffer using the specified compressor.

    The function converts the numpy array into a bytes stream and then applies the compressor.
    The output is a BytesIO object that can be returned as a file-like response in a web framework.

    Parameters:
        np_array (numpy.ndarray): The data chunk to compress.
        compressor (object): Compressor object (default is provided by get_compressor).

    Returns:
        io.BytesIO: A BytesIO object containing the compressed data.
    """
    # buf = np.asarray(np_array).astype(np_array.dtype, casting="safe")
    buf = np_array.tobytes('C')
    if compressor is not None:
        buf = compressor.encode(buf)
    img_ram = io.BytesIO()
    img_ram.write(buf)
    img_ram.seek(0)

    return img_ram

def chunks_combine_channels(metadata,resolution_level,chunk_size=None):
    """
    Assumes 5 dimensions and replaces channel dim (t,c,z,y,x) with the number of channels.
    This is good for neuroglancer compatibility because it enables all channels to be delivered with each chunk
    enabling a shader to mix all channels into RGB representation

    Parameters:
        metadata (dict): Metadata dictionary extracted from the dataset.
        resolution_level (int): The resolution level for which the chunk metadata is extracted.
        chunk_size (tuple, optional): Base chunk size to modify. Uses dataset
            metadata when omitted.

    Returns:
        tuple: The new chunk size tuple with channels combined.
    """

    if chunk_size is None:
        chunk_size = metadata[(resolution_level, 0, 0, 'chunks')]
    chunks = list(chunk_size)
    chunks[1] = metadata['Channels']
    return tuple(chunks)

def get_effective_chunk_size(metadata, resolution_level, custom_chunk_size=None,
                             combine_channels=False):
    """Return the chunk shape advertised by metadata and used for reads."""
    chunk_size = custom_chunk_size
    if chunk_size is None:
        chunk_size = metadata[(resolution_level, 0, 0, 'chunks')]
    chunk_size = tuple(chunk_size)

    if combine_channels:
        chunk_size = chunks_combine_channels(
            metadata, resolution_level, chunk_size=chunk_size
        )
    return chunk_size


def get_zarray_file(numpy_like_dataset,resolution_level,combine_channels=False,
                    force8Bit=False,custom_chunk_size=None):
    """
    Generate the zarray metadata for an OME-Zarr dataset.

    This function creates a metadata dictionary with the appropriate chunk sizes,
    data type encoding, and compressor settings for a given resolution level.
    It allows options for combining channels (for Neuroglancer compatibility) and forcing 8-bit data.

    Parameters:
        numpy_like_dataset: The dataset (as a numpy-like array) from which metadata is extracted.
        resolution_level (int): The resolution level to use when generating metadata.
        combine_channels (bool): If True, combines channels into each chunk. Default is False.
        force8Bit (bool): If True, forces the data type to 8-bit. Default is False.
        custom_chunk_size (tuple, optional): Five-dimensional chunk shape to
            advertise instead of the dataset's native chunk shape.

    Returns:
        dict: A dictionary representing the zarray metadata.
    """
    metadata = utils.metaDataExtraction(numpy_like_dataset,strKey=False)

    zarray = {}
    zarray['chunks'] = get_effective_chunk_size(
        metadata,
        resolution_level,
        custom_chunk_size=custom_chunk_size,
        combine_channels=combine_channels,
    )

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
        logger.info('dtype to uint8')
        zarray['dtype'] = np.dtype('uint8').str
    else:
        logger.info(f'metadata dtype: {metadata[(0, 0, 0, "dtype")]}')
        # NumPy's canonical dtype string is already the encoding required by
        # Zarr v2 and covers signed integers and additional numeric dtypes.
        zarray['dtype'] = np.dtype(metadata[(0, 0, 0, 'dtype')]).str
    zarray['fill_value'] = 0
    zarray['filters'] = None
    zarray['order'] = 'C'
    zarray['shape'] = metadata['TimePoints'],metadata['Channels'],*metadata[(resolution_level,0,0,'shape')][-3:]
    zarray['zarr_format'] = 2
    return zarray


def get_resolution_levels(metadata):
    """Return sorted resolution levels that have both shape and chunk metadata."""
    resolutions = {
        key[0]
        for key in metadata
        if (
            isinstance(key, tuple)
            and len(key) == 4
            and key[1:] == (0, 0, 'shape')
            and (key[0], 0, 0, 'chunks') in metadata
        )
    }
    return tuple(sorted(resolutions))


def get_zmetadata_file(numpy_like_dataset, combine_channels=False,
                       force8Bit=False, custom_chunk_size=None):
    """Generate consolidated Zarr v2 metadata for non-listable HTTP stores."""
    dataset_metadata = utils.metaDataExtraction(
        numpy_like_dataset, strKey=False
    )
    consolidated = {
        '.zgroup': {'zarr_format': 2},
        '.zattrs': get_zattr_file(
            numpy_like_dataset, force8Bit=force8Bit
        ),
    }
    for resolution in get_resolution_levels(dataset_metadata):
        consolidated[f'{resolution}/.zarray'] = get_zarray_file(
            numpy_like_dataset,
            resolution,
            combine_channels=combine_channels,
            force8Bit=force8Bit,
            custom_chunk_size=custom_chunk_size,
        )

    return {
        'zarr_consolidated_format': 1,
        'metadata': consolidated,
    }


colors = [
                "#00FF00",  # 0 green
                "#FF0000",  # 1 red
                "#0000FF",  # 2 blue
                "#800080",  # 3 purple
                "#FFFF00",  # 4 yellow
                "#FFA500",  # 5 orange
                "#00FFFF",  # 6 cyan
                "#FF00FF",  # 7 magenta
                "#32CD32",  # 8 lime green
                "#FF1493",  # 9 deep pink
                "#00BFFF",  # 10 deep sky blue
                "#FFD700",  # 11 gold
                "#7FFF00",  # 12 chartreuse
                "#1E90FF",  # 13 dodger blue
                "#D2691E",  # 14 chocolate
                "#20B2AA",  # 15 light sea green
                "#BA55D3",  # 16 medium orchid
                "#6A5ACD",  # 17 slate blue
                "#FF6347",  # 18 tomato
                "#008080",  # 19 teal
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
    """
    Convert a scalar value from one data type range to another based on maximum allowed values.

    This is typically used to adjust intensity window values when converting image data from a higher bit depth
    to 8-bit.

    Parameters:
        value (numeric): The value to convert.
        fdtype: The original data type of the value.
        tdtype: The target data type to which the value should be converted.

    Returns:
        int: The converted value rounded to the nearest integer.
    """
    ratio = max_values[tdtype]/max_values[fdtype]
    return round(ratio * value)

#####################################
### OME-NGFF 0.4 Complient .zattr ###
#####################################

def get_zattr_file(numpy_like_dataset,force8Bit=False):
    """
    Generate the zattr (attributes) metadata file for an OME-Zarr dataset.

    The metadata includes creator information, multiscale configurations,
    and OMERO metadata for channel windowing and display settings.

    Parameters:
        numpy_like_dataset: The dataset (as a numpy-like array) from which metadata is extracted.
        force8Bit (bool): If True, forces window adjustments for 8-bit data. Default is False.

    Returns:
        dict: A dictionary representing the zattr metadata.
    """
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

    base_resolution = np.asarray(
        metadata[(0, 0, 0, 'resolution')], dtype=np.float64
    )
    base_translation = np.asarray(
        metadata.get((0, 0, 0, 'translation'), (0.0, 0.0, 0.0)),
        dtype=np.float64,
    )

    datasets = []
    for res in range(metadata['ResolutionLevels']):
        source_translation = metadata.get((res, 0, 0, 'translation'))
        if source_translation is not None:
            # Loaders use canonical ZYX micrometre order for source origins.
            spatial_translation = np.asarray(source_translation, dtype=np.float64)
        else:
            current_resolution = np.asarray(
                metadata[(res, 0, 0, 'resolution')], dtype=np.float64
            )
            # NGFF pixel coordinates refer to voxel centres.  For a pyramid
            # made by binning from level 0, the centre of the first coarse
            # voxel is half the difference between its extent and the base
            # voxel extent.  Keep any known level-0 physical origin.
            spatial_translation = (
                base_translation + (current_resolution - base_resolution) / 2.0
            )

        translation = (0.0, 0.0, *spatial_translation.tolist())
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

    multiscale = {
        'axes':axes,
        'datasets':datasets,
        'version':'0.4',
    }

    # Downsampling method fields are optional in NGFF.  Preserve real source
    # declarations when proxying OME-Zarr, but do not invent "gaussian" (or
    # placeholder method metadata) for formats whose pyramid method is unknown.
    source_multiscales = getattr(numpy_like_dataset, 'multiscales', None)
    if isinstance(source_multiscales, list) and source_multiscales:
        source_multiscale = source_multiscales[0]
    elif isinstance(source_multiscales, dict):
        source_multiscale = source_multiscales
    else:
        source_multiscale = {}
    if isinstance(source_multiscale, dict):
        for field in ('type', 'metadata'):
            if field in source_multiscale:
                multiscale[field] = source_multiscale[field]

    zattr['multiscales'] = [multiscale]

    # colors * math.ceil(metadata['Channels']/len(colors))
    if metadata.get('packed_rgb', False) and metadata['Channels'] == 3:
        channel_colors = ["#FF0000", "#00FF00", "#0000FF"]
        channel_labels = ["Red", "Green", "Blue"]
    else:
        channel_colors = colors * math.ceil(metadata['Channels']/len(colors))
        channel_labels = [f"Channel_{ch}" for ch in range(metadata['Channels'])]

    # lowest_res_level = numpy_like_dataset[metadata['ResolutionLevels']-1,0,0,:,:,:]


    #############
    ### OMERO ###
    #############
    channels = []
    source_omero = getattr(numpy_like_dataset, 'omero', None)
    source_channels = (
        source_omero.get('channels', [])
        if isinstance(source_omero, dict)
        else []
    )
    for ch in range(metadata['Channels']):
        source_channel = source_channels[ch] if ch < len(source_channels) else {}
        if not isinstance(source_channel, dict):
            source_channel = {}
        source_window = (
            source_channel.get('window', {})
            if isinstance(source_channel, dict)
            else {}
        )
        window_fields = ('min', 'max', 'start', 'end')
        if all(source_window.get(field) is not None for field in window_fields):
            min_window = source_window['min']
            max_window = source_window['max']
            start = source_window['start']
            end = source_window['end']
            dtype = np.dtype(numpy_like_dataset.dtype)
        else:
            current_channel_data = numpy_like_dataset[
                metadata['ResolutionLevels']-1, 0, ch, :, :, :
            ]
            dtype = current_channel_data.dtype
            logger.info(f'type data: {type(dtype)}')
            if str(dtype).endswith('u2'):
                logger.info('correcting dtype to uint16')
                dtype = np.dtype('uint16')

            is_float = np.issubdtype(dtype, np.floating)
            end = (
                float(current_channel_data.max())
                if is_float
                else int(current_channel_data.max())
            )
            start = (
                float(current_channel_data.min())
                if is_float
                else int(current_channel_data.min())
            )

            if is_float:
                # Float images are commonly normalized to [0, 1], but may use a
                # wider or negative range. Keep that conventional range while
                # ensuring the actual data window is always enclosed.
                max_window = max(1.0, end)
                min_window = min(0.0, start)
            else:
                if np.issubdtype(dtype, np.bool_):
                    min_window, max_window = 0, 1
                else:
                    dtype_limits = np.iinfo(dtype)
                    max_window = min(end * 2, int(dtype_limits.max))
                    min_window = max(start // 2, int(dtype_limits.min))

        if force8Bit:
            # Apply the same source-dtype conversion used for pixel chunks.
            end = utils.dtype_value_to_uint8(end, dtype)
            start = utils.dtype_value_to_uint8(start, dtype)
            min_window = 0
            max_window = 255

        channel = {
            'active': bool(source_channel.get('active', True)),
            'coefficient': source_channel.get('coefficient', 1.0),
            'color': source_channel.get('color') or channel_colors[ch],
            'family': source_channel.get('family', 'linear'),
            'inverted': bool(source_channel.get('inverted', False)),
            'label': source_channel.get('label') or channel_labels[ch],
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

    if force8Bit:
        zattr['brainpi_uint8_conversion'] = {
            'method': 'source_dtype_to_uint8',
            'source_dtype': np.dtype(numpy_like_dataset.dtype).str,
            'source_range': utils.uint8_source_range(numpy_like_dataset.dtype),
        }


    return zattr




def open_omezarr_dataset(config,datapath):
    """
    Open an OME-Zarr dataset using the provided configuration.

    Build the same inode-plus-mtime key used by the other viewers, then ask the
    shared configuration object to load or reuse that exact dataset version.

    Parameters:
        config: A configuration object that contains methods to load the dataset.
        datapath (str): The file system path to the dataset.

    Returns:
        str: Key of the loaded dataset in ``config.opendata``.
    """
    dataset_key = utils.load_dataset(config, datapath)

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

    return dataset_key


def get_dataset_cache_identity(dataset, fallback):
    """
    Build a dataset-specific cache identity that survives identical chunk coordinates
    across different files.

    Parameters:
        dataset: The loaded dataset object.
        fallback (str): Fallback identifier, typically the dataset path/key.

    Returns:
        str: Stable cache identity for the dataset.
    """
    file_ino = getattr(dataset, "file_ino", None)
    modification_time = getattr(dataset, "modification_time", None)
    if file_ino is not None and modification_time is not None:
        return f"{file_ino}{modification_time}"
    return fallback


CHUNK_DIMENSIONS = 5
chunk_name_pattern = re.compile(r'^[0-9]+(?:\.[0-9]+){4}$')


def parse_chunk_name(chunk_name):
    """Parse an exact five-dimensional Zarr v2 chunk key."""
    if not isinstance(chunk_name, str) or chunk_name_pattern.fullmatch(chunk_name) is None:
        raise ValueError(
            'OME-Zarr chunk keys must contain exactly five non-negative '
            'integer coordinates'
        )
    return tuple(int(value) for value in chunk_name.split('.'))


def get_chunk_grid_shape(dataset_shape, chunk_size):
    """Return the number of addressable chunks along each array dimension."""
    dataset_shape = tuple(int(value) for value in dataset_shape)
    chunk_size = tuple(int(value) for value in chunk_size)
    if len(dataset_shape) != CHUNK_DIMENSIONS or len(chunk_size) != CHUNK_DIMENSIONS:
        raise ValueError('OME-Zarr arrays and chunks must be five-dimensional')
    if any(value < 0 for value in dataset_shape):
        raise ValueError('OME-Zarr array dimensions cannot be negative')
    if any(value <= 0 for value in chunk_size):
        raise ValueError('OME-Zarr chunk dimensions must be positive')
    return tuple(
        math.ceil(shape / chunk)
        for shape, chunk in zip(dataset_shape, chunk_size)
    )


def chunk_coordinates_in_bounds(chunk_coordinates, dataset_shape, chunk_size):
    """Return whether a chunk coordinate is inside the advertised chunk grid."""
    if len(chunk_coordinates) != CHUNK_DIMENSIONS:
        return False
    grid_shape = get_chunk_grid_shape(dataset_shape, chunk_size)
    return all(
        0 <= coordinate < grid_size
        for coordinate, grid_size in zip(chunk_coordinates, grid_shape)
    )

# A custom chunk token is placed between extensions, for example
# ``sample.32x128x128.ims.ome.zarr``. Three, four, and five dimensional
# variants are supported and expanded to the endpoint's (t, c, z, y, x)
# convention.
custom_chunk_pattern = re.compile(
    r'(?<=\.)([0-9]+(?:x[0-9]+){2,4})(?=\.)'
)
MAX_CUSTOM_CHUNK_BYTES = 256 * 1024 * 1024


def extract_custom_chunk_size(datapath):
    """Remove a custom chunk token from a path and return its 5-D shape."""
    chunk_match = custom_chunk_pattern.search(datapath)
    if chunk_match is None:
        return datapath, None

    chunk_size = tuple(
        int(value) for value in chunk_match.group(1).split('x')
    )
    if any(value <= 0 for value in chunk_size):
        raise ValueError('Custom chunk dimensions must be positive integers')

    if len(chunk_size) == 3:
        chunk_size = (1, 1, *chunk_size)
    elif len(chunk_size) == 4:
        chunk_size = (1, *chunk_size)
    elif len(chunk_size) != 5:
        raise ValueError('Custom chunk size must have 3, 4, or 5 dimensions')

    # Remove the token and the dot immediately following it, preserving the
    # preceding dot: ``sample.32x128x128.ims`` -> ``sample.ims``.
    datapath = datapath[:chunk_match.start()] + datapath[chunk_match.end() + 1:]
    return datapath, chunk_size


def validate_custom_chunk_size(chunk_size, dtype):
    """Reject custom chunks that could allocate excessive memory per request."""
    chunk_bytes = math.prod(chunk_size) * np.dtype(dtype).itemsize
    if chunk_bytes > MAX_CUSTOM_CHUNK_BYTES:
        raise ValueError(
            f'Custom chunk requires {chunk_bytes} bytes; maximum is '
            f'{MAX_CUSTOM_CHUNK_BYTES} bytes'
        )

# OME-Zarr extensions to enable arbitrary use of extension for ome zarr compatibility in various apps
# NOTE: .ng.ome.zarr causes a change in the represented chunk size by combining channels into each chunk using func chunks_combine_channels
exts = ['.ng.ome.zarr','.ome.zarr','.omezarr','.ome.ngff','.ngff']
VIRTUAL_OMEZARR_SOURCE_EXTENSIONS = (
    '.ims', '.zarr', '.omezans', '.omehans', '.terafly',
    '.nii', '.nii.gz', '.nii.zarr', '.tif', '.tiff', '.ome.tif',
    '.ome.tiff', '.ome-tif', '.ome-tiff', '.jp2', '.nd2',
)


def strip_virtual_omezarr_extension(datapath):
    """Remove a viewer suffix while preserving a real ``.ome.zarr`` path."""
    for ext in exts:
        idx = datapath.rfind(ext)
        if idx < 0:
            continue
        candidate_end = idx + len(ext)
        if candidate_end != len(datapath) and datapath[candidate_end] != '/':
            continue
        if ext == '.ome.zarr':
            source_prefix = datapath[:idx].lower()
            if not source_prefix.endswith(VIRTUAL_OMEZARR_SOURCE_EXTENSIONS):
                # This is the dataset's real extension, not a virtual viewer
                # extension appended to another supported source path.
                continue
        return (
            datapath[:idx] + datapath[candidate_end:],
            ext == '.ng.ome.zarr',
        )
    return datapath, False


def get_omezarr_resource_parts(req_path):
    """Return the logical path below the virtual OME-Zarr store root.

    The last recognized virtual extension is used because a source dataset may
    itself have an OME-Zarr extension before the endpoint's virtual extension.
    """
    extension_end = None
    for ext in exts:
        search_end = len(req_path)
        index = req_path.rfind(ext, 0, search_end)
        while index >= 0:
            candidate_end = index + len(ext)
            if candidate_end == len(req_path) or req_path[candidate_end] == '/':
                if extension_end is None or candidate_end > extension_end:
                    extension_end = candidate_end
                break
            search_end = index
            index = req_path.rfind(ext, 0, search_end)

    if extension_end is None:
        return None

    relative_path = req_path[extension_end:].strip('/')
    if not relative_path:
        return ()
    return tuple(part for part in relative_path.split('/') if part)


def parse_chunk_resource(resource_parts):
    """Parse a dotted or slash-separated chunk resource below a resolution."""
    if resource_parts is None or len(resource_parts) < 2:
        return None
    try:
        resolution = int(resource_parts[0])
    except (TypeError, ValueError):
        return None
    if resolution < 0:
        return None

    if len(resource_parts) == 2:
        chunk_name = resource_parts[1]
    elif len(resource_parts) == CHUNK_DIMENSIONS + 1:
        chunk_name = '.'.join(resource_parts[1:])
    else:
        return None

    try:
        coordinates = parse_chunk_name(chunk_name)
    except ValueError:
        return None
    return resolution, chunk_name, coordinates


def resolution_exists(metadata, resolution):
    """Return whether metadata describes the requested resolution array."""
    if not isinstance(resolution, int) or resolution < 0:
        return False
    return (
        (resolution, 0, 0, 'shape') in metadata
        and (resolution, 0, 0, 'chunks') in metadata
    )


def get_dataset_datapath(datapath, resource_parts):
    """Remove the logical OME-Zarr resource suffix from a mapped data path."""
    if resource_parts is None:
        raise ValueError('Request does not contain a recognized OME-Zarr extension')
    for _ in resource_parts:
        datapath = os.path.split(datapath)[0]
    return datapath


def setup_omezarr(app, config):
    """
    Setup the Flask routes to serve OME-Zarr datasets.

    This function defines and registers a Flask route that inspects the requested URL path,
    processes any special extensions (such as chunk size definitions or Neuroglancer flags),
    and then returns the appropriate data (a chunk, .zarray, .zattrs, or .zgroup file)
    as a response.

    The inner function `omezarr_entry` is decorated with CORS support and registered as a route.

    Parameters:
        app: The Flask application instance.
        config: A configuration object that provides dataset loading, caching, and metadata.

    Returns:
        The Flask application instance with the OME-Zarr route configured.
    """
    # Establish highly used functions as objects to improve speed
    get_html_split_and_associated_file_path = utils.get_html_split_and_associated_file_path

    def omezarr_entry(req_path):
        """
        Inner route function to handle requests for OME-Zarr resources.

        Determines if the request is for a chunk, .zarray, .zattrs, or .zgroup, then processes and
        returns the appropriate response.

        Parameters:
            req_path (str): The remaining path requested after the base route.

        Returns:
            A Flask Response object containing the data or JSON metadata.
        """
        resource_parts = get_omezarr_resource_parts(req_path)
        path_split, datapath = get_html_split_and_associated_file_path(config,request)

        # logger.info(path_split)
        logger.info(f'requested path: {req_path}')

        # Flag to deliver 8bit data to NG
        force8Bit = False
        # if '.8.' in datapath or '.8bit.' in datapath:
        #     logger.warning('Forcing 8bit data delivery')
        #     force8Bit = True
        #     datapath = datapath.replace('.8.', '.')
        #     datapath = datapath.replace('.8bit.', '.')
        if '.8bit.' in datapath:
            force8Bit = True
            # datapath = datapath.replace('.8.', '.')
            datapath = datapath.replace('.8bit.', '.')

        # Attempt to enable ome.zarr ext for compatibility with omezarr utils
        isNeuroGlancer = False
        # for ext in exts:
        #     if len(datapath.split(ext)) > 1:
        #         datapath = datapath.replace(ext,'',1)
        #         if ext == '.ng.ome.zarr': isNeuroGlancer = True
        #         # logger.info(f'DATAPATH MINUS EXT: {datapath}')
        #         break
        datapath, isNeuroGlancer = strip_virtual_omezarr_extension(datapath)

        # A token such as ``.32x128x128.`` requests a virtual chunk shape.
        # Keep it separate from the effective size so metadata and data reads
        # can apply the same Neuroglancer channel-combining rule later.
        try:
            datapath, custom_chunk_size = extract_custom_chunk_size(datapath)
        except ValueError as exc:
            abort(400, description=str(exc))



        logger.info(f'path_split: {path_split}')
        logger.info(f'datapath: {datapath}')
        # Find the file system path to the dataset
        # Assumptions are neuroglancer only requests 'info' file or chunkfiles
        # If only the file name is requested this will redirect to a
        chunk_request = parse_chunk_resource(resource_parts)
        if chunk_request is not None:
            resolution, chunk_name, chunk_coordinates = chunk_request

            # Open dataset
            datapath = get_dataset_datapath(datapath, resource_parts)
            try:
                datapath = open_omezarr_dataset(config,datapath)
            except (FileNotFoundError, NotADirectoryError):
                abort(404)
            dataset = config.opendata.get(datapath)
            if dataset is None:
                abort(404)
            if not resolution_exists(dataset.metadata, resolution):
                abort(404)
            dataset_cache_identity = get_dataset_cache_identity(dataset, datapath)

            chunk_size = get_effective_chunk_size(
                dataset.metadata,
                resolution,
                custom_chunk_size=custom_chunk_size,
                combine_channels=isNeuroGlancer,
            )
            if custom_chunk_size is not None:
                try:
                    # Padding happens before optional 8-bit conversion, so the
                    # source dtype determines peak chunk allocation.
                    validate_custom_chunk_size(chunk_size, dataset.dtype)
                except ValueError as exc:
                    abort(400, description=str(exc))

            # Determine where the chunk is in the actual dataset
            # dataset_shape = config.opendata[datapath].metadata[(resolution, 0, 0, 'shape')]
            # logger.info(f"chunk_name, {chunk_name}")
            # logger.info(f"dataset_shape, {dataset_shape}")
            # logger.info(f"chunk_size, {chunk_size}")
            # dataset_shape = config.opendata[datapath].metadata['shape']
            dataset_shape = (dataset.metadata['TimePoints'],dataset.metadata['Channels'],*dataset.metadata[(resolution, 0, 0, 'shape')][-3:])
            if not chunk_coordinates_in_bounds(
                    chunk_coordinates, dataset_shape, chunk_size):
                abort(404)
            locationDict = where_is_that_chunk(chunk_name=chunk_name, dataset_shape=dataset_shape, chunk_size=chunk_size)
            logger.info('Chunk is here:')
            logger.info(locationDict)

            chunk = None
            if config.cache is not None:
                key = (
                    f'omezarr_tczyx-v2_{dataset_cache_identity}-'
                    f'{resolution}-{chunk_name}-{chunk_size}-'
                    f'{isNeuroGlancer}-{force8Bit}'
                )
                chunk = config.cache.get(key, default=None, retry=True)
                logger.info(f'OME-Zarr chunk cache found')

            if chunk is None:
                try:
                    chunk = get_chunk(locationDict,resolution,dataset,chunk_size)
                    if force8Bit:
                        chunk = utils.dtype_to_uint8(chunk)
                    chunk = pad_chunk(chunk, chunk_size)
                    chunk = compress_zarr_chunk(chunk,compressor=get_compressor())
                except Exception as exc:
                    logger.exception(
                        f'Failed to read source chunk {chunk_name} at '
                        f'resolution {resolution} from {datapath}'
                    )
                    return jsonify({
                        'error': 'Failed to read source dataset chunk',
                        'resolution': resolution,
                        'chunk': chunk_name,
                        'detail': str(exc),
                    }), 502

                # Add to cache
                if config.cache is not None:
                    # a = dask.delayed(config.cache.set)(key, chunk, expire=None, tag=datapath, retry=True)

                    config.cache.set(key, chunk, expire=None, tag=dataset_cache_identity, retry=True)
                    logger.info(f'Ome-Zarr chunk cache saved')
                # Flask return of bytesIO as file

            return Response(response=chunk, status=200,
                            mimetype="application/octet-stream")


        elif resource_parts == ('labels',):
            abort(404)
        elif resource_parts is not None and resource_parts[-1:] == ('.zarray',):
            if len(resource_parts) != 2 or resource_parts[0] == 'labels':
                abort(404)
            try:
                resolution = int(resource_parts[0])
            except (TypeError, ValueError):
                abort(404)
            if resolution < 0:
                abort(404)
            datapath = get_dataset_datapath(datapath, resource_parts)
            try:
                datapath = open_omezarr_dataset(config,datapath)
            except (FileNotFoundError, NotADirectoryError):
                abort(404)
            dataset = config.opendata.get(datapath)
            if dataset is None:
                abort(404)
            if not resolution_exists(dataset.metadata, resolution):
                abort(404)
            if custom_chunk_size is not None:
                chunk_size = get_effective_chunk_size(
                    dataset.metadata,
                    resolution,
                    custom_chunk_size=custom_chunk_size,
                    combine_channels=isNeuroGlancer,
                )
                try:
                    validate_custom_chunk_size(chunk_size, dataset.dtype)
                except ValueError as exc:
                    abort(400, description=str(exc))
            return jsonify(get_zarray_file(
                dataset,
                resolution,
                combine_channels=isNeuroGlancer,
                force8Bit=force8Bit,
                custom_chunk_size=custom_chunk_size,
            ))
        elif resource_parts is not None and resource_parts[-1:] == ('.zmetadata',):
            if resource_parts != ('.zmetadata',):
                abort(404)
            datapath = get_dataset_datapath(datapath, resource_parts)
            try:
                datapath = open_omezarr_dataset(config,datapath)
            except (FileNotFoundError, NotADirectoryError):
                abort(404)
            dataset = config.opendata.get(datapath)
            if dataset is None:
                abort(404)
            if custom_chunk_size is not None:
                for resolution in get_resolution_levels(dataset.metadata):
                    chunk_size = get_effective_chunk_size(
                        dataset.metadata,
                        resolution,
                        custom_chunk_size=custom_chunk_size,
                        combine_channels=isNeuroGlancer,
                    )
                    try:
                        validate_custom_chunk_size(chunk_size, dataset.dtype)
                    except ValueError as exc:
                        abort(400, description=str(exc))
            return jsonify(get_zmetadata_file(
                dataset,
                combine_channels=isNeuroGlancer,
                force8Bit=force8Bit,
                custom_chunk_size=custom_chunk_size,
            ))
        elif resource_parts is not None and resource_parts[-1:] == ('.zattrs',):
            if resource_parts != ('.zattrs',):
                abort(404)
            datapath = get_dataset_datapath(datapath, resource_parts)
            try:
                datapath = open_omezarr_dataset(config,datapath)
            except (FileNotFoundError, NotADirectoryError):
                abort(404)
            dataset = config.opendata.get(datapath)
            if dataset is None:
                abort(404)
            return jsonify(get_zattr_file(dataset,force8Bit=force8Bit))
        elif resource_parts is not None and resource_parts[-1:] == ('.zgroup',):
            if resource_parts != ('.zgroup',):
                abort(404)
            datapath = get_dataset_datapath(datapath, resource_parts)
            try:
                datapath = open_omezarr_dataset(config,datapath)
            except (FileNotFoundError, NotADirectoryError):
                abort(404)
            if config.opendata.get(datapath) is None:
                abort(404)
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
    
