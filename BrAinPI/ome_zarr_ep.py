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
import json
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
    Response,
)

from flask_login import login_required
from flask_cors import cross_origin


import utils


def where_is_that_chunk(
    chunk_name="0.0.1.3.14",
    dataset_shape=(1, 1, 2, 23857, 14623),
    chunk_size=(1, 1, 1, 1000, 1000),
):
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
    t, c, z, y, x = (int(x) for x in chunk_name.split("."))

    location = {}

    location["tStart"] = t * chunk_size[0]
    t = location["tStart"] + chunk_size[0]
    location["tStop"] = t if t <= dataset_shape[0] else dataset_shape[0]

    location["cStart"] = c * chunk_size[1]
    c = location["cStart"] + chunk_size[1]
    location["cStop"] = c if c <= dataset_shape[1] else dataset_shape[1]

    location["zStart"] = z * chunk_size[2]
    z = location["zStart"] + chunk_size[2]
    location["zStop"] = z if z <= dataset_shape[2] else dataset_shape[2]

    location["yStart"] = y * chunk_size[3]
    y = location["yStart"] + chunk_size[3]
    location["yStop"] = y if y <= dataset_shape[3] else dataset_shape[3]

    location["xStart"] = x * chunk_size[4]
    x = location["xStart"] + chunk_size[4]
    location["xStop"] = x if x <= dataset_shape[4] else dataset_shape[4]

    return location


def get_chunk(locationDict, res, dataset, chunk_size):
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
        slice(locationDict["tStart"], locationDict["tStop"]),
        slice(locationDict["cStart"], locationDict["cStop"]),
        slice(locationDict["zStart"], locationDict["zStop"]),
        slice(locationDict["yStart"], locationDict["yStop"]),
        slice(locationDict["xStart"], locationDict["xStop"]),
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
    if chunk.shape == chunk_size:
        return chunk

    canvas = np.zeros(chunk_size, dtype=chunk.dtype)
    if len(chunk.shape) == 5:
        canvas[
            0 : chunk.shape[0],
            0 : chunk.shape[1],
            0 : chunk.shape[2],
            0 : chunk.shape[3],
            0 : chunk.shape[4],
        ] = chunk
    elif len(chunk.shape) == 4:
        canvas[
            0 : chunk.shape[0],
            0 : chunk.shape[1],
            0 : chunk.shape[2],
            0 : chunk.shape[3],
        ] = chunk
    elif len(chunk.shape) == 3:
        canvas[0 : chunk.shape[0], 0 : chunk.shape[1], 0 : chunk.shape[2]] = chunk
    elif len(chunk.shape) == 2:
        canvas[0 : chunk.shape[0], 0 : chunk.shape[1]] = chunk
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
    return Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE, blocksize=0)


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


def compress_zarr_chunk(np_array, compressor=get_compressor()):
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
    buf = np_array.tobytes("C")
    if compressor is not None:
        buf = compressor.encode(buf)
    img_ram = io.BytesIO()
    img_ram.write(buf)
    img_ram.seek(0)

    return img_ram


def chunks_combine_channels(metadata, resolution_level):
    """
    Assumes 5 dimensions and replaces channel dim (t,c,z,y,x) with the number of channels.
    This is good for neuroglancer compatibility because it enables all channels to be delivered with each chunk
    enabling a shader to mix all channels into RGB representation

    Parameters:
        metadata (dict): Metadata dictionary extracted from the dataset.
        resolution_level (int): The resolution level for which the chunk metadata is extracted.

    Returns:
        tuple: The new chunk size tuple with channels combined.
    """

    chunks = list(metadata[(resolution_level, 0, 0, "chunks")])
    chunks[1] = metadata["Channels"]
    return tuple(chunks)


def get_zarray_file(
    numpy_like_dataset, resolution_level, combine_channels=False, force8Bit=False
):
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

    Returns:
        dict: A dictionary representing the zarray metadata.
    """
    metadata = utils.metaDataExtraction(numpy_like_dataset, strKey=False)

    zarray = {}
    if combine_channels:
        zarray["chunks"] = chunks_combine_channels(metadata, resolution_level)
    else:
        zarray["chunks"] = metadata[(resolution_level, 0, 0, "chunks")]

    compressor = get_compressor()
    zarray["compressor"] = {}
    zarray["compressor"]["blocksize"] = compressor.blocksize
    zarray["compressor"]["clevel"] = compressor.clevel
    zarray["compressor"]["cname"] = compressor.cname
    zarray["compressor"]["id"] = compressor.codec_id
    zarray["compressor"]["shuffle"] = compressor.shuffle

    # zarray['dimension_separator'] = '.'
    zarray["dimension_separator"] = (
        "/"  # <-- required for compatibility with ome-zarr 4.0
    )
    if force8Bit:
        zarray["dtype"] = encoding_values["uint8"]
    else:
        zarray["dtype"] = encoding_values[
            metadata[(0, 0, 0, "dtype")]
        ]  #'<u2' if numpy_like_dataset.dtype == np.uint16 else '<u2'  ## Need to figure out other dtype opts for uint8, float et al
    zarray["fill_value"] = 0
    zarray["filters"] = None
    zarray["order"] = "C"
    zarray["shape"] = (
        metadata["TimePoints"],
        metadata["Channels"],
        *metadata[(resolution_level, 0, 0, "shape")][-3:],
    )
    zarray["zarr_format"] = 2

    return zarray


colors = [
    "00FF00",  # green
    "FF0000",  # red
    "FF00FF",  # purple
    "0000FF",  # blue
    "FFFF00",  # yellow
]

values = {
    np.dtype("uint8"): (0, 255),
    np.dtype("uint16"): (0, 65535),
    np.dtype("float"): (0, 1),
    np.dtype("float32"): (0, 1),
    np.dtype("float64"): (0, 1),
    "uint8": (0, 255),
    "uint16": (0, 65535),
    "float": (0, 1),
    "float32": (0, 1),
    "float64": (0, 1),
}

value_types = {
    np.dtype("uint8"): "uint8",
    np.dtype("uint16"): "uint16",
    np.dtype("float"): float,
    np.dtype("float32"): float,
    np.dtype("float64"): float,
    "uint8": "uint8",
    "uint16": "uint16",
    "float": float,
    "float32": float,
    "float64": float,
}

encoding_values = {
    np.dtype("uint8"): "|u1",
    np.dtype("uint16"): "<u2",
    np.dtype("float"): "<f4",
    np.dtype("float32"): "<f4",
    np.dtype("float64"): "<f8",
    "uint8": "|u1",
    "uint16": "<u2",
    "float": "<f4",
    "float32": "<f4",
    "float64": "<f8",
}

max_values = {
    np.dtype("uint8"): 254,
    np.dtype("uint16"): 65534,
    np.dtype("float"): 1,
    np.dtype("float32"): 1,
    np.dtype("float64"): 1,
    "uint8": 254,
    "uint16": 65534,
    "float": 1,
    "float32": 1,
    "float64": 1,
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


def conv_dtype_value(value, fdtype, tdtype):
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
    ratio = max_values[tdtype] / max_values[fdtype]
    return round(ratio * value)


#####################################
### OME-NGFF 0.4 Complient .zattr ###
#####################################


def get_zattr_file(numpy_like_dataset, force8Bit=False):
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
    metadata = utils.metaDataExtraction(numpy_like_dataset, strKey=False)
    # metadata = metaDataExtraction(numpy_like_dataset,strKey=False)

    zattr = {}

    ### Build creator info ###
    zattr["_creator"] = {"name": "BrAinPI", "version": "0.3.0"}

    ###################
    ### MULTISCALES ###
    ###################
    axes = [
        {"name": "t", "type": "time", "unit": "second"},
        {"name": "c", "type": "channel"},
        {"name": "z", "type": "space", "unit": "micrometer"},
        {"name": "y", "type": "space", "unit": "micrometer"},
        {"name": "x", "type": "space", "unit": "micrometer"},
    ]

    datasets = []
    for res in range(metadata["ResolutionLevels"]):
        if res == 0:
            translation = (0, 0, 0, 0, 0)
        else:
            # Proportional difference in spacing between current and previous scale
            translation = [
                x / y
                for x, y in zip(
                    metadata[(res, 0, 0, "resolution")],
                    metadata[(res - 1, 0, 0, "resolution")],
                )
            ]
            # Determine micron shift in origin by dividing current resoluton by proportional difference
            translation = [
                x / y if y != 1 else 0
                for x, y in zip(metadata[(res, 0, 0, "resolution")], translation)
            ]
            # translation = [x if x!=1 else 0 for x in translation]
            translation = (0, 0, *translation)

            # # I think we can just pass the previous resolution here (need to test carefully)
            # translation = metadata[(res-1, 0, 0, 'resolution')]
            # translation = (0, 0, *translation)
        level = {
            "path": str(res),
            "coordinateTransformations": [
                {
                    "scale": (1, 1, *metadata[(res, 0, 0, "resolution")]),
                    "type": "scale",
                },
                {
                    "translation": translation,  # Spatial units, same as scale
                    "type": "translation",
                },
            ],
        }
        datasets.append(level)

    zattr["multiscales"] = [
        {
            "axes": axes,
            "datasets": datasets,
            "version": "0.4",
            "type": "gaussian",
            "metadata": {
                "description": "Describe how multiscale was created",
                "method": "The function used to create",
                "version": "version of method",
                "other": "stuff",
                "other2": "stuff2",
            },
        }
    ]

    colors * math.ceil(metadata["Channels"] / len(colors))

    # lowest_res_level = numpy_like_dataset[metadata['ResolutionLevels']-1,0,0,:,:,:]

    #############
    ### OMERO ###
    #############
    channels = []
    for ch in range(metadata["Channels"]):
        current_channel_data = numpy_like_dataset[
            metadata["ResolutionLevels"] - 1, 0, ch, :, :, :
        ]
        dtype = current_channel_data.dtype

        end = (
            int(current_channel_data.max())
            if not value_types[dtype] == float
            else float(current_channel_data.max())
        )
        start = (
            int(current_channel_data.min())
            if not value_types[dtype] == float
            else float(current_channel_data.min())
        )

        if force8Bit:
            end = conv_dtype_value(end, dtype, "uint8")
            start = conv_dtype_value(start, dtype, "uint8")

        if value_types[dtype] == float and (
            (end > 1 or end < 1) or (start < 1 or start > 1)
        ):  # <---  May need to look at this more closely
            if end > 1:
                max_window = end * 2
            if end < 1:
                max_window = end / 2
            if start > 1:
                min_window = start / 2
            if start <= 0:
                min_window = start * 2

        else:
            max_window = end * 2 if end * 2 <= values[dtype][1] else values[dtype][1]
            min_window = (
                start // 2 if start // 2 >= values[dtype][0] else values[dtype][0]
            )

            if force8Bit:
                max_window = conv_dtype_value(max_window, dtype, "uint8")
                min_window = conv_dtype_value(min_window, dtype, "uint8")

        channel = {
            "active": True,
            "coefficient": 1.0,
            "color": colors[ch],
            "family": "linear",
            "inverted": False,
            "label": "Channel_{}".format(ch),
            "window": {
                "end": end,
                "max": max_window,
                "min": min_window,
                "start": start,
            },
        }

        channels.append(channel)

    zattr["omero"] = {
        "id": 1,
        "name": "Need to add file name.ext",
        "version": "0.5-dev",
        "channels": channels,
        "rdefs": {
            "defaultT": 0,  # Default timepoint to display
            "defaultZ": metadata[(0, 0, 0, "shape")][2]
            // 2,  # Default z-layer to display
            "model": "color",  #'color' or 'greyscale'
        },
    }

    return zattr


def open_omezarr_dataset(config, datapath):
    """
    Open an OME-Zarr dataset using the provided configuration.

    This function calls a loader on the configuration object to load the dataset.
    It may also perform additional setup for Neuroglancer functionality if needed.

    Parameters:
        config: A configuration object that contains methods to load the dataset.
        datapath (str): The file system path to the dataset.

    Returns:
        The loaded dataset (the exact type depends on the configuration's implementation).
    """
    datapath = config.loadDataset(datapath, datapath)

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


file_name_template = "{}.{}.{}.{}.{}"
file_pattern = file_name_template.format(
    "[0-9]+", "[0-9]+", "[0-9]+", "[0-9]+", "[0-9]+", "[0-9]+"
)

# OME-Zarr extensions to enable arbitrary use of extension for ome zarr compatibility in various apps
# NOTE: .ng.ome.zarr causes a change in the represented chunk size by combining channels into each chunk using func chunks_combine_channels
exts = [".ng.ome.zarr", ".ome.zarr", ".omezarr", ".ome.ngff", ".ngff"]

chunks_size_pattern = "^.*[0-9]+x[0-9]+x[0-9]+.*$"


def get_omezarr_request_info(config, request_path):
    """Normalize an OME-Zarr request path into dataset metadata."""
    path_map = utils.get_path_map(config.settings, user_authenticated=True)
    path_split = utils.split_html(request_path)
    datapath = utils.from_html_to_path(request_path, path_map)

    try:
        new_path = path_split[-5:]
        if all(isinstance(int(x), int) for x in new_path):
            request_path = "/" + os.path.join(*path_split[:-5], ".".join(new_path))
            path_split = utils.split_html(request_path)
            datapath = utils.from_html_to_path(request_path, path_map)
    except Exception:
        pass

    force8Bit = False
    if ".8." in datapath or ".8bit." in datapath:
        force8Bit = True
        datapath = datapath.replace(".8.", ".")
        datapath = datapath.replace(".8bit.", ".")

    isNeuroGlancer = False
    for ext in exts:
        if len(datapath.split(ext)) > 1:
            datapath = datapath.replace(ext, "")
            if ext == ".ng.ome.zarr":
                isNeuroGlancer = True
            break

    requested_chunk_size = None
    match = re.match
    Match_class = re.Match
    if match(chunks_size_pattern, datapath):
        reverse_split = datapath.split(".")[::-1]
        any_matches = tuple(
            (
                x
                for x in reverse_split
                if isinstance(match(chunks_size_pattern, x), Match_class)
            )
        )

        if len(any_matches) > 0:
            for ii in any_matches:
                chunk = ii.split("x")
                requested_chunk_size = tuple((int(x) for x in chunk))
                if len(requested_chunk_size) == 3:
                    requested_chunk_size = (1, 1, *requested_chunk_size)
                elif len(requested_chunk_size) == 4:
                    requested_chunk_size = (1, *requested_chunk_size)
                elif len(requested_chunk_size) == 2:
                    requested_chunk_size = (1, 1, 1, *requested_chunk_size)

                datapath = datapath.replace(f".{ii}.", ".")
                break

    return {
        "request_path": request_path,
        "path_split": path_split,
        "datapath": datapath,
        "force8Bit": force8Bit,
        "isNeuroGlancer": isNeuroGlancer,
        "requested_chunk_size": requested_chunk_size,
    }


def get_omezarr_object(config, request_path):
    """Return bytes and mimetype for a virtual OME-Zarr object."""
    request_info = get_omezarr_request_info(config, request_path)
    path_split = request_info["path_split"]
    datapath = request_info["datapath"]
    force8Bit = request_info["force8Bit"]
    isNeuroGlancer = request_info["isNeuroGlancer"]

    match = re.match
    Match_class = re.Match

    if isinstance(match(file_pattern, path_split[-1]), Match_class):
        chunk_name = path_split[-1]
        resolution = int(datapath.split("/")[-2])

        datapath = os.path.split(datapath)[0]
        datapath = os.path.split(datapath)[0]
        datapath = open_omezarr_dataset(config, datapath)
        config.opendata[datapath].metadata

        if isNeuroGlancer:
            chunk_size = chunks_combine_channels(
                config.opendata[datapath].metadata, resolution
            )
        else:
            chunk_size = config.opendata[datapath].metadata[
                (resolution, 0, 0, "chunks")
            ]

        dataset_shape = (
            config.opendata[datapath].metadata["TimePoints"],
            config.opendata[datapath].metadata["Channels"],
            *config.opendata[datapath].metadata[(resolution, 0, 0, "shape")][-3:],
        )
        locationDict = where_is_that_chunk(
            chunk_name=chunk_name,
            dataset_shape=dataset_shape,
            chunk_size=chunk_size,
        )

        chunk = None
        if config.cache is not None:
            key = f"omezarr_{locationDict}-{resolution}-{chunk_size}-{isNeuroGlancer}-{force8Bit}"
            chunk = config.cache.get(key, default=None, retry=True)

        if chunk is None:
            chunk = get_chunk(
                locationDict, resolution, config.opendata[datapath], chunk_size
            )
            chunk = pad_chunk(chunk, chunk_size)
            if force8Bit:
                chunk = utils.conv_np_dtypes(chunk, "uint8")
            chunk = compress_zarr_chunk(chunk, compressor=get_compressor()).getvalue()

            if config.cache is not None:
                config.cache.set(key, chunk, expire=None, tag=datapath, retry=True)

        return chunk, "application/octet_stream"

    if path_split[-1] == "labels":
        raise FileNotFoundError(request_path)

    if path_split[-1] == ".zarray":
        if path_split[-2] == "labels":
            raise FileNotFoundError(request_path)
        try:
            resolution = int(datapath.split("/")[-2])
            datapath = os.path.split(datapath)[0]
            datapath = os.path.split(datapath)[0]
            datapath = open_omezarr_dataset(config, datapath)
            payload = get_zarray_file(
                config.opendata[datapath],
                resolution,
                combine_channels=isNeuroGlancer,
                force8Bit=force8Bit,
            )
            return json.dumps(payload, separators=(",", ":")).encode(
                "utf-8"
            ), "application/json"
        except Exception as exc:
            raise FileNotFoundError(request_path) from exc

    if path_split[-1] == ".zattrs":
        if path_split[-2] == "labels":
            raise FileNotFoundError(request_path)
        datapath = os.path.split(datapath)[0]
        datapath = open_omezarr_dataset(config, datapath)
        payload = get_zattr_file(config.opendata[datapath], force8Bit=force8Bit)
        return json.dumps(payload, separators=(",", ":")).encode(
            "utf-8"
        ), "application/json"

    if path_split[-1] == ".zgroup":
        return json.dumps({"zarr_format": 2}, separators=(",", ":")).encode(
            "utf-8"
        ), "application/json"

    raise FileNotFoundError(request_path)


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
        try:
            body, mimetype = get_omezarr_object(config, request.path)
            return Response(response=body, status=200, mimetype=mimetype)
        except FileNotFoundError:
            abort(404)

        #####  FUNCTION END  ######

    zarrpath = "/omezarr/"  # <--- final slash is required for proper navigation through dir tree

    # Decorating neuro_glancer_entry to allow caching ##
    # if config.cache is not None:
    #     logger.info('Caching setup')
    #     omezarr_entry = config.cache.memoize()(omezarr_entry)
    #     logger.info(omezarr_entry)
    # neuro_glancer_entry = login_required(neuro_glancer_entry)

    omezarr_entry = cross_origin(allow_headers=["Content-Type"])(omezarr_entry)
    # omezarr_entry = login_required(omezarr_entry)
    omezarr_entry = app.route(zarrpath + "<path:req_path>")(omezarr_entry)
    omezarr_entry = app.route(zarrpath, defaults={"req_path": ""})(omezarr_entry)

    return app


"""
OME-NGFF Examples

Multi channel, multiscale, Cells:
    napari --plugin napari-ome-zarr https://uk1s3.embassy.ebi.ac.uk/idr/share/gbi2022/6001237/idr.zarr
    https://uk1s3.embassy.ebi.ac.uk/idr/share/gbi2022/6001237/idr.zarr/.zattrs
    https://uk1s3.embassy.ebi.ac.uk/idr/share/gbi2022/6001237/idr.zarr/0/.zarray
    
Single channel, multiscale, EM:
    napari --plugin napari-ome-zarr https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.1/4495402.zarr
    https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.1/4495402.zarr/.zattrs
    https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.1/4495402.zarr/0/.zarray
    
"""


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
