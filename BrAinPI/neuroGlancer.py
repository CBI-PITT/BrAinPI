# -*- coding: utf-8 -*-
"""Generate Neuroglancer state, metadata, shaders, and precomputed chunks.

Image loaders are exposed as Neuroglancer precomputed volumes. Native
annotation and segmentation ``.pcd`` datasets are detected and passed through.
Float16 and float64 source chunks are encoded as float32 to match the advertised
precomputed data type.
"""
# bil_api imports
from itertools import product
from contextlib import nullcontext
import io
import json
import re
from urllib.parse import quote
from neuroglancer_scripts.chunk_encoding import RawChunkEncoder
import numpy as np
import os
from logger_tools import logger
import cv2
## Project imports
import utils
from utils import compress_flask_response
import config_tools
from flask import (
    render_template,
    request,
    send_file,
    redirect,
    jsonify,
    make_response,
    Response,
)

from flask_cors import cross_origin


DEFAULT_LUT_PERCENTILES = (1, 99)


def _percentile_display_range(values, percentiles=DEFAULT_LUT_PERCENTILES):
    """
    Estimate a stable display range from sampled data for Neuroglancer LUTs.
    """
    values = np.asarray(values)
    if values.dtype.fields:
        if len(values.dtype.fields) != 1:
            raise TypeError(
                "Display range calculation requires a numeric array or a "
                "single-field structured array."
            )
        values = values[next(iter(values.dtype.fields))]
    if not np.issubdtype(values.dtype, np.number):
        raise TypeError(f"Display range calculation does not support dtype {values.dtype}")
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0, 1.0

    positive = finite[finite > 0]
    working = positive if positive.size else finite
    low, high = np.percentile(working, percentiles)
    low = float(low)
    high = float(high)
    if high <= low:
        high = low + 1.0
    return low, high


def encode_ng_file(numpy_array, channels):
    """
    Encode a numpy array into a Neuroglancer-compatible chunk format.

    Args:
        numpy_array (np.ndarray): The array to encode.
        channels (int): Number of channels in the array.

    Returns:
        io.BytesIO: The encoded chunk as a memory buffer.
    """
    if numpy_array.dtype == np.float16 or numpy_array.dtype == np.float64:
        numpy_array = numpy_array.astype(np.float32, copy=False)

    encoder = RawChunkEncoder(numpy_array.dtype, channels)
    img_ram = io.BytesIO()
    img_ram.write(encoder.encode(numpy_array))
    img_ram.seek(0)
    return img_ram


def ng_shader(numpy_like_object):
    """
    Generate a dynamic Neuroglancer shader string based on dataset metadata.

    Args:
        numpy_like_object: An object containing metadata and resolution levels.

    Returns:
        str: The Neuroglancer shader string.
    """
    # metadata should have been appended to object during opening utils.config.loadDataset
    metadata = numpy_like_object.metadata
    try:
        # Determine if the object has omero metadata (ome.zarr .zattrs)
        omero = numpy_like_object.omero
    except Exception:
        omero = None

    res = numpy_like_object.ResolutionLevels

    # Extract values for setting LUTs in proper range
    # User omero values if they exist otherwise determine from lowest resolution multiscale
    if metadata["ndim"] == 6 or metadata.get("packed_rgb", False):
        logger.info("RGB dataset detected, using RGB shader")
        rgb_labels = ("red", "green", "blue")
        shaderStr = ""
        for idx, label in enumerate(rgb_labels):
            shaderStr = (
                shaderStr
                + f"#uicontrol bool {label} checkbox(default=true)\n"
            )
        shaderStr = shaderStr + "\n\nvoid main() {\n\n"
        for idx, (label, channel_name) in enumerate(zip(rgb_labels, ("R", "G", "B"))):
            shaderStr = shaderStr + (
                f"  float {channel_name} = {label} ? "
                f"toNormalized(getDataValue({idx})) :0.0;\n"
            )
        shaderStr = shaderStr + f"  vec3 rgb = vec3(R,G,B);\n\n"
        shaderStr = shaderStr + "emitRGB(rgb);\n"

        shaderStr = shaderStr + "}"
        return shaderStr
    
    defaultColors = [
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
    channel_count = min(int(metadata["Channels"]), 7)
    omero_channels = omero.get("channels", []) if isinstance(omero, dict) else []
    # Neuroglancer invlerp ``range`` is the interval mapped to [0, 1] and
    # therefore controls rendered brightness. ``window`` only controls the
    # interval shown by the ECDF UI. Keep the names explicit to avoid swapping
    # these two distinct concepts.
    mappingMins = []
    mappingMaxs = []
    ecdfWindowMins = []
    ecdfWindowMaxs = []
    isVisable = []
    labels = []
    colors = []
    used_labels = set()

    for idx in range(channel_count):
        channel = omero_channels[idx] if idx < len(omero_channels) else {}
        window = channel.get("window", {}) if isinstance(channel, dict) else {}
        required_window_fields = ("min", "max", "start", "end")
        if all(window.get(field) is not None for field in required_window_fields):
            # OMERO start/end define the initial intensity mapping, while
            # min/max provide the wider domain displayed by the ECDF control.
            mappingMins.append(window["start"])
            mappingMaxs.append(window["end"])
            ecdfWindowMins.append(window["min"])
            ecdfWindowMaxs.append(window["max"])
        else:
            lowestResVolume = numpy_like_object[res - 1, 0, idx, :, :, :]
            display_min, display_max = _percentile_display_range(lowestResVolume)
            logger.info(
                f"Neuroglancer channel {idx}: OMERO window unavailable; using "
                f"percentiles {DEFAULT_LUT_PERCENTILES[0]}/"
                f"{DEFAULT_LUT_PERCENTILES[1]} as invlerp mapping range "
                f"[{display_min}, {display_max}]"
            )
            dtype = np.dtype(numpy_like_object.dtype)
            if np.issubdtype(dtype, np.integer):
                dtype_info = np.iinfo(dtype)
                range_min, range_max = dtype_info.min, dtype_info.max
            else:
                finite = np.asarray(lowestResVolume)[
                    np.isfinite(lowestResVolume)
                ]
                range_min = float(finite.min()) if finite.size else display_min
                range_max = float(finite.max()) if finite.size else display_max
            mappingMins.append(display_min)
            mappingMaxs.append(display_max)
            ecdfWindowMins.append(range_min)
            ecdfWindowMaxs.append(range_max)

        isVisable.append(bool(channel.get("active", True)))

        raw_label = str(channel.get("label") or f"channel{idx}").lower()
        label = re.sub(r"[^a-z0-9_]", "_", raw_label).strip("_")
        if not label or label[0].isdigit():
            label = f"channel{idx}_{label}".rstrip("_")
        base_label = label
        suffix = 1
        while label in used_labels:
            label = f"{base_label}_{suffix}"
            suffix += 1
        used_labels.add(label)
        labels.append(label)

        color = str(channel.get("color") or "").strip().lstrip("#")
        if not re.fullmatch(r"[0-9a-fA-F]{6}", color):
            colors.append(defaultColors[idx % len(defaultColors)])
        else:
            colors.append("#" + color.upper())
    shaderStr = ""
    # shaderStr = shaderStr + '// Init for each channel:\n\n'
    # shaderStr = shaderStr + '// Channel visability check boxes\n'

    for idx in range(channel_count):
        shaderStr = (
            shaderStr
            + f"#uicontrol bool {labels[idx]}_visable checkbox(default={str(isVisable[idx]).lower()});\n"
        )
        # if metadata["Channels"] > 7:
        #     break
    shaderStr = shaderStr + "\n"

    # shaderStr = shaderStr + '\n// Lookup tables\n'
    for idx in range(channel_count):
        shaderStr = (
            shaderStr
            + f"#uicontrol invlerp {labels[idx]}_lut (range=[{mappingMins[idx]},{mappingMaxs[idx]}],window=[{ecdfWindowMins[idx]},{ecdfWindowMaxs[idx]}]"
        )
        if channel_count > 1:
            shaderStr = shaderStr + f",channel=[{idx}]);\n"
        else:
            shaderStr = shaderStr + ");\n"
        shaderStr = (
            shaderStr
            + f"#uicontrol float {labels[idx]}_gamma slider(min=0, max=5, step=0.01, default=1)"
        )
        shaderStr = shaderStr + ";\n"
        # if metadata["Channels"] > 7:
        #     break

    shaderStr = shaderStr + "\n"
    # shaderStr = shaderStr + '\n// Colors\n'

    for idx in range(channel_count):
        shaderStr = (
            shaderStr
            + f'#uicontrol vec3 {labels[idx]}_color color(default="{colors[idx]}");\n'
        )
        # if metadata["Channels"] > 7:
        #     break

    shaderStr = shaderStr + "\n"
    # shaderStr = shaderStr + '\n//RGB vector at 0 (ie channel off)\n'

    for idx in range(channel_count):
        shaderStr = shaderStr + f"vec3 {labels[idx]} = vec3(0);\n"
        # if metadata["Channels"] > 7:
        #     break

    shaderStr = shaderStr + "\n\nvoid main() {\n\n"
    # shaderStr = shaderStr + '// For each color, if visable, get data, adjust with lut, then apply to color\n'

    for idx in range(channel_count):
        shaderStr = shaderStr + f"if ({labels[idx]}_visable == true)\n"
        # shaderStr = shaderStr + f'{labels[idx]} = {labels[idx]}_color * ((toNormalized(getDataValue({idx})) + {labels[idx]}_lut()));\n\n'
        shaderStr = (
            shaderStr
            + f"{labels[idx]} = pow({labels[idx]}_color *  {labels[idx]}_lut(), vec3({labels[idx]}_gamma));\n\n"
        )
        # if metadata["Channels"] > 7:
        #     break
    # shaderStr = shaderStr + '// Add RGB values of all channels\n'
    shaderStr = shaderStr + "vec3 rgb = ("
    for idx in range(channel_count):
        shaderStr = shaderStr + f"{labels[idx]}"
        # if metadata["Channels"] > 7:
        #     break
        if idx < channel_count - 1:
            shaderStr = shaderStr + " + "
    shaderStr = shaderStr + ");\n\n"

    # shaderStr = shaderStr + '//Retain RGB value with max of 1\n'
    shaderStr = shaderStr + "vec3 render = min(rgb,vec3(1));\n\n"
    # shaderStr = shaderStr + '// Render the resulting pixel map\n'
    shaderStr = shaderStr + "emitRGB(render);\n"
    shaderStr = shaderStr + "}"

    return shaderStr


## Build neuroglancer json
def ng_json(numpy_like_object, file=None, different_chunks=False):
    """
    Generate Neuroglancer JSON metadata for a 5D numpy-like volume.

    Args:
        numpy_like_object: A numpy-like object representing the dataset.
        file (str or None, optional): The output file name or "dict" for a dictionary output.
                                       Defaults to None (returns a BytesIO buffer).
        different_chunks (bool or int or tuple, optional): Custom chunking configuration. Defaults to False.

    Returns:
        io.BytesIO, str, or dict: The Neuroglancer JSON metadata as a memory buffer, string, or dictionary.
    """

    # Alternative chunking depth along axial plane
    offDimSize = different_chunks

    metadata = utils.metaDataExtraction(numpy_like_object, strKey=False)

    neuro_info = {}
    
    dtype = metadata["dtype"]
    if dtype == "float16" or dtype == "float64":
        dtype = "float32"
    elif dtype.endswith("u2"):
        dtype = "uint16"
    elif dtype.endswith("i2"):
        dtype = "int16"
    elif dtype.endswith("u1"):
        dtype = "uint8"
    elif dtype.endswith("i1"):
        dtype = "int8"
    elif dtype.endswith("i4"):
        dtype = "int32"
    elif dtype.endswith("u4"):
        dtype = "uint32"
    # elif dtype.endswith("u8"):
    #     dtype = "uint64"
    # elif dtype.endswith("i8"):
    #     dtype = "int64"
    neuro_info["data_type"] = dtype
    neuro_info["num_channels"] = 3 if metadata["ndim"] == 6 else metadata["Channels"]

    scales = []
    current_scale = {}
    for res in range(metadata["ResolutionLevels"]):
        logger.info("Creating JSON")
        try:
            chunks = list(
                reversed(list(metadata[(res, 0, 0, "chunks")][-3:]))
            )  # <-- [x,y,z] orientation
        except:
            chunks = list(
                reversed(list(metadata[(res, "chunks")][-3:]))
            )  # <-- [x,y,z] orientation
        logger.info(chunks)
        if different_chunks == False:
            current_scale["chunk_sizes"] = [list(chunks)]
        elif isinstance(different_chunks, int):
            current_scale["chunk_sizes"] = [
                [chunks[0], chunks[1], offDimSize],
                [chunks[0], offDimSize, chunks[1]],
                [offDimSize, chunks[0], chunks[1]],
            ]
        elif isinstance(different_chunks, tuple) and len(different_chunks) == 3:
            current_scale["chunk_sizes"] = [list(different_chunks)]

        current_scale["encoding"] = "raw"
        current_scale["key"] = str(res)
        current_scale["resolution"] = [
            x * 1000 for x in list(reversed(list(metadata[(res, 0, 0, "resolution")])))
        ]
        current_scale["size"] = list(
            reversed(list(metadata[(res, 0, 0, "shape")][-3:]))
        )
        current_scale["voxel_offset"] = [0, 0, 0]

        scales.append(current_scale)
        current_scale = {}

    neuro_info["scales"] = scales
    neuro_info["type"] = "image"
    # neuro_info['shader'] = ng_shader(numpy_like_object)

    logger.info(neuro_info)
    if file is None:
        b = io.BytesIO()
        b.write(json.dumps(neuro_info).encode())
        b.seek(0)
        return b
    elif file == "str":
        return json.dumps(neuro_info)
    elif file == "dict":
        return neuro_info
    else:
        with open(file, "w") as f:
            f.write(json.dumps(neuro_info))
        return


def ng_files(numpy_like_object):
    """
    Takes numpy_like_object representing a supported filetype
    and produces a dict where keys are int == resolution level and objects are
    compreshensive lists of filenames representing each chunk of structure:
    xstart-xstop_ystart-ystop_zstart-zstop. Not used?

    Args:
        numpy_like_object: A numpy-like object representing the dataset.

    Returns:
        dict: A dictionary where keys are resolution levels and values are lists of file names.
    """

    metadata = utils.metaDataExtraction(numpy_like_object, strKey=False)

    name_template = "{}-{}_{}-{}_{}-{}"
    fileLists = {}
    ## Make file list
    for res in range(metadata["ResolutionLevels"]):
        chunks = metadata[(res, 0, 0, "chunks")]
        shape = metadata[(res, 0, 0, "shape")]

        fileLists[res] = []
        for x, y, z in product(
            range(0, shape[-1], chunks[-1]),  # X-axis
            range(0, shape[-2], chunks[-2]),  # Y-axis
            range(0, shape[-3], chunks[-3]),  # Z-axis
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
            logger.info(currentName)
    return fileLists


def make_ng_link(open_dataset_with_ng_json, compatible_file_link, config=None):
    """
    Build a fully functional Neuroglancer link for a dataset.

    Args:
        open_dataset_with_ng_json: A dataset object containing Neuroglancer JSON metadata.
        compatible_file_link (str): Path to the Neuroglancer-compatible dataset.
        config (object, optional): Configuration settings. Defaults to None.

    Returns:
        str: The Neuroglancer link.
    """
    native_info = getattr(open_dataset_with_ng_json, "info", None)
    native_kind = _native_ng_dataset_kind(native_info) if native_info else None
    if native_kind in ("annotation", "segmentation"):
        return _make_native_ng_link(
            native_info,
            compatible_file_link,
            config=config,
        )

    import neuroglancer

    brainpi_url = config.settings.get("app", "url")
    ngURL = config.settings.get("neuroglancer", "url")

    # Start server simply to build viewer state
    token = "qwertysplithereqwertysplithereqwerty"
    viewer = neuroglancer.UnsynchronizedViewer(token=token)
    source = "precomputed://" + brainpi_url + compatible_file_link

    with viewer.txn() as s:
        # name = compatible_file_link.split('/')[-1].split('.')[0]
        name = os.path.split(compatible_file_link)[-1]
        s.layers[name] = neuroglancer.ImageLayer(
            source=source, tab="rendering", shader=ng_shader(open_dataset_with_ng_json)
        )

    # Neuroglancer CoordinateSpace
    # https://github.com/google/neuroglancer/blob/2200afbb85ab69550eeb3d2e089154d0ebc8a647/python/neuroglancer/coordinate_space.py#L146
    coord = neuroglancer.CoordinateSpace(
        names=("c^", "x", "y", "z"),
        scales=(
            1,
            open_dataset_with_ng_json.ng_json["scales"][0]["resolution"][0] / 1000,
            open_dataset_with_ng_json.ng_json["scales"][0]["resolution"][1] / 1000,
            open_dataset_with_ng_json.ng_json["scales"][0]["resolution"][2] / 1000,
        ),
        units=("", "um", "um", "um"),
    )
    viewer.state.dimensions = coord
    # I think this is units (microns) scale / pixel
    viewer.state.crossSectionScale = 50
    # ~ crossSectionScale*600 produces the same size projection x-section
    viewer.state.projection_scale = viewer.state.crossSectionScale * 600

    viewer.state.selected_layer.layer = name
    viewer.state.selected_layer.visible = True
    viewer.state.prefetch = True
    viewer.state.concurrent_downloads = 100
    viewer.state.layout.type = "xy"  # Options: ['xy', 'yz', 'xz', 'xy-3d', 'yz-3d', 'xz-3d', '4panel', '3d'] default=4panel

    url = viewer.get_viewer_url()
    state = url.split("/v/" + token + "/")[-1]

    ## If source URL is not secure, use the non-secure version of neuroglancer
    if "https://" in source == False:
        ngURL = ngURL.replace("https://", "http://")

    outURL = ngURL + state
    logger.info(outURL)

    # Cleanup to unsure that the neuroglancer server is no longer running
    if neuroglancer.server.is_server_running():
        neuroglancer.server.stop()
    del viewer
    del neuroglancer

    return outURL


def _native_ng_dataset_kind(info):
    """
    Classify a native Neuroglancer precomputed dataset from its info payload.
    """
    if not isinstance(info, dict):
        return None

    if info.get("type") == "image":
        return "image"
    if info.get("type") == "segmentation":
        return "segmentation"

    atype = str(info.get("@type", ""))
    if atype.startswith("neuroglancer_annotations_"):
        return "annotation"
    if info.get("annotation_type") is not None:
        return "annotation"
    return None


def _find_native_ng_dataset(fs_path):
    """
    Walk up from a filesystem path to find a native Neuroglancer precomputed root.
    """
    if not fs_path:
        return None, None, None

    current = fs_path
    if os.path.isfile(current):
        current = os.path.dirname(current)

    while True:
        info_path = os.path.join(current, "info")
        if current.lower().endswith(".pcd") and os.path.isfile(info_path):
            try:
                with open(info_path, "r", encoding="utf-8") as handle:
                    info = json.load(handle)
            except Exception:
                return None, None, None
            return current, info, _native_ng_dataset_kind(info)

        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent

    return None, None, None


def _make_native_ng_dimensions(info):
    """
    Build a Neuroglancer state dimensions object from a native precomputed info.
    """
    kind = _native_ng_dataset_kind(info)
    if kind == "annotation":
        dims = info.get("dimensions")
        if isinstance(dims, dict):
            return dims
        return None

    scales = info.get("scales") or []
    if not scales:
        return None

    resolution = scales[0].get("resolution")
    if resolution is None or len(resolution) < 3:
        return None

    return {
        "x": [float(resolution[0]) / 1000.0, "um"],
        "y": [float(resolution[1]) / 1000.0, "um"],
        "z": [float(resolution[2]) / 1000.0, "um"],
    }


def _make_native_ng_position(info):
    """
    Estimate a useful starting position from native precomputed metadata.
    """
    kind = _native_ng_dataset_kind(info)
    if kind == "annotation":
        lower = info.get("lower_bound")
        upper = info.get("upper_bound")
        if (
            isinstance(lower, (list, tuple))
            and isinstance(upper, (list, tuple))
            and len(lower) >= 3
            and len(upper) >= 3
        ):
            return [
                (float(lower[idx]) + float(upper[idx])) / 2.0 for idx in range(3)
            ]
        return None

    scales = info.get("scales") or []
    if not scales:
        return None

    size = scales[0].get("size")
    if not isinstance(size, (list, tuple)) or len(size) < 3:
        return None

    return [float(size[0]) / 2.0, float(size[1]) / 2.0, float(size[2]) / 2.0]


def _make_native_ng_link(info, compatible_file_link, config=None):
    """
    Build a Neuroglancer link for native annotation or segmentation datasets.
    """
    brainpi_url = config.settings.get("app", "url")
    ngURL = config.settings.get("neuroglancer", "url")
    source = "precomputed://" + brainpi_url + compatible_file_link
    name = os.path.split(compatible_file_link)[-1]
    kind = _native_ng_dataset_kind(info)

    layer = {
        "source": source,
        "name": name,
    }
    if kind == "annotation":
        layer["type"] = "annotation"
        layer["tab"] = "annotations"
    elif kind == "segmentation":
        layer["type"] = "segmentation"
        layer["tab"] = "segments"
    else:
        raise ValueError("Unsupported native Neuroglancer dataset type")

    state_dict = {
        "layers": [layer],
        "selectedLayer": {
            "layer": name,
            "visible": True,
        },
        "layout": "4panel",
    }

    dimensions = _make_native_ng_dimensions(info)
    if dimensions is not None:
        state_dict["dimensions"] = dimensions

    position = _make_native_ng_position(info)
    if position is not None:
        state_dict["position"] = position

    if kind == "annotation":
        state_dict["crossSectionScale"] = 50
        state_dict["projectionScale"] = 32000

    if "https://" not in source:
        ngURL = ngURL.replace("https://", "http://")

    encoded_state = quote(json.dumps(state_dict, separators=(",", ":")), safe="")
    outURL = ngURL + "#!" + encoded_state
    logger.info(outURL)
    return outURL


# def make_ng_link(open_dataset_with_ng_json, compatible_file_link, ngURL='https://neuroglancer-demo.appspot.com/'):
#     '''
#     Attempts to build a fully working link to ng dataset
#     '''
#     stateDict = {}
#     stateDict['dimensions'] = {'x': [ open_dataset_with_ng_json.ng_json['scales'][0]['resolution'][0]/1000,'um' ],
#                                'y': [ open_dataset_with_ng_json.ng_json['scales'][0]['resolution'][1]/1000,'um' ],
#                                'z': [ open_dataset_with_ng_json.ng_json['scales'][0]['resolution'][2]/1000,'um' ]
#                                }
#     stateDict['position'] = [ open_dataset_with_ng_json.ng_json['scales'][0]['size'][0]//2,
#                              open_dataset_with_ng_json.ng_json['scales'][0]['size'][1]//2,
#                              open_dataset_with_ng_json.ng_json['scales'][0]['size'][2]//2
#                              ]
#
#     stateDict['crossSectionScale'] = 50
#     stateDict['projectionScale'] = 50 * stateDict['dimensions']['z'][0]
#
#     stateDict['layers'] = []
#
#     layer = {}
#     layer['type'] = 'image'
#     layer['source'] = 'precomputed://' + 'https://brain-api.cbi.pitt.edu' + compatible_file_link #<-- Needs to be imported intellegently
#     # layer['tab'] = 'rendering'
#     layer['shader'] = ng_shader(open_dataset_with_ng_json) # Includes controls and defaults
#     # layer['shaderControls'] = {'normalized': {'range': [0, 9814], 'channel': [0]}} #<-- include an intellegent way to adjust shader
#     # layer['channelDimensions'] = {'c^': [1, '']}
#     layer['name'] = os.path.split(compatible_file_link)[-1]
#     # layer['selectedLayer'] = {'visible': True, 'layer': layer['name']}
#     layer['layout'] = '4panel'
#
#     stateDict['layers'].append(layer)
#
#     ## If source URL is not secure, use the non-secure version of neuroglancer
#     if 'https://' in stateDict['layers'][0]['source'] == False:
#         ngURL = ngURL.replace('https://','http://')
#
#     outURL = ngURL + r'#!'
#     outURL = outURL + str(stateDict)
#     # outURL = outURL.replace(',','%2C')
#     # outURL = outURL.replace('\\','')
#     outURL = outURL.replace('True','true')
#     outURL = outURL.replace('False','false')
#     print(outURL)
#
#     return outURL


def neuroglancer_dtypes():
    """
    List supported file types for Neuroglancer.

    Returns:
        list: Supported file extensions.
    """
    return [
        ".ims",  # imaris
        # '.omezarr', #ome.zarr
        ".omezans",  # Archived_Nested_Store
        ".omehans",  # H5_Nested_Store
        ".zarr",  # Custom multiscale zarr implementation
        ".nii",
        ".nii.gz",
        ".nii.zarr",
        # '.weave',
        # '.z_sharded'
        ".terafly",
        ".ome.tif", 
        ".ome.tiff",
        ".tif",
        ".tiff",
        ".ome-tif",
        ".ome-tiff",
        ".jp2",
        ".nd2",
        ".pcd",
    ]


def open_ng_dataset(config, datapath):
    """
    Open a Neuroglancer-compatible dataset and prepare its JSON metadata.

    Args:
        config (object): Configuration settings.
        datapath (str): Path to the dataset.

    Returns:
        str: The dataset path.
    """
    # datapath = config.loadDataset(datapath, datapath)

    # logger.info("IN OPEN NG DATASET 411")

    # if not hasattr(config.opendata[datapath], "ng_json"):
    #     logger.info("IN NO ATTR DATASET 414")
    #     # or not hasattr(config.opendata[datapath],'ng_files'):

    #     ## Forms a comrehensive file list for all chunks
    #     ## Not necessary for neuroglancer to function and take a long time
    #     # config.opendata[datapath].ng_files = \
    #     #     neuroGlancer.ng_files(config.opendata[datapath])

    #     ## Temp ignoring of ng_files
    #     ## Add attribute so this constantly repeated
    #     # config.opendata[datapath].ng_files = True

    #     settings = config_tools.get_config("settings.ini")
    #     chunk_type = settings.get("neuroglancer", "chunk_type")

    #     if chunk_type.lower() == "isotropic":
    #         chunk_depth = settings.getint("neuroglancer", "chunk_depth")
    #         config.opendata[datapath].ng_json = ng_json(
    #             config.opendata[datapath],
    #             file="dict",
    #             different_chunks=(chunk_depth, chunk_depth, chunk_depth),
    #         )
    #     elif chunk_type.lower() == "anisotropic":
    #         chunk_depth = settings.getint("neuroglancer", "chunk_depth")
    #         config.opendata[datapath].ng_json = ng_json(
    #             config.opendata[datapath], file="dict", different_chunks=chunk_depth
    #         )
    #     else:
    #         config.opendata[datapath].ng_json = ng_json(
    #             config.opendata[datapath], file="dict"
    #         )

    # return datapath
    datapath_key = utils.load_dataset(config, datapath)

    logger.info("IN OPEN NG DATASET 411")

    dataset_lock = getattr(config, "dataset_lock", None)
    lock_context = dataset_lock(datapath_key) if dataset_lock else nullcontext()
    with lock_context:
        dataset = config.opendata[datapath_key]
        if hasattr(dataset, "ng_json"):
            return datapath_key

        logger.info("IN NO ATTR DATASET 414")
        # or not hasattr(config.opendata[datapath],'ng_files'):

        ## Forms a comrehensive file list for all chunks
        ## Not necessary for neuroglancer to function and take a long time
        # config.opendata[datapath].ng_files = \
        #     neuroGlancer.ng_files(config.opendata[datapath])

        ## Temp ignoring of ng_files
        ## Add attribute so this constantly repeated
        # config.opendata[datapath].ng_files = True

        settings = config_tools.get_config("settings.ini")
        chunk_type = settings.get("neuroglancer", "chunk_type")

        if chunk_type.lower() == "isotropic":
            chunk_depth = settings.getint("neuroglancer", "chunk_depth")
            dataset.ng_json = ng_json(
                dataset,
                file="dict",
                different_chunks=(chunk_depth, chunk_depth, chunk_depth),
            )
        elif chunk_type.lower() == "anisotropic":
            chunk_depth = settings.getint("neuroglancer", "chunk_depth")
            dataset.ng_json = ng_json(
                dataset, file="dict", different_chunks=chunk_depth
            )
        else:
            dataset.ng_json = ng_json(dataset, file="dict")

    return datapath_key



#######################################################################################
##  Neuroglancer entry point : decorated separately below to enable caching and flask entry
#######################################################################################


def setup_neuroglancer(app, config):
    """
    Set up Flask routes and endpoints for Neuroglancer integration.

    Args:
        app (Flask): The Flask application instance.
        config (object): Configuration settings.

    Returns:
        Flask: The modified Flask application with Neuroglancer endpoints.
    """
    # get_server will only open 1 server if it does not already exist.
    if config.settings.getboolean("neuroglancer", "use_local_server"):
        from neuroglancer_server import get_server

        ng_server = get_server()
        config.ng_server = ng_server

    # Establish file_pattern once so it isn't created on each request.
    file_pattern = "[0-9]+-[0-9]+_[0-9]+-[0-9]+_[0-9]+-[0-9]+"

    # Establish highly used functions as objects to improve speed
    get_html_split_and_associated_file_path = (
        utils.get_html_split_and_associated_file_path
    )
    match = re.match
    Match_class = re.Match

    @logger.catch
    def neuro_glancer_entry(req_path, request=request):
        """Handle a Neuroglancer view, ``info`` document, or raw chunk request.

        Args:
            req_path: Route path below the ``/ng/`` prefix.
            request: Flask request object; injectable for tokenized URL helpers.

        Returns:
            flask.Response: Redirect page, JSON metadata, native resource, or
            encoded precomputed chunk depending on the requested suffix.
        """
        # Request is an option for using this function separate from traditional flask response
        # See usage in tokenized_urls module
        logger.trace(request.path)
        path_split, datapath = get_html_split_and_associated_file_path(config, request)
        requested_fs_path = datapath
        # logger.info(f'{path_split},{datapath}')

        # Test for different patterns
        # file_name_template = '{}-{}_{}-{}_{}-{}'
        # file_pattern = file_name_template.format('[0-9]+','[0-9]+','[0-9]+','[0-9]+','[0-9]+','[0-9]+')
        # file_pattern = '[0-9]+-[0-9]+_[0-9]+-[0-9]+_[0-9]+-[0-9]+'
        ## NEED to figure out how to extract the datapath from any version of ng request:
        # /hdshjk/file.ims : /hdshjk/file.ims/info : /hdshjk/file.ims/info/0/0-1_2-3_4-5

        # Find the file system path to the dataset
        # Assumptions are neuroglancer only requests 'info' file or chunkfiles
        # If only the file name is requested this will redirect to a
        if isinstance(match(file_pattern, path_split[-1]), Match_class):
            datapath = os.path.split(datapath)[0]
            datapath = os.path.split(datapath)[0]
            # datapath = '/' + os.path.join(*datapath.split('/')[:-2])
        elif path_split[-1] == "info":
            datapath = os.path.split(datapath)[0]
            # datapath = '/' + os.path.join(*datapath.split('/')[:-1])
            # datapath = os.path.join(*datapath.split('/')[:-1])

        # elif utils.is_file_type(neuroglancer_dtypes(), datapath):
        #     datapath = open_ng_dataset(config,datapath) # Ensures that dataset is open AND info_json is formed
        #     link_to_ng = make_ng_link(config.opendata[datapath], request.path, config=config)
        #     # redirect.html URLs are not necessary, but they facilitate the inclusion of gtag for google analytics
        #     return render_template('redirect.html',gtag=config.settings.get('GA4','gtag'),
        #                            redirect_url=link_to_ng,
        #                            redirect_name='Neuroglancer',
        #                            description=datapath)
        #     # return redirect(link_to_ng) # Redirect browser to fully formed neuroglancer link

        elif utils.is_file_type(neuroglancer_dtypes(), datapath):
            view_path = request.path + "/ng_view"
            file_name = datapath.split("/")[-1]
            return render_template(
                "file_loading.html",
                # gtag=config.settings.get("GA4", "gtag"),
                redirect_url=view_path,
                redirect_name="Neuroglancer",
                description=datapath,
                file_name=file_name,
            )
        elif path_split[-1].endswith("ng_view"):
            try:
                path_split = tuple(part for part in path_split if part != "ng_view")
                datapath = datapath.replace("/ng_view", "")
                request.path = request.path.replace("/ng_view", "")
                native_root, native_info, native_kind = _find_native_ng_dataset(datapath)
                if native_root == datapath and native_kind in ("annotation", "segmentation"):
                    link_to_ng = _make_native_ng_link(
                        native_info,
                        request.path,
                        config=config,
                    )
                    return render_template(
                        "redirect.html",
                        gtag=config.settings.get("GA4", "gtag"),
                        redirect_url=link_to_ng,
                        redirect_name="Neuroglancer",
                        description=datapath,
                    )
                datapath_key = open_ng_dataset(
                    config, datapath
                )  # Ensures that dataset is open AND info_json is formed
                link_to_ng = make_ng_link(
                    config.opendata[datapath_key], request.path, config=config
                )
                # redirect.html URLs are not necessary, but they facilitate the inclusion of gtag for google analytics
                return render_template(
                    "redirect.html",
                    gtag=config.settings.get("GA4", "gtag"),
                    redirect_url=link_to_ng,
                    redirect_name="Neuroglancer",
                    description=datapath,
                )
                # return redirect(link_to_ng) # Redirect browser to fully formed neuroglancer link
            except Exception as e:
                logger.error(f'{datapath}: {e}')
                return render_template(
                    "file_exception.html",
                    gtag=config.settings.get("GA4", "gtag"),
                    exception=e,
                )
        elif path_split[-1].endswith("json"):
            view_path = request.path + "/ng_state_view"
            file_name = datapath.split("/")[-1]
            return render_template(
                "file_loading.html",
                # gtag=config.settings.get("GA4", "gtag"),
                redirect_url=view_path,
                redirect_name="Neuroglancer",
                description=datapath,
                file_name=file_name,
            )
        elif path_split[-1].endswith("ng_state_view"):
            try:
                path_split = tuple(part for part in path_split if part != "ng_state_view")
                datapath = datapath.replace("/ng_state_view", "")
                request.path = request.path.replace("/ng_state_view", "")
                base_url = config.settings.get("neuroglancer", "url")
                with open(datapath, "r") as f:
                    state_dict = f.read()   # <- dict, not raw string
                link_to_ng = base_url + "#!" + state_dict

                return render_template(
                    "redirect.html",
                    gtag=config.settings.get("GA4", "gtag"),
                    redirect_url=link_to_ng,
                    redirect_name="Neuroglancer",
                    description=datapath,
                )
                # return redirect(link_to_ng) # Redirect browser to fully formed neuroglancer link
            except Exception as e:
                logger.error(f'{datapath}: {e}')
                return render_template(
                    "file_exception.html",
                    gtag=config.settings.get("GA4", "gtag"),
                    exception=e,
                )
        else:
            native_root, native_info, native_kind = _find_native_ng_dataset(
                requested_fs_path
            )
            if (
                native_kind in ("annotation", "segmentation")
                and os.path.isfile(requested_fs_path)
                and os.path.commonpath([native_root, requested_fs_path]) == native_root
            ):
                if os.path.basename(requested_fs_path) == "info":
                    return jsonify(native_info)
                return send_file(
                    requested_fs_path,
                    as_attachment=False,
                    download_name=os.path.basename(requested_fs_path),
                )
            return "No path to neuroglancer supported dataset"

        # datapath = open_ng_dataset(config,datapath) # Ensures that dataset is open AND info_json is formed

        # Return 'info' json
        if path_split[-1] == "info":
            # stat = os.stat(datapath)
            # file_ino = str(stat.st_ino)
            # modification_time = str(stat.st_mtime)
            try:
                native_root, native_info, native_kind = _find_native_ng_dataset(
                    requested_fs_path
                )
                if native_root == datapath and native_kind in ("annotation", "segmentation"):
                    return jsonify(native_info)
                datapath_key = open_ng_dataset(config, datapath)
                b = io.BytesIO()
                b.write(
                    json.dumps(
                        config.opendata[datapath_key].ng_json, indent=2, sort_keys=False
                    ).encode()
                )
                # b.write(json.dumps(config.opendata[datapath].ng_json).encode())
                b.seek(0)

                return jsonify(config.opendata[datapath_key].ng_json)
            # return send_file(
            #     b,
            #     as_attachment=False,
            #     download_name='info',
            #     mimetype='application/json'
            # )
            # return send_file(
            #     b,
            #     as_attachment=False,
            #     download_name='info', # name needs to match chunk
            #     mimetype='application/octet-stream'
            # )
            except Exception as e:
                logger.error(f'{datapath}: {e}')
                return render_template(
                    "file_exception.html",
                    gtag=config.settings.get("GA4", "gtag"),
                    exception=e,
                )

        ## Serve neuroglancer raw-format files
        elif isinstance(match(file_pattern, path_split[-1]), Match_class):
            native_root, native_info, native_kind = _find_native_ng_dataset(
                requested_fs_path
            )
            if (
                native_kind in ("annotation", "segmentation")
                and os.path.isfile(requested_fs_path)
                and os.path.commonpath([native_root, requested_fs_path]) == native_root
            ):
                return send_file(
                    requested_fs_path,
                    as_attachment=False,
                    download_name=os.path.basename(requested_fs_path),
                )
            datapath_key = open_ng_dataset(config, datapath)
            dataset = config.opendata[datapath_key]
            # logger.info(request.path + '\n')

            x, y, z = path_split[-1].split("_")
            x = x.split("-")
            y = y.split("-")
            z = z.split("-")
            x = [int(x) for x in x]
            y = [int(x) for x in y]
            z = [int(x) for x in z]

            if x[1] <= x[0] or y[1] <= y[0] or z[1] <= z[0]:
                logger.warning(
                    f"Ignoring empty Neuroglancer chunk request: {path_split[-1]}"
                )
                return Response(status=404)

            res = int(path_split[-2])

            img = None
            # key = f'ng_{datapath}-{res}-{x}-{y}-{z}'
            if config.cache is not None:
                key = f"ng_{datapath_key}-{res}-{x}-{y}-{z}"
                img = config.cache.get(key, default=None, retry=True)
                if img is not None:
                    logger.info("ng cache found")

            if img is None:
                try:
                    img = dataset[
                        res,
                        slice(0, 1),
                        slice(None),
                        slice(z[0], z[1]),
                        slice(y[0], y[1]),
                        slice(x[0], x[1]),
                    ]
                    # this is only for tif RGB files
                    if img.ndim == 6:
                        # drop first two axes, keep RGB, move channels first
                        img = np.moveaxis(img[0, 0, ..., :3], -1, 0)
                    while img.ndim > 4:
                        img = np.squeeze(img, axis=0)
                    while img.ndim < 4:
                        img = np.expand_dims(img, axis=0)
                    img = encode_ng_file(
                        img, dataset.ng_json["num_channels"]
                    )
                except Exception as exc:
                    logger.exception(
                        f"Failed to read source chunk {path_split[-1]} "
                        f"at resolution {res} from {datapath}"
                    )
                    return jsonify(
                        {
                            "error": "Failed to read source dataset chunk",
                            "resolution": res,
                            "chunk": path_split[-1],
                            "detail": str(exc),
                        }
                    ), 502

                if config.cache is not None:
                    config.cache.set(key, img, expire=None, tag=datapath_key, retry=True)
                    logger.info("ng cache saved")
            # Flask return of bytesIO as file
            # return Response(response=img, status=200,
            #                 mimetype="application/octet_stream")
            response = Response(
                response=img, status=200, mimetype="application/octet-stream"
            )

            response = compress_flask_response(response, request, 9)

            return response

            # res = make_response(
            #     send_file(
            #     img,
            #     as_attachment=True,
            #     download_name=path_split[-1], # name needs to match chunk
            #     mimetype='application/octet-stream'
            # )
            # )
            # return res

        # # Not necessary with config.opendata[datapath].ng_files not being built
        # # Build appropriate File List in base path
        # if len(url_path_split) == 1:
        #     res_files = list(config.opendata[datapath].ng_files.keys())
        #     # return str(res_files)
        #     files = ['info', *res_files]
        #     files = [str(x) for x in files]
        #     path = [request.script_root]
        #     return render_template('vfs_bil.html', path=path, files=files)

        # # Not necessary with config.opendata[datapath].ng_files not being built
        # # Build html to display all ng_files chunks
        # if len(url_path_split) == 2 and isinstance(match('[0-9]+',url_path_split[-1]),Match_class):
        #     res = int(url_path_split[-1])
        #     files = config.opendata[datapath].ng_files[res]
        #     path = [request.script_root]
        #     return render_template('vfs_bil.html', path=path, files=files)

        return "Path not accessable"

    ##############################################################################

    ngPath = (
        "/ng/"  # <--- final slash is required for proper navigation through dir tree
    )

    #################################################################
    ## ** TURN ON CACHING OF NG END POINT BY UNCOMMENTING BELOW ** ##
    #################################################################
    ## Decorating neuro_glancer_entry to allow caching ##
    if config.cache is not None:
        print("Caching setup")
        # neuro_glancer_entry = config.cache.memoize()(neuro_glancer_entry)
        # neuro_glancer_entry = config.fcache.cached(timeout=3600)(neuro_glancer_entry)
        # neuro_glancer_entry = lru_cache(maxsize=5000)(neuro_glancer_entry) #Causing some IO errors not sure why
    # neuro_glancer_entry = login_required(neuro_glancer_entry)
    # neuro_glancer_entry = brainpi_cache_ram.memoize(neuro_glancer_entry)

    neuro_glancer_entry = cross_origin(allow_headers=["Content-Type"])(
        neuro_glancer_entry
    )
    # neuro_glancer_entry = login_required(neuro_glancer_entry)
    neuro_glancer_entry = app.route(ngPath + "<path:req_path>")(neuro_glancer_entry)
    neuro_glancer_entry = app.route(ngPath, defaults={"req_path": ""})(
        neuro_glancer_entry
    )

    return app


##############################################################################
## END NEUROGLANCER
##############################################################################


##############################################################################
##  Notes and examples below
##############################################################################

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
#   "data_type": "uint8",
#   "num_channels": 1,
#   "scales": [{"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "8_8_8",
#     "resolution": [8, 8, 8],
#     "size": [6446, 6643, 8090],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "16_16_16",
#     "resolution": [16, 16, 16],
#     "size": [3223, 3321, 4045],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "32_32_32",
#     "resolution": [32, 32, 32],
#     "size": [1611, 1660, 2022],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "64_64_64",
#     "resolution": [64, 64, 64],
#     "size": [805, 830, 1011],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "128_128_128",
#     "resolution": [128, 128, 128],
#     "size": [402, 415, 505],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "256_256_256",
#     "resolution": [256, 256, 256],
#     "size": [201, 207, 252],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "512_512_512",
#     "resolution": [512, 512, 512],
#     "size": [100, 103, 126],
#     "voxel_offset": [0, 0, 0]}],
#   "type": "image"}


"""
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
    
"""

## n-tracer info

# {'data_type': 'uint16',
#  'num_channels': 1,
#  'type': 'image',
#  'scales': [{'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '1',
#    'resolution': [350, 350, 1000],
#    'size': [32768, 20480, 13312],
#    'voxel_offset': [0, 0, 0]},
#   {'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '2',
#    'resolution': [700, 700, 2000],
#    'size': [16384, 10240, 6656],
#    'voxel_offset': [0, 0, 0]},
#   {'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '4',
#    'resolution': [1400, 1400, 4000],
#    'size': [8192, 5120, 3328],
#    'voxel_offset': [0, 0, 0]},
#   {'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '8',
#    'resolution': [2800, 2800, 8000],
#    'size': [4096, 2560, 1664],
#    'voxel_offset': [0, 0, 0]},
#   {'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '16',
#    'resolution': [5600, 5600, 16000],
#    'size': [2048, 1280, 832],
#    'voxel_offset': [0, 0, 0]},
#   {'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '32',
#    'resolution': [11200, 11200, 32000],
#    'size': [1024, 640, 416],
#    'voxel_offset': [0, 0, 0]}]}

####  ng_docs
"""
https://github.com/google/neuroglancer/blob/master/src/neuroglancer/sliceview/README.md

Neuroglancer also supports multiple (anisotropic) chunk sizes to be used 
simultaneously with a single volume, in which case each SliceView selects 
the chunk size (at each resolution) that is most efficient. For example, 
to support XY, XZ, and YZ cross-sectional views, chunk sizes of 
(512, 512, 1), (512, 1, 512) and (1, 512, 512) could be used. This does have 
the disadvantage, however, that chunk data is not shared at all by the 3 views
"""


# # ## Browser state example 'Hook's Brain:

# a = '{"dimensions":{"x":[4.98e-7%2C"m"]%2C"y":[4.98e-7%2C"m"]%2C"z":[0.00000533%2C"m"]}%2C"position":[9396.5%2C13847.5%2C562.5]%2C"crossSectionScale":54.598150033144236%2C"projectionScale":32000%2C"layers":[{"type":"image"%2C"source":"precomputed://https://brain-api.cbi.pitt.edu/api/ng/3"%2C"tab":"rendering"%2C"shaderControls":{"normalized":{"range":[0%2C9814]%2C"channel":[1]}}%2C"channelDimensions":{"c^":[1%2C""]}%2C"name":"3"}]%2C"selectedLayer":{"visible":true%2C"layer":"3"}%2C"layout":"4panel"}'

# # b = a.replace(r'https://neuroglancer-demo.appspot.com/#!','')
# # b = b.replace(r'http://neuroglancer-demo.appspot.com/#!','')
# b = a.replace('%2C',',')
# b = b.replace('true','True')
# b = b.replace('false','False')
# b = eval(b)


#################################################################################################################
## Note on interacting with neuroglancer python package to manipulate viewer
#################################################################################################################

# # This code enables the python neuroglancer package to start a viewer server that IS SYNCRONIZED to the python process
# # In this way any change to the remote viewer is captured by the python process and any change to the viewer is
# # imediately effects the remote viewer.
#
# import neuroglancer
# neuroglancer.server.set_server_bind_address('128.182.82.56')
# viewer = neuroglancer.Viewer()
#
#
# # This code enables the python neuroglancer package to start a viewer server that is not syncronized to the python process
# # In this way an independant view can be designed and then the only requirement is that the server remains active.
# # It appears that an unlimited number of views can be shared off of the same server.
# import neuroglancer
#
# # ip should be the domain/ip to the server
# ip = '128.182.82.56'
#
# # token enables you to define a path to the server (http://{ip}/v/{token}
# token = test
#
# neuroglancer.server.set_server_bind_address(ip)
#
# #UnsynchronizedViewer allows the server to accept any JSON state
# viewer = neuroglancer.UnsynchronizedViewer(token='test')
#
# # Define custom shaders
# shader="""
# 	void main() {
# 	emitRGB(vec3(toNormalized(getDataValue(0)),
# 	toNormalized(getDataValue(1)),
# 	toNormalized(getDataValue(2))));
# 	}
#     """
#
# #tab = 'source','rendering','annotations'
# with viewer.txn() as s:
#     s.layers['image'] = neuroglancer.ImageLayer(
# 	source='precomputed://http://c02.bil.psc.edu:5002/ng/proj/rf1hillman/2023_01_19_largeSlab_NPBB299_2_tiff_corrected.omezans',
# 	tab='rendering', shader=shader
# 	)
#
# #Enable/Disable prefetch
# viewer.state.prefetch=False
#
# #Change concurrent downloads
# viewer.state.concurrent_downloads=100 # 100 is default
#
# # Controlling the view in neuroglancer
# # Visual side panel:
#
# #Select specific layer by name:
# viewer.state.selected_layer.layer = 'image'
# viewer.state.selected_layer.visible = True
