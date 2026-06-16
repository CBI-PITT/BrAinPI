import utils
import os
from flask import (
    render_template,
    request,
    redirect,
    jsonify,
    Response,
)
from PIL import Image
import io
from flask_cors import cross_origin
import numpy as np
import hashlib
from logger_tools import logger
import cv2
import re
import json

DEFAULT_CHANNEL_COLORS = [
    "#00ff00",
    "#ff0000",
    "#0000ff",
    "#ffff00",
    "#ff00ff",
    "#00ffff",
    "#ffa500",
    "#ffffff",
]


def _is_rgb_volume(img_obj):
    """
    Detect packed RGB-style datasets that should bypass single-channel controls.
    """
    try:
        return int(img_obj.metadata.get("ndim", 0)) == 6
    except Exception:
        return False


def _normalize_hex_color(color, fallback):
    """
    Normalize a color string to #rrggbb.
    """
    try:
        color = str(color).strip()
    except Exception:
        return fallback
    if color.startswith("#"):
        color = color[1:]
    if len(color) == 3:
        color = "".join(ch * 2 for ch in color)
    if len(color) != 6:
        return fallback
    try:
        int(color, 16)
    except ValueError:
        return fallback
    return f"#{color.lower()}"


def _sample_channel_range(img_obj, channel_index):
    """
    Estimate a stable display range from the lowest resolution for a channel.
    """
    cache = getattr(img_obj, "_osd_channel_range_cache", None)
    if cache is None:
        cache = {}
        setattr(img_obj, "_osd_channel_range_cache", cache)
    if channel_index in cache:
        return cache[channel_index]

    lowest_res = int(img_obj.metadata.get("ResolutionLevels", img_obj.ResolutionLevels)) - 1
    sample = img_obj[
        lowest_res,
        slice(0, 1),
        slice(channel_index, channel_index + 1),
        slice(None),
        slice(None),
        slice(None),
    ]
    sample = np.asarray(sample, dtype=np.float32)
    finite = sample[np.isfinite(sample)]
    if finite.size == 0:
        cache[channel_index] = (0.0, 1.0)
        return cache[channel_index]

    positive = finite[finite > 0]
    working = positive if positive.size else finite
    low = float(np.min(working))
    high = float(np.max(working))
    if high <= low:
        high = low + 1.0
    cache[channel_index] = (low, high)
    return cache[channel_index]


def _sample_rgb_channel_ranges(img_obj):
    """
    Estimate stable display ranges for packed RGB images from the lowest resolution.
    """
    cache = getattr(img_obj, "_osd_rgb_channel_range_cache", None)
    if cache is not None:
        return cache

    lowest_res = int(img_obj.metadata.get("ResolutionLevels", img_obj.ResolutionLevels)) - 1
    sample = img_obj[
        lowest_res,
        slice(0, 1),
        slice(0, 1),
        slice(None),
        slice(None),
        slice(None),
    ]
    sample = np.squeeze(np.asarray(sample, dtype=np.float32))

    if sample.ndim != 3 or sample.shape[-1] != 3:
        cache = [(0.0, 255.0)] * 3
        setattr(img_obj, "_osd_rgb_channel_range_cache", cache)
        return cache

    ranges = []
    for idx in range(sample.shape[-1]):
        channel = sample[..., idx]
        finite = channel[np.isfinite(channel)]
        if finite.size == 0:
            ranges.append((0.0, 1.0))
            continue
        positive = finite[finite > 0]
        working = positive if positive.size else finite
        low = float(np.min(working))
        high = float(np.max(working))
        if high <= low:
            high = low + 1.0
        ranges.append((low, high))

    setattr(img_obj, "_osd_rgb_channel_range_cache", ranges)
    return ranges


def _get_channel_info(img_obj, channel_index):
    """
    Build client-side defaults for a single display channel.
    """
    fallback_color = DEFAULT_CHANNEL_COLORS[channel_index % len(DEFAULT_CHANNEL_COLORS)]
    label = f"Channel {channel_index}"
    color = fallback_color
    range_min = None
    range_max = None
    window_start = None
    window_end = None

    try:
        omero = getattr(img_obj, "omero", None)
        if omero:
            channels = omero.get("channels", [])
            if 0 <= channel_index < len(channels):
                channel_meta = channels[channel_index]
                label = str(channel_meta.get("label") or label)
                color = _normalize_hex_color(channel_meta.get("color"), fallback_color)
                window = channel_meta.get("window", {})
                range_min = window.get("min")
                range_max = window.get("max")
                window_start = window.get("start")
                window_end = window.get("end")
    except Exception:
        pass

    metadata = getattr(img_obj, "metadata", {})
    if range_min is None:
        range_min = metadata.get((0, 0, channel_index, "min"))
    if range_max is None:
        range_max = metadata.get((0, 0, channel_index, "max"))

    if range_min is None or range_max is None:
        range_min, range_max = _sample_channel_range(img_obj, channel_index)

    range_min = float(range_min)
    range_max = float(range_max)
    if range_max <= range_min:
        range_max = range_min + 1.0

    if window_start is None:
        window_start = range_min
    if window_end is None:
        window_end = range_max

    window_start = max(range_min, min(float(window_start), range_max))
    window_end = max(window_start, min(float(window_end), range_max))
    span = range_max - range_min

    return {
        "index": channel_index,
        "label": label,
        "color": color,
        "range_min": range_min,
        "range_max": range_max,
        "window_min_default": 0.0 if span <= 0 else (window_start - range_min) / span,
        "window_max_default": 1.0 if span <= 0 else (window_end - range_min) / span,
        "gamma_default": 1.0,
    }


def _scale_rgb_to_uint8(chunk, img_obj):
    """
    Scale packed RGB data to uint8 using stable per-channel ranges.
    """
    ranges = _sample_rgb_channel_ranges(img_obj)
    scaled = np.zeros(chunk.shape, dtype=np.uint8)
    for idx in range(min(chunk.shape[-1], len(ranges))):
        low, high = ranges[idx]
        scaled[..., idx] = _scale_to_uint8(chunk[..., idx], low, high)
    return scaled


def _build_channel_infos(img_obj):
    """
    Build and cache display defaults for all channels.
    """
    cache = getattr(img_obj, "_osd_channel_infos", None)
    if cache is not None:
        return cache
    infos = [
        _get_channel_info(img_obj, channel_idx)
        for channel_idx in range(int(img_obj.metadata.get("Channels")))
    ]
    setattr(img_obj, "_osd_channel_infos", infos)
    return infos


def _scale_to_uint8(chunk, low, high):
    """
    Scale a single channel to uint8 using a stable display range.
    """
    chunk = np.asarray(chunk, dtype=np.float32)
    chunk = np.nan_to_num(chunk, nan=low, posinf=high, neginf=low)
    if high <= low:
        return np.zeros(chunk.shape, dtype=np.uint8)
    chunk = np.clip(chunk, low, high)
    chunk = (chunk - low) / (high - low)
    chunk = np.clip(chunk, 0.0, 1.0)
    return (chunk * 255.0).astype(np.uint8)

def _build_time_index_map(img_obj):
    """
    Build mapping for multidimensional time axes (t/m) to linear time index.
    """
    time_keys = getattr(img_obj, "time_keys", None)
    time_key_names = getattr(img_obj, "time_key_names", None)
    if not time_keys or not time_key_names:
        return None, None, None
    names = [str(n).lower() for n in time_key_names]
    if any(name not in ("t", "m") for name in names):
        return None, None, None

    name_index = {name: idx for idx, name in enumerate(names)}
    t_values = (
        sorted({int(key[name_index["t"]]) for key in time_keys})
        if "t" in name_index
        else [0]
    )
    m_values = (
        sorted({int(key[name_index["m"]]) for key in time_keys})
        if "m" in name_index
        else [0]
    )

    time_index_map = {}
    for idx, key in enumerate(time_keys):
        t_val = int(key[name_index["t"]]) if "t" in name_index else 0
        m_val = int(key[name_index["m"]]) if "m" in name_index else 0
        time_index_map[f"{t_val}:{m_val}"] = idx

    return t_values, m_values, time_index_map


def _map_z_index_to_resolution(img_obj, res, z_index):
    """
    Map a full-resolution z index to the closest valid z index for a
    requested resolution level.
    """
    try:
        full_shape = img_obj.metadata.get((0, 0, 0, "shape")) or img_obj.metadata.get("shape")
        level_shape = img_obj.metadata.get((res, 0, 0, "shape")) or img_obj.metadata.get("shape")
        full_z = int(full_shape[-3])
        level_z = int(level_shape[-3])
        if level_z <= 1 or full_z <= 1:
            return max(0, min(int(z_index), level_z - 1))

        scaled_z = int((int(z_index) / max(full_z - 1, 1)) * (level_z - 1))
        return max(0, min(scaled_z, level_z - 1))
    except Exception:
        return max(0, int(z_index))


def openseadragon_dtypes():
    """
    Returns a list of supported file extensions for OpenSeadragon.

    Returns:
        list: A list of supported file extensions.
    """
    return [".tif", ".tiff", ".ome.tif", ".ome.tiff", ".ome-tif", ".ome-tiff", ".jp2", ".nd2",
            '.terafly','.ims','.ome.zarr',".omehans",".omezans",".nii",".nii.gz",".nii.zarr",".zarr"]


# def calculate_hash(input_string):
#     """
#     Calculates the SHA-256 hash of the input string.

#     Args:
#         input_string (str): The input string to hash.

#     Returns:
#         str: The SHA-256 hash of the input string.
#     """
#     hash_result = hashlib.sha256(input_string.encode()).hexdigest()
#     return hash_result


openSeadragonPath = "/osd/"


def setup_openseadragon(app, config):
    """
    Sets up the OpenSeadragon configuration for the Flask application.

    Args:
        app: The Flask application instance.
        config: The configuration object containing settings.
    """
    allowed_file_size_gb = int(
        config.settings.get("tif_loader", "pyramids_images_allowed_generation_size_gb")
    )
    allowed_file_size_byte = allowed_file_size_gb * 1024 * 1024 * 1024
    # print('allowed_file_size',allowed_file_size_byte)
    file_pattern = "[0-9]+_[0-9]+-[0-9]+_[0-9]+-[0-9]+"
    get_html_split_and_associated_file_path = (
        utils.get_html_split_and_associated_file_path
    )
    match = re.match
    Match_class = re.Match
    @logger.catch
    def openseadragon_entry(req_path):
        path_split, datapath = get_html_split_and_associated_file_path(config, request)
        # if isinstance(match(file_pattern, path_split[-1]), Match_class):
        #     datapath = os.path.split(datapath)[0]
        #     datapath = os.path.split(datapath)[0]
        logger.trace(req_path)
        # logger.info(f'{path_split},{datapath}')

        if (
            utils.split_html(datapath)[-1]
            .lower()
            .endswith(tuple(openseadragon_dtypes()))
        ):

            datapath_split = datapath.split("/")
            file_name = datapath_split[-1]
            view_path = request.path + "/osd_view"

            return render_template(
                "file_loading.html",
                gtag=config.settings.get("GA4", "gtag"),
                redirect_url=view_path,
                redirect_name="OpenSeadragon",
                description=datapath,
                file_name=file_name,
            )

        elif utils.split_html(datapath)[-1].endswith("osd_view"):
            # path_split_list = list(path_split)
            # path_split_list.remove("osd_view")
            # path_split_tuple = tuple(path_split_list)
            try:
                path_split = tuple(part for part in path_split if part != "osd_view")
                datapath = datapath.replace("/osd_view", "")
                stat = os.stat(datapath)
                file_ino = str(stat.st_ino)
                modification_time = str(stat.st_mtime)
                datapath_key = config.loadDataset(
                    file_ino + modification_time, datapath
                )
                img_obj = config.opendata[datapath_key]
                #   further check if the file has been deleted during server runing
                #   mainly used for the generated pyramid images
                # if not os.path.exists(img_obj.metadata.get('datapath')):
                #     logger.info("may delete")
                #     del config.opendata[file_ino + modification_time]
                #     datapath_key = config.loadDataset(
                #         file_ino + modification_time, datapath
                #     )
                #     img_obj = config.opendata[datapath_key]
                # logger.info(img_obj.metadata.get('datapath'))
                if img_obj.metadata.get('datapath'):
                    if not os.path.exists(img_obj.metadata.get('datapath')):
                        logger.info("files may be deleted, doing regeneration...")
                        del config.opendata[file_ino + modification_time]
                        datapath_key = config.loadDataset(
                            file_ino + modification_time, datapath
                        )
                        img_obj = config.opendata[datapath_key]
                t_values, m_values, time_index_map = _build_time_index_map(img_obj)
                if t_values:
                    t_point = len(t_values)
                else:
                    t_point = int(img_obj.metadata.get('TimePoints'))
                if m_values:
                    m_point = len(m_values)
                else:
                    m_point = 1
                is_rgb_volume = _is_rgb_volume(img_obj)
                level_shapes = []
                level_chunks = []
                for res in range(int(img_obj.metadata.get('ResolutionLevels'))):
                    shape = img_obj.metadata.get((res, 0, 0, 'shape'))
                    chunks = img_obj.metadata.get((res, 0, 0, 'chunks'))
                    if shape is None:
                        shape = img_obj.metadata.get('shape')
                    if chunks is None:
                        chunks = img_obj.metadata.get('chunks')
                    level_shapes.append(
                        {
                            "z": int(shape[-3]),
                            "y": int(shape[-2]),
                            "x": int(shape[-1]),
                        }
                    )
                    level_chunks.append(
                        {
                            "y": int(chunks[-2]),
                            "x": int(chunks[-1]),
                        }
                    )
                channel_infos = [] if is_rgb_volume else _build_channel_infos(img_obj)
                return render_template(
                    "openseadragon_temp.html",
                    height=int(img_obj.metadata.get('shape')[-2]),
                    width=int(img_obj.metadata.get('shape')[-1]),
                    # tileSize=img_obj.metadata.get('chunks')[-2:],
                    # tileHeight= int(img_obj.tile_size[-2]), 
                    # tileWidth= int(img_obj.tile_size[-1]),
                    tileHeight=img_obj.metadata.get('chunks')[-2],
                    tileWidth=img_obj.metadata.get('chunks')[-1],
                    host=config.settings.get("app", "url"),
                    parent_url="/".join(path_split),
                    # t_point=img_obj.metadata.get('TimePoints'),
                    t_point=t_point,
                    t_point_values=t_values,
                    m_point=m_point,
                    m_point_values=m_values,
                    time_index_map=time_index_map,
                    channel=img_obj.metadata.get('Channels'),
                    z_stack=img_obj.metadata.get('shape')[-3],
                    resolutionlevels=img_obj.metadata.get('ResolutionLevels') - 1,
                    level_shapes=level_shapes,
                    level_chunks=level_chunks,
                    channel_infos=channel_infos,
                    is_rgb_volume=is_rgb_volume,
                )
            except Exception as e:
                logger.error(f'{datapath}: {e}')
                return render_template(
                    "file_exception.html",
                    gtag=config.settings.get("GA4", "gtag"),
                    exception=e,
                )

        # elif utils.split_html(datapath)[-1].endswith("png"):
        elif isinstance(match(file_pattern, path_split[-1]), Match_class):
            # return 'break point'
            datapath_split = datapath.split("/")
            # The actual path excluded the r-t-c-z-y-x parameters
            datapath = "/" + os.path.join(*datapath_split[:-4])
            stat = os.stat(datapath)
            file_ino = str(stat.st_ino)
            modification_time = str(stat.st_mtime)
            datapath_key = config.loadDataset(file_ino + modification_time, datapath)
            # print('datapath', datapath)

            img_obj = config.opendata[datapath_key]

            # key = datapath_split[-7:-1]
            key = datapath_split[-4:]
            r = int(key[0])
            t = int(key[1])
            c = int(key[2])
            z, y, x = key[3].split("_")
            z = _map_z_index_to_resolution(img_obj, r, int(z))
            y = y.split("-")
            x = x.split("-")
            y = [int(x) for x in y]
            x = [int(x) for x in x]
            # return get_slice(tif_obj,key)
            img = None
            if config.cache is not None:
                # print("cache not none")
                cache_key = f"osd_{file_ino + modification_time}-{r}-{t}-{c}-{z}-{y}-{x}"
                img = config.cache.get(cache_key, default=None, retry=True)
                if img is not None:
                    logger.info("osd cache found")
            if img is None:
                chunk = img_obj[r,
                                slice(t,t+1),
                                slice(c,c+1),
                                slice(z,z+1),
                                slice(y[0],y[1]),
                                slice(x[0],x[1]),
                                ]
                logger.info(chunk.shape)
                chunk = np.squeeze(chunk)
                if len(chunk.shape) == 3 and chunk.shape[2] == 3:  # Color image
                    if chunk.dtype != np.uint8:
                        chunk = _scale_rgb_to_uint8(chunk, img_obj)
                    chunk = cv2.cvtColor(chunk, cv2.COLOR_RGB2BGR)
                elif not _is_rgb_volume(img_obj):
                    channel_info = _build_channel_infos(img_obj)[c]
                    chunk = _scale_to_uint8(
                        chunk,
                        channel_info["range_min"],
                        channel_info["range_max"],
                    )
                elif chunk.dtype != np.uint8:
                    chunk = utils.conv_np_dtypes(chunk, "uint8")

                image_stream = io.BytesIO()

                # Encode the image as PNG and write it to the in-memory byte stream
                success, encoded_image = cv2.imencode(".png", chunk)
                if not success:
                    raise RuntimeError("Failed to encode image as PNG")

                image_stream.write(encoded_image.tobytes())

                # Seek to the beginning of the stream (important)
                image_stream.seek(0)
                img = image_stream
                # img = image_stream
                # img.seek(0)

                if config.cache is not None:
                    config.cache.set(
                        cache_key, img, expire=None, tag=datapath, retry=True
                    )
                    logger.info("osd cache saved")
            return Response(img, mimetype="image/png")
        elif utils.split_html(datapath)[-1].endswith("info"):
            try:
                datapath = datapath.replace("/info", "")
                # print(datapath)

                # stat = os.stat(datapath)
                # file_ino = str(stat.st_ino)
                # modification_time = str(stat.st_mtime)
                # datapath_key = str(config.loadDataset(file_ino + modification_time, datapath))
                # tif_obj = config.opendata[datapath_key]
                stat = os.stat(datapath)
                file_ino = str(stat.st_ino)
                modification_time = str(stat.st_mtime)
                datapath_key = config.loadDataset(file_ino + modification_time, datapath)
                # print('datapath', datapath)

                img_obj = config.opendata[datapath_key]
                # file_precheck_info = tif_file_precheck(datapath)
                # meta_data_info = file_precheck_info.metaData
                # # print(asizeof.asizeof(file_precheck_info))
                # del file_precheck_info
                # gc.collect()
                json_serializable_metadata = {
                    str(key): value for key, value in img_obj.metadata.items()
                }
                return json.dumps(json_serializable_metadata, indent=4)
                # return json.dumps(img_obj.metadata)
            except Exception as e:
                logger.error(e)
                return render_template(
                    "file_exception.html",
                    gtag=config.settings.get("GA4", "gtag"),
                    exception=e,
                )
        else:
            return "No end point recognized!"

    openseadragon_entry = cross_origin(allow_headers=["Content-Type"])(
        openseadragon_entry
    )
    openseadragon_entry = app.route(openSeadragonPath + "<path:req_path>")(
        openseadragon_entry
    )
    # Not sure if it should be included or not
    openseadragon_entry = app.route(openSeadragonPath, defaults={"req_path": ""})(
        openseadragon_entry
    )
