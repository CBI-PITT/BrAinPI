
"""Nikon ND2 loader implementing the common TCZYX dataset contract.

The loader opens the ND2 file during construction, derives time/channel/Z
indices from sequence metadata, and caches slices using the shared
inode/mtime/slice identity.
"""

import logging
import os
import time

import numpy as np

import limnd2

from logger_tools import logger
from loader_indexing import normalize_data_key
from utils import loader_cache_key

ND2_LOADER_LOGGING = True


# Set up file-based logging for nd2_loader interface tracking
if ND2_LOADER_LOGGING:
    _nd2_loader_log = logging.getLogger("nd2_loader_interface")
    _nd2_loader_log.setLevel(logging.DEBUG)
    _nd2_loader_log.propagate = False
    if not _nd2_loader_log.handlers:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(os.path.dirname(base_dir), "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "nd2_loader_interface.log")
        try:
            os.remove(log_path)
        except FileNotFoundError:
            pass
        _log_handler = logging.FileHandler(log_path, mode="w")
        _log_handler.setLevel(logging.DEBUG)
        _log_formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S.%f",
        )
        _log_handler.setFormatter(_log_formatter)
        _nd2_loader_log.addHandler(_log_handler)
else:
    _nd2_loader_log = logging.getLogger("nd2_loader_interface")


class nd2_loader:
    """
    A class to load, validate, and process ND2 image files.
    This mirrors the public interface of tiff_loader for external integrations.
    """

    def __init__(
        self,
        file_location,
        pyramid_generation_allowed=False,
        pyramid_images_connection={},
        pyramids_images_allowed_store_size_gb=100,
        pyramids_images_allowed_generation_size_gb=10,
        pyramids_images_store=None,
        extension_type=".nd2",
        ResolutionLevelLock=None,
        squeeze_output=True,
        cache=None,
        max_chunk_edge=512,
        lut_sample_slices=5,
        lut_percentiles=(0.5, 99.5),
        lut_sample_stride=1,
    ) -> None:
        self.cache = cache
        self.squeeze = squeeze_output
        self.metaData = {}
        self.allowed_store_size_gb = float(pyramids_images_allowed_store_size_gb)
        self.allowed_file_size_gb = float(pyramids_images_allowed_generation_size_gb)
        self.pyramids_images_store = pyramids_images_store
        self.extension_type = extension_type
        self.max_chunk_edge = int(max_chunk_edge)
        self.lut_sample_slices = int(lut_sample_slices)
        self.lut_percentiles = tuple(lut_percentiles)
        self.lut_sample_stride = int(lut_sample_stride)
        self.ResolutionLevelLock = (
            0 if ResolutionLevelLock is None else ResolutionLevelLock
        )
        self.image = None
        self.open(file_location)

    def open(self, file_location: str | None = None) -> None:
        """
        Open the ND2 file and populate metadata needed for slicing.
        """
        if file_location is not None:
            self.datapath = file_location
            self.location = file_location

        if self.image is not None:
            self.close()

        self.metaData = {}
        self.file_stat = os.stat(self.datapath)
        self.file_ino = str(self.file_stat.st_ino)
        self.modification_time = str(self.file_stat.st_mtime)
        self.file_size = self.file_stat.st_size
        self.image = self.validate_nd2_file(self.datapath)
        self.filename, self.filename_extension = self.file_extension_split(self.datapath)

        self._attrs = self.image.imageAttributes
        self.height = self._attrs.height
        self.width = self._attrs.uiWidth
        self.Channels = self._attrs.componentCount

        self.type = "TCZYX"
        self.standard_axes = {"T": 0, "C": 1, "Z": 2, "Y": 3, "X": 4}
        self.axes_pos_dic = self.axes_pos_extract(self.type)

        self._seq_index_map, self._time_keys, self._z_count = self._build_sequence_map()
        self.TimePoints = len(self._time_keys)
        self.z = self._z_count

        self.axes_value_dic = self.axes_value_extract(
            self.type,
            (self.TimePoints, self.Channels, self.z, self.height, self.width),
        )

        self._downsample_levels = list(
            getattr(self._attrs, "downsampleLevels", None) or []
        )
        self.ResolutionLevels = (
            len(self._downsample_levels) if self._downsample_levels else 1
        )
        self.is_pyramidal = getattr(self.image.chunker, "hasDownsampledImages", False)

        tile_w = self._attrs.uiTileWidth or 128
        tile_h = self._attrs.uiTileHeight or 128
        self.tile_size = (tile_w, tile_h)

        calibration = getattr(self.image, "imageDataCalibration", None) or []
        if not isinstance(calibration, (list, tuple)):
            calibration = list(calibration)

        def _calibration_value(index: int, default: float = 1.0) -> float:
            try:
                value = calibration[index]
            except (IndexError, TypeError):
                return default
            if not isinstance(value, (int, float)):
                return default
            return value if value > 0 else default

        def _normalize_dtype(value) -> str:
            try:
                return np.dtype(value).name
            except Exception:
                dtype_str = str(value)
                if dtype_str.startswith("<class '") and dtype_str.endswith("'>"):
                    dtype_str = dtype_str[len("<class '") : -len("'>")]
                    return dtype_str.split(".")[-1]
                return dtype_str

        def _dtype_max(dtype_name: str) -> float:
            try:
                dt = np.dtype(dtype_name)
            except Exception:
                return 1.0
            if np.issubdtype(dt, np.integer):
                return float(np.iinfo(dt).max)
            if np.issubdtype(dt, np.floating):
                return 1.0
            return 1.0

        def _compute_channel_lut() -> tuple[list[float], list[float]] | None:
            if not self._seq_index_map:
                return None
            if self.lut_sample_slices <= 0:
                return None
            low_pct, high_pct = self.lut_percentiles
            if low_pct < 0 or high_pct > 100 or low_pct >= high_pct:
                return None
            res = max(0, self.ResolutionLevels - 1)
            down_attrs = self._attrs.makeDownsampled(res)
            shape_y = down_attrs.height
            shape_x = down_attrs.uiWidth
            if shape_x <= 0 or shape_y <= 0:
                return None
            if self.z <= 0:
                return None

            sample_count = min(self.z, self.lut_sample_slices)
            z_indices = np.linspace(0, self.z - 1, num=sample_count)
            z_indices = sorted({int(round(z)) for z in z_indices})
            stride = max(1, self.lut_sample_stride)
            rect = (0, 0, shape_x, shape_y)

            channel_samples: list[list[np.ndarray]] = [[] for _ in range(self.Channels)]
            for z_idx in z_indices:
                seq_index = self._seq_index_map.get((0, z_idx))
                if seq_index is None:
                    continue
                frame = self.image.image(
                    seq_index,
                    rect=rect,
                    downsample_level=res,
                )
                if frame is None:
                    continue
                if frame.ndim == 2:
                    frame = frame[:, :, np.newaxis]
                if stride > 1:
                    frame = frame[::stride, ::stride, :]
                for c in range(min(self.Channels, frame.shape[2])):
                    channel_samples[c].append(frame[:, :, c].ravel())

            channel_mins: list[float] = []
            channel_maxs: list[float] = []
            for c in range(self.Channels):
                if not channel_samples[c]:
                    return None
                values = np.concatenate(channel_samples[c])
                if values.size == 0:
                    return None
                low_val, high_val = np.percentile(values, [low_pct, high_pct])
                channel_mins.append(float(low_val))
                channel_maxs.append(float(high_val))
            return channel_mins, channel_maxs

        def _color_from_ui(ui_color: int) -> str | None:
            if not isinstance(ui_color, int):
                return None
            b = (ui_color >> 16) & 0xFF
            g = (ui_color >> 8) & 0xFF
            r = ui_color & 0xFF
            return f"{r:02X}{g:02X}{b:02X}"

        def _metadata_colors() -> list[str] | None:
            metadata = getattr(self.image, "pictureMetadata", None)
            if metadata is None:
                metadata = getattr(self.image, "metadata", None)
            if metadata is None:
                return None
            planes = getattr(metadata, "channels", None)
            if not planes and hasattr(metadata, "sPicturePlanes"):
                planes = getattr(metadata.sPicturePlanes, "sPlaneNew", None)
            if not planes:
                return None
            colors = []
            for plane in planes:
                color_hex = None
                if hasattr(plane, "colorAsHtmlString"):
                    try:
                        color_hex = plane.colorAsHtmlString
                        if color_hex and color_hex.startswith("#"):
                            color_hex = color_hex[1:]
                    except Exception:
                        color_hex = None
                if not color_hex and hasattr(plane, "uiColor"):
                    try:
                        color_hex = _color_from_ui(plane.uiColor)
                    except Exception:
                        color_hex = None
                if color_hex:
                    colors.append(color_hex.upper())
            return colors if colors else None

        z_um = _calibration_value(2)
        y_um = _calibration_value(3)
        x_um = _calibration_value(4)

        for r in range(self.ResolutionLevels):
            down_attrs = self._attrs.makeDownsampled(r)
            dtype = _normalize_dtype(down_attrs.dtype)
            shape_y = down_attrs.height
            shape_x = down_attrs.uiWidth
            chunk_w = min(tile_w, shape_x) if shape_x > 0 else tile_w
            chunk_h = min(tile_h, shape_y) if shape_y > 0 else tile_h
            chunk_w = min(chunk_w, self.max_chunk_edge)
            chunk_h = min(chunk_h, self.max_chunk_edge)
            for t in range(self.TimePoints):
                for c in range(self.Channels):
                    self.metaData[r, t, c, "shape"] = (
                        1,
                        1,
                        self.z,
                        shape_y,
                        shape_x,
                    )
                    self.metaData[r, t, c, "resolution"] = (
                        z_um,
                        y_um * (2**r),
                        x_um * (2**r),
                    )
                    self.metaData[r, t, c, "chunks"] = (
                        1,
                        1,
                        1,
                        chunk_h,
                        chunk_w,
                    )
                    self.metaData[r, t, c, "dtype"] = dtype
                    self.metaData[r, t, c, "ndim"] = 5

        self.metaData["datapath"] = self.datapath
        self.change_resolution_lock(self.ResolutionLevelLock)
        if not hasattr(self, "omero"):
            dtype_max = _dtype_max(self.dtype)
            lut = _compute_channel_lut()
            if lut is not None:
                channel_mins, channel_maxs = lut
            else:
                channel_mins = [0.0] * self.Channels
                channel_maxs = [dtype_max] * self.Channels
            colors = _metadata_colors()
            if not colors:
                if self.Channels == 1:
                    colors = ["00FF00"]
                else:
                    colors = [
                        "FF0000",
                        "00FF00",
                        "0000FF",
                        "FFFF00",
                        "FF00FF",
                        "00FFFF",
                    ]
            self.omero = {
                "channels": [
                    {
                        "label": f"channel{c}",
                        "window": {
                            "start": channel_mins[c],
                            "end": channel_maxs[c],
                            "min": channel_mins[c],
                            "max": channel_maxs[c],
                        },
                        "color": colors[c % len(colors)],
                        "active": True,
                    }
                    for c in range(self.Channels)
                ]
            }

    def close(self) -> None:
        """Close the underlying ND2 handle and clear the active image object."""
        if self.image is not None:
            self.image.finalize()
            self.image = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def change_resolution_lock(self, ResolutionLevelLock):
        """
        Change the resolution lock level and update metadata accordingly.
        """
        self.ResolutionLevelLock = ResolutionLevelLock
        self.shape = (
            self.TimePoints,
            self.Channels,
            self.metaData[self.ResolutionLevelLock, 0, 0, "shape"][-3],
            self.metaData[self.ResolutionLevelLock, 0, 0, "shape"][-2],
            self.metaData[self.ResolutionLevelLock, 0, 0, "shape"][-1],
        )
        self.ndim = len(self.shape)
        self.chunks = self.metaData[self.ResolutionLevelLock, 0, 0, "chunks"]
        self.resolution = self.metaData[self.ResolutionLevelLock, 0, 0, "resolution"]
        self.dtype = self.metaData[self.ResolutionLevelLock, 0, 0, "dtype"]

    def __getitem__(self, key):
        """
        Access a slice of the ND2 image.
        """
        res = 0 if self.ResolutionLevelLock is None else self.ResolutionLevelLock
        if isinstance(key, tuple) and len(key) == 6:
            res = key[0]
            if res >= self.ResolutionLevels:
                raise ValueError(
                    "Layer is larger than the number of ResolutionLevels"
                )
            key = tuple([x for x in key[1::]])

        key = normalize_data_key(key, self.ndim)
        array = self.getSlice(
            r=res,
            t=key[0],
            c=key[1],
            z=key[2],
            y=key[3],
            x=key[4],
        )

        if self.squeeze:
            result = np.squeeze(array)
            return result
        return array

    def getSlice(self, r, t, c, z, y, x):
        """
        Retrieve a slice of the image at a specific resolution and dimensions.
        """
        incomingSlices = (r, t, c, z, y, x)
        slice_start = time.perf_counter()
        image_call_time = 0.0
        image_call_count = 0
        cache_hit = False
        if self.cache is not None:
            cache_key = loader_cache_key(
                self.file_ino, self.modification_time, incomingSlices
            )
            result = self.cache.get(cache_key, default=None, retry=True)
            if result is not None:
                cache_hit = True
                output = result
                total_time = time.perf_counter() - slice_start
                _nd2_loader_log.debug(
                    "getSlice r=%s t=%s c=%s z=%s y=%s x=%s total=%.4fs image_calls=%d image_time=%.4fs cache_hit=%s",
                    r,
                    t,
                    c,
                    z,
                    y,
                    x,
                    total_time,
                    image_call_count,
                    image_call_time,
                    cache_hit,
                )
                return output

        if r >= self.ResolutionLevels:
            raise ValueError("Layer is larger than the number of ResolutionLevels")

        down_attrs = self._attrs.makeDownsampled(r)
        size_y = down_attrs.height
        size_x = down_attrs.uiWidth

        t_slice = self._normalize_slice(t, self.TimePoints)
        c_slice = self._normalize_slice(c, self.Channels)
        z_slice = self._normalize_slice(z, self.z)
        y_slice = self._normalize_slice(y, size_y)
        x_slice = self._normalize_slice(x, size_x)

        t_idx = list(range(t_slice.start, t_slice.stop, t_slice.step))
        c_idx = list(range(c_slice.start, c_slice.stop, c_slice.step))
        z_idx = list(range(z_slice.start, z_slice.stop, z_slice.step))
        y_idx = list(range(y_slice.start, y_slice.stop, y_slice.step))
        x_idx = list(range(x_slice.start, x_slice.stop, x_slice.step))

        output = np.zeros(
            (len(t_idx), len(c_idx), len(z_idx), len(y_idx), len(x_idx)),
            dtype=down_attrs.dtype,
        )

        if not (t_idx and c_idx and z_idx and y_idx and x_idx):
            total_time = time.perf_counter() - slice_start
            _nd2_loader_log.debug(
                "getSlice r=%s t=%s c=%s z=%s y=%s x=%s total=%.4fs image_calls=%d image_time=%.4fs cache_hit=%s",
                r,
                t,
                c,
                z,
                y,
                x,
                total_time,
                image_call_count,
                image_call_time,
                cache_hit,
            )
            return output

        y0 = y_idx[0]
        y1 = y_idx[-1] + 1
        x0 = x_idx[0]
        x1 = x_idx[-1] + 1
        rect = (x0, y0, x1 - x0, y1 - y0)

        for ti, t_val in enumerate(t_idx):
            for zi, z_val in enumerate(z_idx):
                seq_index = self._seq_index_map.get((t_val, z_val))
                if seq_index is None:
                    continue
                image_start = time.perf_counter()
                frame = self.image.image(
                    seq_index,
                    rect=rect,
                    downsample_level=r,
                )
                image_call_time += time.perf_counter() - image_start
                image_call_count += 1
                if frame is None:
                    continue
                if frame.ndim == 2:
                    frame = frame[:, :, np.newaxis]
                if y_slice.step != 1 or x_slice.step != 1:
                    frame = frame[:: y_slice.step, :: x_slice.step, :]
                frame = frame[:, :, c_idx]
                output[ti, :, zi, :, :] = np.moveaxis(frame, -1, 0)

        if self.cache is not None:
            self.cache.set(
                cache_key,
                output,
                expire=None,
                tag=self.file_ino + self.modification_time,
                retry=True,
            )
        total_time = time.perf_counter() - slice_start
        _nd2_loader_log.debug(
            "getSlice r=%s t=%s c=%s z=%s y=%s x=%s total=%.4fs image_calls=%d image_time=%.4fs cache_hit=%s",
            r,
            t,
            c,
            z,
            y,
            x,
            total_time,
            image_call_count,
            image_call_time,
            cache_hit,
        )
        return output

    def validate_nd2_file(self, file_path):
        """
        Validate the provided ND2 file.
        """
        try:
            return limnd2.Nd2Reader(file_path)
        except Exception as exc:
            raise Exception(f"File '{file_path}' is not a valid ND2 file. {exc}")

    def file_extension_split(self, filename):
        """
        Split the file name and extension of the ND2 file.
        """
        base, ext = os.path.splitext(filename)
        return [base, ext]

    def axes_pos_extract(self, axes):
        """
        Extract the positions of axes from their labels.
        """
        dic = {"T": None, "C": None, "Z": None, "Y": None, "X": None}
        characters = list(axes)
        for index, char in enumerate(characters):
            if char in dic:
                dic[char] = index
        return dic

    def axes_value_extract(self, axes, shape):
        """
        Extract the sizes of axes from their labels and the image shape.
        """
        dic = {"T": 1, "C": 1, "Z": 1, "Y": 1, "X": 1}
        characters = list(axes)
        for index, char in enumerate(characters):
            if char in dic:
                dic[char] = shape[index]
        return dic

    def _normalize_slice(self, slc: slice, size: int) -> slice:
        if not isinstance(slc, slice):
            slc = slice(slc, slc + 1, None)
        step = 1 if slc.step is None else slc.step
        if step <= 0:
            raise ValueError("Negative or zero slice steps are not supported.")
        start = 0 if slc.start is None else slc.start
        stop = size if slc.stop is None else slc.stop
        if start < 0:
            start += size
        if stop < 0:
            stop += size
        start = max(0, min(size, start))
        stop = max(0, min(size, stop))
        return slice(start, stop, step)

    def _set_time_key_metadata(self, time_key_names, time_keys) -> None:
        """
        Store time-key metadata for mapping multidimensional time axes.
        """
        self.time_key_names = [str(n).lower() for n in (time_key_names or [])]
        self.time_keys = []
        self.time_key_index = {}
        for idx, key in enumerate(time_keys or []):
            if not isinstance(key, tuple):
                key = (key,)
            self.time_keys.append(key)
            self.time_key_index[key] = idx

        self.time_key_values = {}
        for idx, name in enumerate(self.time_key_names):
            values = {key[idx] for key in self.time_keys}
            self.time_key_values[name] = sorted(values)

        self.TPoints = len(self.time_key_values.get("t", [0]))
        self.MPoints = len(self.time_key_values.get("m", [0]))

    def _build_sequence_map(self) -> tuple[dict[tuple[int, int], int], list[tuple], int]:
        exp = self.image.experiment
        frame_count = self._attrs.frameCount
        if exp is None:
            time_key_names = ["t"]
            time_keys = [(i,) for i in range(frame_count)]
            seq_map = {(i, 0): i for i in range(frame_count)}
            self._set_time_key_metadata(time_key_names, time_keys)
            return seq_map, time_keys, 1

        loop_names = list(exp.dimnames(skipSpectralLoop=True))
        time_key_names = [n for n in loop_names if n != "z"]
        loop_indexes = self.image.generateLoopIndexes(named=True)
        if not loop_indexes:
            time_key_names = ["t"]
            time_keys = [(i,) for i in range(frame_count)]
            seq_map = {(i, 0): i for i in range(frame_count)}
            self._set_time_key_metadata(time_key_names, time_keys)
            return seq_map, time_keys, 1

        time_keys = []
        time_key_to_index = {}
        seq_map: dict[tuple[int, int], int] = {}
        z_max = 0
        for seq_index, item in enumerate(loop_indexes):
            z_idx = item.get("z", 0)
            z_max = max(z_max, z_idx)
            time_key = tuple(item.get(n, 0) for n in time_key_names) or (0,)
            if time_key not in time_key_to_index:
                time_key_to_index[time_key] = len(time_keys)
                time_keys.append(time_key)
            t_idx = time_key_to_index[time_key]
            seq_map[(t_idx, z_idx)] = seq_index

        self._set_time_key_metadata(time_key_names, time_keys)
        return seq_map, time_keys, z_max + 1
