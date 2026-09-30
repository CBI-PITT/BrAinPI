"""JPEG 2000 loader with explicit image semantics and TIFF pyramid backing.

JP2 codestreams are inherently two-dimensional.  Their optional third decoded
axis is a component/sample axis, not automatically an RGB declaration.  This
loader maps a JP2 into one of three BrAinPI layouts:

* packed RGB: ``YXS`` -> ``TCZYX`` with RGB samples folded into ``C``; or
* component channels: ``CYX`` -> ``TCZYX`` with one logical channel per JP2
  component, including a one-component grayscale image as ``C=1``.

For normal operation, it builds one in-memory image in the source dtype by
decoding bounded JP2 windows, then creates a tiled pyramidal TIFF from that
array.  This avoids Glymur's full-image float32 working allocation while also
avoiding repeated JP2 decoding for every pyramid level.
"""

import itertools
import math
import os
import shutil
import tempfile

import glymur
import numpy as np
import tifffile
from filelock import FileLock

import tiff_loader
from logger_tools import logger
from loader_axes import empty_tczyx, finalize_tczyx, plan_tczyx_read
from loader_indexing import normalize_data_key
from utils import (
    calculate_hash,
    loader_cache_key,
    pyramid_artifact_path,
)


# Match the established JP2 loader: fewer, larger Glymur reads are much faster
# than many small windows.  The final array remains native dtype in RAM.
JP2_LOAD_CHUNK_EDGE = 10000
COLOURSPACE_SRGB = 16
COLOURSPACE_YCC = 18


def _find_colour_specification(boxes):
    """Return the first nested JP2 ``colr`` box, if a file has one."""
    for box in boxes:
        if getattr(box, "box_id", None) == "colr":
            return box
        nested = getattr(box, "box", None)
        if nested:
            found = _find_colour_specification(nested)
            if found is not None:
                return found
    return None


def _colourspace_value(colour_box):
    """Read Glymur's colour-space field across supported Glymur versions."""
    if colour_box is None:
        return None
    return getattr(colour_box, "colorspace", getattr(colour_box, "enumcs", None))


def _component_count(jp2_img):
    """Return decoded component count while rejecting non-2-D JP2 layouts."""
    shape = tuple(jp2_img.shape)
    if len(shape) == 2:
        return 1
    if len(shape) == 3 and shape[-1] >= 1:
        return int(shape[-1])
    raise TypeError(
        f"JP2 shape {shape!r} is unsupported; only 2-D YX or YXS data is supported."
    )


def detect_jp2_image_type(jp2_img):
    """Classify a JP2 as packed RGB or independent component channels."""
    components = _component_count(jp2_img)
    colour_box = _find_colour_specification(getattr(jp2_img, "box", ()))
    colours = _colourspace_value(colour_box)
    if colours in (COLOURSPACE_SRGB, COLOURSPACE_YCC):
        if components != 3:
            raise TypeError(
                f"JP2 declares RGB/YCC but decodes to {components} components."
            )
        return "rgb"

    logger.info(
        f"JP2 has {components} components without an RGB/YCC declaration; "
        "preserving them as independent channels."
    )
    return "channels"


def _jp2_axes(image_type):
    return {"rgb": "YXS", "channels": "CYX"}[image_type]


def _prepare_pixels(pixels, image_type, component_count):
    """Convert a decoded JP2 window into its TIFF storage layout."""
    pixels = np.asarray(pixels)
    if image_type == "rgb":
        if pixels.ndim == 3 and pixels.shape[-1] == 3:
            return pixels
    elif image_type == "channels":
        if component_count == 1 and pixels.ndim == 2:
            return pixels[np.newaxis, :, :]
        if pixels.ndim == 3 and pixels.shape[-1] == component_count:
            return np.moveaxis(pixels, -1, 0)
    raise ValueError(
        f"Decoded JP2 window shape {pixels.shape!r} is incompatible with "
        f"{image_type!r} interpretation."
    )


def _tile_shape(jp2_img):
    """Return a TIFF-compatible tile size based on JP2 tiling when available."""
    tile = getattr(jp2_img, "tilesize", None) or (256, 256)
    height, width = (int(tile[0]), int(tile[1]))
    # TIFF tiles must be multiples of 16.  A larger tile remains valid at image edges.
    height = max(16, int(math.ceil(height / 16.0) * 16))
    width = max(16, int(math.ceil(width / 16.0) * 16))
    return height, width


def _level_count(height, width, tile_shape):
    levels = 1
    tile_height, tile_width = tile_shape
    while height > tile_height or width > tile_width:
        height = math.ceil(height / 2)
        width = math.ceil(width / 2)
        levels += 1
    return levels


def _pyramid_level_pixels(pixels, image_type, scale):
    """Return an in-memory view of one dyadic pyramid level."""
    if image_type == "channels":
        return pixels[:, ::scale, ::scale]
    if image_type == "rgb":
        return pixels[::scale, ::scale, :]
    return pixels[::scale, ::scale]


def _full_image_shape(jp2_img, image_type, component_count):
    """Return the native-dtype buffer shape for the selected JP2 interpretation."""
    height, width = jp2_img.shape[:2]
    if image_type == "channels":
        return component_count, height, width
    if image_type == "rgb":
        return height, width, 3
    return height, width


def load_full_image_by_chunks(jp2_img, image_type, component_count):
    """Fill one native-dtype image using bounded JP2 decode windows.

    Glymur/OpenJPEG may create a float32 working buffer while decoding.  Reading
    the complete JP2 in one operation therefore has a much higher peak-memory
    cost than the final image.  Here that temporary buffer is limited to one
    window, while the retained full image remains in the source dtype.
    """
    height, width = jp2_img.shape[:2]
    pixels = np.empty(
        _full_image_shape(jp2_img, image_type, component_count), dtype=jp2_img.dtype
    )
    total_windows = math.ceil(height / JP2_LOAD_CHUNK_EDGE) * math.ceil(
        width / JP2_LOAD_CHUNK_EDGE
    )
    report_every = max(1, math.ceil(total_windows / 10))
    completed_windows = 0
    logger.info(
        f"JP2 pyramid: loading {height}x{width} into a {pixels.dtype} array "
        f"using {total_windows} decode window(s)."
    )
    for y0 in range(0, height, JP2_LOAD_CHUNK_EDGE):
        for x0 in range(0, width, JP2_LOAD_CHUNK_EDGE):
            y1 = min(y0 + JP2_LOAD_CHUNK_EDGE, height)
            x1 = min(x0 + JP2_LOAD_CHUNK_EDGE, width)
            decoded = _prepare_pixels(
                jp2_img[y0:y1, x0:x1], image_type, component_count
            )
            if image_type == "channels":
                pixels[:, y0:y1, x0:x1] = decoded
            elif image_type == "rgb":
                pixels[y0:y1, x0:x1, :] = decoded
            else:
                pixels[y0:y1, x0:x1] = decoded
            completed_windows += 1
            if completed_windows % report_every == 0 or completed_windows == total_windows:
                logger.info(
                    f"JP2 pyramid: loaded {completed_windows}/{total_windows} windows "
                    f"({completed_windows * 100 // total_windows}%)."
                )
    return pixels


def generate_tiff_pyramid(source_path, destination_path, image_type):
    """Create an atomic TIFF pyramid from one staged in-memory JP2 image."""
    jp2_img = glymur.Jp2k(source_path)
    component_count = _component_count(jp2_img)
    height, width = jp2_img.shape[:2]
    tile_shape = _tile_shape(jp2_img)
    levels = _level_count(height, width, tile_shape)
    destination_dir = os.path.dirname(destination_path)
    os.makedirs(destination_dir, exist_ok=True)
    work_dir = tempfile.mkdtemp(prefix="jp2-pyramid-", dir=destination_dir)
    temporary_tiff = os.path.join(work_dir, "pyramid.ome.tif")

    try:
        logger.success(
            f"JP2 pyramid: generating {levels} TIFF level(s) for "
            f"{os.path.basename(source_path)} as {_jp2_axes(image_type)}."
        )
        pixels = load_full_image_by_chunks(jp2_img, image_type, component_count)
        logger.success(
            f"JP2 pyramid: staged {pixels.nbytes / 1024**2:.1f} MiB in memory."
        )
        photometric = "rgb" if image_type == "rgb" else "minisblack"
        metadata = {"axes": _jp2_axes(image_type)}
        with tifffile.TiffWriter(temporary_tiff, bigtiff=True) as tif:
            for level in range(levels):
                scale = 2**level
                level_data = _pyramid_level_pixels(pixels, image_type, scale)
                logger.info(
                    f"JP2 pyramid level {level}: writing TIFF data "
                    f"with shape {level_data.shape}."
                )
                tif.write(
                    level_data,
                    subifds=levels - 1 if level == 0 else None,
                    subfiletype=1 if level else 0,
                    tile=tile_shape,
                    photometric=photometric,
                    metadata=metadata if level == 0 else None,
                )
                logger.success(f"JP2 pyramid level {level}: TIFF write complete.")
        os.replace(temporary_tiff, destination_path)
        logger.success(f"JP2 pyramid: saved {destination_path}.")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


class jp2_loader:
    """Load JP2 with distinct source, generated-pyramid, and active readers."""

    def __init__(
        self,
        location,
        pyramid_generation_allowed=False,
        pyramids_images_allowed_generation_size_gb=2,
        pyramids_images_store=None,
        extension_type=".ome.tif",
        ResolutionLevelLock=None,
        verbose=None,
        squeeze=True,
        cache=None,
    ):
        self.source_path = os.fspath(location)
        self.active_path = self.source_path
        self.pyramid_path = None
        self.uses_pyramid = False
        self.pyramid_reader = None
        self.squeeze = squeeze
        self.cache = cache
        self.verbose = verbose
        self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock
        self.pyramid_generation_allowed = pyramid_generation_allowed
        self.pyramids_images_store = (
            os.path.expanduser(pyramids_images_store) if pyramids_images_store else None
        )
        self.extension_type = extension_type
        self.allowed_file_size_byte = float(pyramids_images_allowed_generation_size_gb) * 1024**3

        self.source_stat = os.stat(self.source_path)
        self.source_ino = str(self.source_stat.st_ino)
        self.source_mtime = str(self.source_stat.st_mtime)
        self.source_size = self.source_stat.st_size
        self.source_hash = calculate_hash(self.source_ino + self.source_mtime)
        self.source_reader = glymur.Jp2k(self.source_path)
        self.active_reader = self.source_reader
        glymur.set_option("lib.num_threads", 4)
        self.component_count = _component_count(self.source_reader)
        self.image_type = detect_jp2_image_type(self.source_reader)
        self.tile_size = _tile_shape(self.source_reader)

        logger.info(
            f"JP2 {self.source_path} detected as {self.image_type}: "
            f"source shape={self.source_reader.shape}, components={self.component_count}, "
            f"axes={_jp2_axes(self.image_type)}"
        )

        if pyramid_generation_allowed:
            self._open_tiff_backing()
        else:
            self._set_direct_metadata()
        self.change_resolution_lock(self.ResolutionLevelLock)

    def _pyramid_path(self):
        if not self.pyramids_images_store:
            raise ValueError("JP2 pyramid storage is not configured.")
        return pyramid_artifact_path(
            self.pyramids_images_store,
            self.source_hash,
            self.extension_type,
        )

    def _open_tiff_backing(self):
        pyramid_path = self._pyramid_path()
        if not os.path.exists(pyramid_path):
            if self.source_size > self.allowed_file_size_byte:
                raise ValueError(
                    f"JP2 file is too large to generate a pyramid: {self.source_size} bytes "
                    f"exceeds the configured {self.allowed_file_size_byte} byte limit."
                )
            os.makedirs(os.path.dirname(pyramid_path), exist_ok=True)
            lock = FileLock(pyramid_path + ".lock")
            with lock:
                if not os.path.exists(pyramid_path):
                    logger.info(f"Generating TIFF pyramid for JP2: {self.source_path}")
                    generate_tiff_pyramid(self.source_path, pyramid_path, self.image_type)

        self.pyramid_path = pyramid_path
        self.pyramid_reader = tiff_loader.tiff_loader(
            self.pyramid_path,
            pyramid_generation_allowed=False,
            pyramids_images_allowed_generation_size_gb=self.allowed_file_size_byte / 1024**3,
            pyramids_images_store=self.pyramids_images_store,
            extension_type=self.extension_type,
            ResolutionLevelLock=self.ResolutionLevelLock,
            squeeze=self.squeeze,
            # The outer JP2 loader owns caching so keys always use the original
            # JP2 identity rather than the generated TIFF artifact identity.
            cache=None,
        )
        self.uses_pyramid = True
        self.active_path = self.pyramid_path
        self.active_reader = self.pyramid_reader
        self._copy_tiff_metadata()

    def _copy_tiff_metadata(self):
        for name in (
            "shape", "ndim", "chunks", "resolution", "dtype", "TimePoints",
            "ResolutionLevels", "Channels", "ResolutionLevelLock",
        ):
            setattr(self, name, getattr(self.pyramid_reader, name))
        self.metaData = dict(self.pyramid_reader.metaData)
        self.metaData["source_path"] = self.source_path
        self.metaData["pyramid_path"] = self.pyramid_path
        self.metaData["active_path"] = self.active_path
        self.metaData["uses_pyramid"] = self.uses_pyramid
        self.metaData["jp2_image_type"] = self.image_type
        self.metaData["jp2_component_count"] = self.component_count
        self.metaData["packed_rgb"] = self.image_type == "rgb"
        self.metaData["samples_folded_into_channels"] = self.image_type == "rgb"

    def _set_direct_metadata(self):
        height, width = self.source_reader.shape[:2]
        self.TimePoints = 1
        self.Channels = self.component_count
        self.ResolutionLevels = _level_count(height, width, self.tile_size)
        self.metaData = {
            "source_path": self.source_path,
            "pyramid_path": self.pyramid_path,
            "active_path": self.active_path,
            "uses_pyramid": self.uses_pyramid,
            "jp2_image_type": self.image_type,
            "jp2_component_count": self.component_count,
            "packed_rgb": self.image_type == "rgb",
            "samples_folded_into_channels": self.image_type == "rgb",
        }
        for resolution in range(self.ResolutionLevels):
            scale = 2**resolution
            shape = (1, 1, 1, math.ceil(height / scale), math.ceil(width / scale))
            chunks = (1, 1, 1, self.tile_size[0], self.tile_size[1])
            for timepoint, channel in itertools.product(
                range(self.TimePoints), range(self.Channels)
            ):
                self.metaData[resolution, timepoint, channel, "shape"] = shape
                self.metaData[resolution, timepoint, channel, "resolution"] = (
                    1.0,
                    float(scale),
                    float(scale),
                )
                self.metaData[resolution, timepoint, channel, "chunks"] = chunks
                self.metaData[resolution, timepoint, channel, "dtype"] = self.source_reader.dtype
                self.metaData[resolution, timepoint, channel, "ndim"] = 5

    def change_resolution_lock(self, resolution_level_lock):
        """Select the default resolution and refresh array-like attributes.

        Args:
            resolution_level_lock: Zero-based pyramid level.

        Raises:
            ValueError: If the requested level does not exist.
        """
        if self.uses_pyramid:
            self.pyramid_reader.change_resolution_lock(resolution_level_lock)
            self._copy_tiff_metadata()
            return
        if not 0 <= resolution_level_lock < self.ResolutionLevels:
            raise ValueError("Resolution level is outside the JP2 pyramid.")
        self.ResolutionLevelLock = resolution_level_lock
        metadata = self.metaData[resolution_level_lock, 0, 0, "shape"]
        self.shape = (self.TimePoints, self.Channels, *metadata[-3:])
        self.ndim = 5
        self.chunks = self.metaData[resolution_level_lock, 0, 0, "chunks"]
        self.resolution = self.metaData[resolution_level_lock, 0, 0, "resolution"]
        self.dtype = self.metaData[resolution_level_lock, 0, 0, "dtype"]

    def __getitem__(self, key):
        resolution = self.ResolutionLevelLock
        if isinstance(key, tuple) and len(key) == 6:
            resolution, key = key[0], key[1:]
        if not 0 <= resolution < self.ResolutionLevels:
            raise ValueError("Resolution level is outside the JP2 pyramid.")
        key = normalize_data_key(key, self.ndim)
        result = self.getSlice(resolution, *key)
        return np.squeeze(result) if self.squeeze else result

    def getSlice(self, r, t, c, z, y, x):
        """Read one TCZYX selection from direct JP2 or TIFF pyramid backing.

        Args:
            r: Resolution level.
            t: Time slice; JP2 contains one logical time point.
            c: Logical component/channel slice.
            z: Z slice; JP2 contains one logical plane.
            y: Y slice.
            x: X slice.

        Returns:
            numpy.ndarray: Selection with loader dimensions retained.
        """
        incomingSlices = (r, t, c, z, y, x)
        cache_key = None
        cache = getattr(self, "cache", None)
        if cache is not None:
            cache_key = loader_cache_key(
                self.source_ino, self.source_mtime, incomingSlices
            )
            result = cache.get(cache_key, default=None, retry=True)
            if result is not None:
                logger.info("JP2 loader cache found")
                return result
        if self.uses_pyramid:
            result = self.pyramid_reader.getSlice(r, t, c, z, y, x)
            if cache is not None:
                cache.set(
                    cache_key,
                    result,
                    expire=None,
                    tag=self.source_ino + self.source_mtime,
                    retry=True,
                )
                logger.info("JP2 loader cache saved")
            return result
        scale = 2**r
        height, width = self.source_reader.shape[:2]
        level_height = math.ceil(height / scale)
        level_width = math.ceil(width / scale)
        source_axes = "YXS" if self.component_count > 1 else "YX"
        source_shape = (
            (level_height, level_width, self.component_count)
            if self.component_count > 1
            else (level_height, level_width)
        )
        plan = plan_tczyx_read(
            (t, c, z, y, x),
            (1, self.component_count, 1, level_height, level_width),
            source_axes,
            source_shape,
        )
        if plan.empty:
            result = empty_tczyx(plan, self.dtype)
        else:
            y_read = plan.read_key[0]
            x_read = plan.read_key[1]
            source_key = (
                slice(
                    y_read.start * scale,
                    min(y_read.stop * scale, height),
                    scale,
                ),
                slice(
                    x_read.start * scale,
                    min(x_read.stop * scale, width),
                    scale,
                ),
            )
            if self.component_count > 1:
                source_key = (*source_key, plan.read_key[2])
            decoded = self.source_reader[source_key]
            result = finalize_tczyx(decoded, plan)

        if cache is not None:
            cache.set(
                cache_key,
                result,
                expire=None,
                tag=self.source_ino + self.source_mtime,
                retry=True,
            )
            logger.info("JP2 loader cache saved")
        return result
