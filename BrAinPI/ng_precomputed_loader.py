# -*- coding: utf-8 -*-
"""
Loader for image-style Neuroglancer precomputed datasets.

This aligns native precomputed image datasets with the same loader contract used
by the other BrAinPI readers, which makes them usable through both the
Neuroglancer endpoint and the virtual OME-Zarr endpoint.
"""

import json
import os

import numpy as np
from logger_tools import logger
from loader_indexing import normalize_data_key
from neuroglancer_scripts.chunk_encoding import RawChunkEncoder
from utils import loader_cache_key


class ng_precomputed_loader:
    """Load raw image-style Neuroglancer precomputed datasets as TCZYX."""

    def __init__(
        self,
        location,
        ResolutionLevelLock=None,
        verbose=None,
        squeeze=True,
        cache=None,
    ):
        self.location = location
        self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock
        self.verbose = verbose
        self.squeeze = squeeze
        self.cache = cache
        self.metaData = {}
        self.file_stat = os.stat(location)
        self.file_ino = str(self.file_stat.st_ino)
        self.modification_time = str(self.file_stat.st_mtime)

        info_path = os.path.join(location, "info")
        with open(info_path, "r", encoding="utf-8") as handle:
            self.info = json.load(handle)

        if self.info.get("type") != "image":
            raise ValueError("Only image-style Neuroglancer precomputed datasets are supported.")

        self.scales = list(self.info.get("scales", []))
        if not self.scales:
            raise ValueError("Neuroglancer precomputed info missing scales.")

        self.dtype = np.dtype(self.info["data_type"]).newbyteorder("<")
        self.TimePoints = 1
        self.Channels = int(self.info["num_channels"])
        self.ResolutionLevels = len(self.scales)
        self._encoders = {}

        for res, scale in enumerate(self.scales):
            if scale.get("encoding") != "raw":
                raise ValueError("Only raw-encoded Neuroglancer precomputed datasets are supported.")

            self._encoders[res] = RawChunkEncoder(self.info["data_type"], self.Channels)
            size_xyz = tuple(int(x) for x in scale["size"])
            resolution_xyz_nm = tuple(float(x) for x in scale["resolution"])
            chunk_xyz = tuple(int(x) for x in scale["chunk_sizes"][0])

            shape_zyx = (size_xyz[2], size_xyz[1], size_xyz[0])
            resolution_zyx_um = (
                resolution_xyz_nm[2] / 1000.0,
                resolution_xyz_nm[1] / 1000.0,
                resolution_xyz_nm[0] / 1000.0,
            )
            chunk_zyx = (chunk_xyz[2], chunk_xyz[1], chunk_xyz[0])

            for channel in range(self.Channels):
                self.metaData[res, 0, channel, "shape"] = (1, 1, *shape_zyx)
                self.metaData[res, 0, channel, "resolution"] = resolution_zyx_um
                self.metaData[res, 0, channel, "chunks"] = (1, 1, *chunk_zyx)
                self.metaData[res, 0, channel, "dtype"] = self.dtype
                self.metaData[res, 0, channel, "ndim"] = 5

        self.change_resolution_lock(self.ResolutionLevelLock)

    def change_resolution_lock(self, ResolutionLevelLock):
        """Select a default scale and update shape, chunk, and dtype fields.

        Args:
            ResolutionLevelLock: Zero-based index into the precomputed scales.
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

    def _normalize_key(self, key):
        res = self.ResolutionLevelLock
        if isinstance(key, tuple) and len(key) == self.ndim + 1:
            res, key = key[0], key[1:]
        if not 0 <= res < self.ResolutionLevels:
            raise ValueError("Resolution level is outside the precomputed pyramid.")
        return res, normalize_data_key(key, self.ndim)

    def __getitem__(self, key):
        res, key = self._normalize_key(key)
        array = self.getSlice(
            r=res,
            t=key[0],
            c=key[1],
            z=key[2],
            y=key[3],
            x=key[4],
        )

        if self.squeeze:
            return np.squeeze(array)
        return array

    def _read_chunk(self, res, x0, x1, y0, y1, z0, z1):
        filename = f"{x0}-{x1}_{y0}-{y1}_{z0}-{z1}"
        chunk_path = os.path.join(self.location, self.scales[res]["key"], filename)
        with open(chunk_path, "rb") as handle:
            buf = handle.read()
        return self._encoders[res].decode(buf, (x1 - x0, y1 - y0, z1 - z0))

    def getSlice(self, r, t, c, z, y, x):
        """Assemble a TCZYX region from intersecting raw precomputed chunks.

        Args:
            r: Scale index.
            t: Time slice; only index zero exists.
            c: Channel slice.
            z: Z slice.
            y: Y slice.
            x: X slice.

        Returns:
            numpy.ndarray: Requested region in TCZYX order.

        Raises:
            ValueError: If the scale is invalid or stepped spatial slicing is
            requested.
            IndexError: If a nonzero time point is requested.
        """
        if r >= self.ResolutionLevels:
            raise ValueError("Layer is larger than the number of ResolutionLevels")

        incomingSlices = (r, t, c, z, y, x)
        cache_key = loader_cache_key(
            self.file_ino, self.modification_time, incomingSlices
        )
        if self.cache is not None:
            cached = self.cache.get(cache_key, default=None, retry=True)
            if cached is not None:
                logger.info("ng precomputed loader cache found")
                return cached

        t_indices = np.arange(self.TimePoints)[t]
        if t_indices.size == 0:
            return np.empty((0, 0, 0, 0, 0), dtype=self.dtype)
        if np.any(t_indices != 0):
            raise IndexError("Neuroglancer precomputed loader only supports a single timepoint at index 0.")

        c_indices = np.atleast_1d(np.arange(self.Channels)[c])
        size_z = self.metaData[r, 0, 0, "shape"][-3]
        size_y = self.metaData[r, 0, 0, "shape"][-2]
        size_x = self.metaData[r, 0, 0, "shape"][-1]
        z_start, z_stop, z_step = z.indices(size_z)
        y_start, y_stop, y_step = y.indices(size_y)
        x_start, x_stop, x_step = x.indices(size_x)
        if z_step != 1 or y_step != 1 or x_step != 1:
            raise ValueError("Stepped slicing is not supported for Neuroglancer precomputed data.")

        out_shape = (len(t_indices), len(c_indices), z_stop - z_start, y_stop - y_start, x_stop - x_start)
        out = np.zeros(out_shape, dtype=self.dtype)

        chunk_z = self.metaData[r, 0, 0, "chunks"][-3]
        chunk_y = self.metaData[r, 0, 0, "chunks"][-2]
        chunk_x = self.metaData[r, 0, 0, "chunks"][-1]

        for cz in range((z_start // chunk_z) * chunk_z, z_stop, chunk_z):
            z0 = cz
            z1 = min(cz + chunk_z, size_z)
            for cy in range((y_start // chunk_y) * chunk_y, y_stop, chunk_y):
                y0 = cy
                y1 = min(cy + chunk_y, size_y)
                for cx in range((x_start // chunk_x) * chunk_x, x_stop, chunk_x):
                    x0 = cx
                    x1 = min(cx + chunk_x, size_x)

                    chunk = self._read_chunk(r, x0, x1, y0, y1, z0, z1)
                    src_z0 = max(z_start, z0) - z0
                    src_z1 = min(z_stop, z1) - z0
                    src_y0 = max(y_start, y0) - y0
                    src_y1 = min(y_stop, y1) - y0
                    src_x0 = max(x_start, x0) - x0
                    src_x1 = min(x_stop, x1) - x0

                    dst_z0 = max(z_start, z0) - z_start
                    dst_z1 = min(z_stop, z1) - z_start
                    dst_y0 = max(y_start, y0) - y_start
                    dst_y1 = min(y_stop, y1) - y_start
                    dst_x0 = max(x_start, x0) - x_start
                    dst_x1 = min(x_stop, x1) - x_start

                    out[0, :, dst_z0:dst_z1, dst_y0:dst_y1, dst_x0:dst_x1] = chunk[
                        c_indices,
                        src_z0:src_z1,
                        src_y0:src_y1,
                        src_x0:src_x1,
                    ]

        if self.cache is not None:
            self.cache.set(
                cache_key,
                out,
                expire=None,
                tag=self.file_ino + self.modification_time,
                retry=True,
            )
            logger.info("ng precomputed loader cache saved")
        return out
