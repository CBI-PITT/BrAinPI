"""NIfTI and NIfTI-Zarr loading with optional multiscale cache generation.

The loader normalizes OME-style array axes to TCZYX. Single-field structured
dtypes are exposed through their numeric field; ambiguous multi-field dtypes
are rejected.
"""

import zarr, os, itertools
import numpy as np
import shutil
import subprocess
import sys
import time
import nibabel as nib
from filelock import FileLock
from zarr.storage import LocalStore
from collections.abc import MutableMapping
from zarr.abc.store import (
    Store
)
from typing import Union
StoreLike = Union[ Store, MutableMapping]
from logger_tools import logger
from loader_axes import (
    empty_tczyx,
    execute_array_read,
    finalize_tczyx,
    plan_tczyx_read,
)
from loader_indexing import normalize_data_key
import gc
from utils import (
    calculate_hash,
    loader_cache_key,
    pyramid_artifact_path,
)


def run_generation_subprocess(inp, out, time_axe):
    """Run NIfTI conversion in a fresh interpreter, safe from gthread forks."""
    worker = os.path.join(os.path.dirname(__file__), "nifti_generation_worker.py")
    command = [sys.executable, worker, os.fspath(inp), os.fspath(out)]
    if time_axe:
        command.append("--no-time")
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip()
        message = (
            f"NIfTI pyramid generator exited with code {completed.returncode}"
        )
        if details:
            message = f"{message}\n{details}"
        raise RuntimeError(message)

class nifti_zarr_loader:
    """
    Load NIfTI while retaining its nibabel ``source_reader`` separately from
    the generated Zarr ``pyramid_reader`` selected through ``active_reader``.
    """
    def __init__(
        self,
        location,
        pyramid_generation_allowed=False,
        pyramids_images_allowed_generation_size_gb=2,
        pyramids_images_store=None,
        extension_type=".nii.zarr",
        ResolutionLevelLock=None,
        zarr_store_type: StoreLike = LocalStore,
        verbose=None,
        squeeze=True,
        cache=None,
    ):
        """
        Initialize the nifti_zarr_loader object.

        Args:
            location (str): Path to the NIfTI file.
            pyramid_generation_allowed (bool): Whether a raw ``.nii`` or
                ``.nii.gz`` source may be converted to a generated Zarr
                pyramid. When false, raw NIfTI is exposed lazily as one native
                resolution through nibabel; existing ``.nii.zarr`` sources do
                not require this permission.
            pyramids_images_allowed_generation_size_gb (float): Maximum allowed size for pyramid generation in GB. Defaults to 2.
            pyramids_images_store (str, optional): Directory for storing pyramid images. Defaults to None.
            extension_type (str, optional): File extension for the generated pyramid images. Defaults to ".nii.zarr".
            ResolutionLevelLock (int, optional): Lock for accessing a specific resolution level. Defaults to None.
            zarr_store_type (zarr.storage, optional): Zarr store type. Defaults to LocalStore.
            verbose (bool, optional): Verbose logging. Defaults to None.
            squeeze (bool, optional): Whether to remove singleton dimensions from arrays. Defaults to True.
            cache (object, optional): Cache object for storing slices. Defaults to None.
        """
        self.source_path = os.fspath(location)
        self.pyramid_generation_allowed = pyramid_generation_allowed
        self.active_path = self.source_path
        self.pyramid_path = None
        self.uses_pyramid = False
        self.source_reader = None
        self.pyramid_reader = None
        self.active_reader = None
        self.is_native_nifti = False
        self.ResolutionLevelLock = (
            0 if ResolutionLevelLock is None else ResolutionLevelLock
        )
        self.source_stat = os.stat(self.source_path)
        self.filename = os.path.split(self.source_path)[1]
        self.source_ino = str(self.source_stat.st_ino)
        self.source_mtime = str(self.source_stat.st_mtime)
        self.source_size = self.source_stat.st_size
        self.source_hash = calculate_hash(self.source_ino + self.source_mtime)
        self.allowed_file_size_gb = float(pyramids_images_allowed_generation_size_gb)
        self.allowed_file_size_byte = self.allowed_file_size_gb * 1024 * 1024 * 1024
        self.pyramids_images_store = pyramids_images_store
        self.extension_type = extension_type
        self.verbose = verbose
        self.squeeze = squeeze
        self.cache = cache
        self.metaData = {}
        # Go through pyramid generation process for nii.gz files
        if self.source_path.endswith(".nii.gz") or self.source_path.endswith(".nii"):
            self.source_reader = self.validate_nifti_file(self.source_path)
            if not self.pyramid_generation_allowed:
                self._initialize_native_nifti()
                return
            self.pyramid_builders()
        # Open zarr store
        self.zarr_store = zarr_store_type  # Only relevant for non-s3 datasets
        store = self.zarr_store_type(self.active_path)
        zgroup = zarr.open(store)
        if self.uses_pyramid:
            self.pyramid_reader = zgroup
        else:
            self.source_reader = zgroup
        self.active_reader = zgroup
        self.zattrs = zgroup.attrs

        if "omero" in self.zattrs:
            self.omero = zgroup.attrs["omero"]
        # assert 'omero' in self.zattrs
        # self.omero = zgroup.attrs['omero']
        assert "multiscales" in self.zattrs
        self.multiscales = zgroup.attrs["multiscales"]
        try:
            self.axes = self.multiscales[0]["axes"]
        except:
            self.axes = self.multiscales["axes"]
        # self.axes = self.multiscales[0]['axes']
        if len(self.axes) < 3:
            raise Exception()
        self.source_axes = "".join(axis["name"] for axis in self.axes).upper()
        self.axes_pos_dic = {"t": None, "c": None, "z": None, "y": None, "x": None}
        self.space_unit = None
        for index, axe in enumerate(self.axes):
            self.axes_pos_dic[axe["name"]] = index
            if axe["type"] == "space":
                self.space_unit = axe["unit"]
        logger.info(self.axes_pos_dic)
        logger.info(self.multiscales)
        logger.info(self.space_unit)
        del store

        self.metaData["source_path"] = self.source_path
        self.metaData["pyramid_path"] = self.pyramid_path
        self.metaData["active_path"] = self.active_path
        self.metaData["uses_pyramid"] = self.uses_pyramid

        try:
            self.multiscale_datasets = self.multiscales[0]["datasets"]
        except:
            self.multiscale_datasets = self.multiscales["datasets"]

        self.ResolutionLevels = len(self.multiscale_datasets)

        self.dataset_paths = []
        self.dataset_scales = []
        self.dataset_translations = []
        self.arrays = {}
        self._structured_fields = {}
        for r in range(self.ResolutionLevels):
            dataset = self.multiscale_datasets[r]
            self.dataset_paths.append(dataset["path"])
            transformations = dataset.get("coordinateTransformations", [])
            scale = next(
                (item.get("scale") for item in transformations
                 if item.get("type") == "scale"),
                None,
            )
            if scale is None:
                raise ValueError(f"NIfTI-Zarr resolution {r} is missing its scale transform")
            translation = next(
                (item.get("translation") for item in transformations
                 if item.get("type") == "translation"),
                None,
            )
            self.dataset_scales.append(scale)
            self.dataset_translations.append(translation)
            array = self.open_array(r)
            dtype_fields = array.dtype.fields
            if dtype_fields is None:
                self._structured_fields[r] = None
                logical_dtype = array.dtype
            elif len(dtype_fields) == 1:
                field_name = next(iter(dtype_fields))
                self._structured_fields[r] = field_name
                logical_dtype = np.dtype(dtype_fields[field_name][0])
            else:
                raise TypeError(
                    "NIfTI-Zarr arrays with multiple structured dtype fields "
                    f"are not supported: {tuple(dtype_fields)}"
                )
            if r == 0:
                self.TimePoints = (
                    array.shape[self.axes_pos_dic["t"]]
                    if self.axes_pos_dic["t"] is not None
                    else 1
                )
                self.Channels = (
                    array.shape[self.axes_pos_dic["c"]]
                    if self.axes_pos_dic["c"] is not None
                    else 1
                )
            # shape_z = array.shape[self.dim_pos_dic['z']]
            # shape_y = array.shape[self.dim_pos_dic['y']]
            # shape_x = array.shape[self.dim_pos_dic['x']]
            # if shape_z <= 64 and shape_y <= 64 and shape_x <=64:
            #     self.ResolutionLevels = r + 1
            #     break

            for t, c in itertools.product(range(self.TimePoints), range(self.Channels)):

                # Collect attribute info
                # self.metaData[r, t, c, "shape"] = array.shape
                # shape = array.shape
                # if len(shape) != 5:
                #     # Prepend 1 to the shape to make its length 5
                #     new_shape = (1,) * (5 - len(shape)) + shape
                #     self.metaData[r, t, c, "shape"] = new_shape
                # else:
                #     self.metaData[r, t, c, "shape"] = shape
                shape_z = array.shape[self.axes_pos_dic.get("z")] if self.axes_pos_dic.get("z") is not None else 1
                shape_y = array.shape[self.axes_pos_dic.get("y")] if self.axes_pos_dic.get("y") is not None else 1
                shape_x = array.shape[self.axes_pos_dic.get("x")] if self.axes_pos_dic.get("x") is not None else 1
                self.metaData[r, t, c, 'shape'] = (1, 1, shape_z, shape_y, shape_x)

                # change to um if mm
                if self.space_unit == "mm" or self.space_unit == "millimeter":
                    self.metaData[r, t, c, "resolution"] = (
                        self.dataset_scales[r][self.axes_pos_dic['z']] * 1000 if self.axes_pos_dic['z'] is not None else 1000,
                        self.dataset_scales[r][self.axes_pos_dic['y']] * 1000 if self.axes_pos_dic['y'] is not None else 1000,
                        self.dataset_scales[r][self.axes_pos_dic['x']] * 1000 if self.axes_pos_dic['x'] is not None else 1000)
                    # self.metaData[r, t, c, "resolution"] = [val * 1000 for val in self.dataset_scales[r][-3:]]
                else:
                    self.metaData[r, t, c, "resolution"] = (self.dataset_scales[r][self.axes_pos_dic['z']] if self.axes_pos_dic['z'] is not None else 1,
                    self.dataset_scales[r][self.axes_pos_dic['y']] if self.axes_pos_dic['y'] is not None else 1,
                    self.dataset_scales[r][self.axes_pos_dic['x']] if self.axes_pos_dic['x'] is not None else 1)
                    # self.metaData[r, t, c, "resolution"] = self.dataset_scales[r][-3:]

                if self.dataset_translations[r] is not None:
                    spatial_translation = tuple(
                        self.dataset_translations[r][self.axes_pos_dic[axis]]
                        if self.axes_pos_dic[axis] is not None else 0
                        for axis in ("z", "y", "x")
                    )
                    if self.space_unit == "mm" or self.space_unit == "millimeter":
                        spatial_translation = tuple(
                            value * 1000 for value in spatial_translation
                        )
                    self.metaData[r, t, c, "translation"] = spatial_translation

                # Collect dataset info
                self.metaData[r, t, c, "chunks"] = (1,1, array.chunks[self.axes_pos_dic['z'] if self.axes_pos_dic['z'] is not None else 1],
                array.chunks[self.axes_pos_dic['y'] if self.axes_pos_dic['y'] is not None else 1],
                array.chunks[self.axes_pos_dic['x'] if self.axes_pos_dic['x'] is not None else 1])
                # self.metaData[r, t, c, "chunks"] = (1, 1, *array.chunks[-3:])
                # dtype = array.dtype
                # if dtype == "int8":
                #     dtype = "uint8"
                # elif dtype == "int16":
                #     dtype = "uint16"
                # elif dtype == "float64" or dtype == "float16":
                #     dtype = "float32"
                self.metaData[r, t, c, "dtype"] = logical_dtype
                self.metaData[r, t, c, "ndim"] = array.ndim
                if self._structured_fields[r] is not None:
                    self.metaData[r, t, c, "source_dtype"] = array.dtype
                    self.metaData[r, t, c, "source_field"] = self._structured_fields[r]

                try:
                    self.metaData[r, t, c, "max"] = self.omero["channels"][c]["window"][
                        "end"
                    ]
                    self.metaData[r, t, c, "min"] = self.omero["channels"][c]["window"][
                        "start"
                    ]
                except:
                    pass
            self.arrays[r] = array

            # may not need
            shape_z = array.shape[self.axes_pos_dic["z"]]
            shape_y = array.shape[self.axes_pos_dic["y"]]
            shape_x = array.shape[self.axes_pos_dic["x"]]
            if shape_z <= 64 and shape_y <= 64 and shape_x <= 64:
                self.ResolutionLevels = r + 1
                break

        self.change_resolution_lock(self.ResolutionLevelLock)
        # logger.info(self.metaData)

    def _initialize_native_nifti(self):
        """Expose a raw NIfTI through its lazy nibabel ArrayProxy."""
        source_shape = tuple(self.source_reader.shape)
        if not 3 <= len(source_shape) <= 5:
            raise TypeError(
                f"Native NIfTI shape {source_shape!r} is unsupported; "
                "expected XYZ, XYZT, or XYZTC."
            )
        self.source_axes = "XYZTC"[: len(source_shape)]
        self.active_reader = self.source_reader
        self.active_path = self.source_path
        self.is_native_nifti = True
        self.uses_pyramid = False
        self.ResolutionLevels = 1
        self.TimePoints = source_shape[3] if len(source_shape) >= 4 else 1
        self.Channels = source_shape[4] if len(source_shape) >= 5 else 1
        self.shape = (
            self.TimePoints,
            self.Channels,
            source_shape[2],
            source_shape[1],
            source_shape[0],
        )
        self.ndim = 5
        self.dtype = np.dtype(self.source_reader.get_data_dtype())
        zooms = tuple(float(value) for value in self.source_reader.header.get_zooms())
        # NIfTI spatial units are millimeters unless the header declares another
        # unit. BrAinPI advertises spatial resolution in micrometers.
        spatial_unit, _time_unit = self.source_reader.header.get_xyzt_units()
        unit_scale = {
            "meter": 1_000_000.0,
            "mm": 1_000.0,
            "micron": 1.0,
            "unknown": 1_000.0,
        }.get(spatial_unit, 1_000.0)
        self.resolution = tuple(
            zooms[index] * unit_scale for index in (2, 1, 0)
        )
        self.chunks = (
            1,
            1,
            1,
            min(256, self.shape[-2]),
            min(256, self.shape[-1]),
        )
        self.metaData.update(
            {
                "source_path": self.source_path,
                "pyramid_path": None,
                "active_path": self.active_path,
                "uses_pyramid": False,
                "source_axes": self.source_axes,
            }
        )
        for timepoint, channel in itertools.product(
            range(self.TimePoints), range(self.Channels)
        ):
            self.metaData[0, timepoint, channel, "shape"] = self.shape
            self.metaData[0, timepoint, channel, "resolution"] = self.resolution
            self.metaData[0, timepoint, channel, "chunks"] = self.chunks
            self.metaData[0, timepoint, channel, "dtype"] = self.dtype
            self.metaData[0, timepoint, channel, "ndim"] = self.ndim

    def validate_nifti_file(self, file_path):
        """Open and retain the original NIfTI image.

        Args:
            file_path (str): Path to the NIfTI file.

        Returns:
            nibabel.spatialimages.SpatialImage: Original source reader. The
            per-source generation-size limit is checked only if no reusable
            pyramid exists.
        """
        return nib.load(file_path)

    def pyramid_builders(self):
        """
        Build a pyramid structure for the NIfTI file.

        The original NIfTI path and identity are read from ``source_*``.
        """
        pyramid_image_location = pyramid_artifact_path(
            self.pyramids_images_store,
            self.source_hash,
            self.extension_type,
        )
        if os.path.exists(pyramid_image_location):
            logger.info("Using existing generated NIfTI pyramid")
        else:
            if self.source_size > self.allowed_file_size_byte:
                raise ValueError(
                    f"File '{self.filename}' cannot generate a pyramid: "
                    f"{self.source_size} bytes exceeds the configured "
                    f"{self.allowed_file_size_byte} byte limit."
                )
            self.pyramid_building_process(
                self.source_path,
                False,
                pyramid_image_location,
            )
        self.pyramid_path = pyramid_image_location
        self.uses_pyramid = True
        self.active_path = self.pyramid_path

    def pyramid_building_process(
        self,
        nifti_file_location,
        time_axe,
        pyramid_image_location,
    ):
        """
        Generate a pyramid structure for a NIfTI file and store it in a specified location.

        This method processes a NIfTI file to create a multi-resolution pyramid structure for 
        efficient image storage and retrieval. It handles multiprocessing, file locking, and 
        storage management to ensure safe and efficient execution.

        Args:
            nifti_file_location (str): The file path to the input NIfTI file.
            time_axe (bool): If True, the time axis is ignored during the conversion process.
            pyramid_image_location (str): The final location of the generated pyramid image.
        """
        pyramids_images_store_dir = os.path.dirname(pyramid_image_location)
        os.makedirs(pyramids_images_store_dir, exist_ok=True)
        file_temp = os.path.join(
            pyramids_images_store_dir,
            "temp_" + os.path.basename(pyramid_image_location),
        )
        file_lock = FileLock(pyramid_image_location + ".lock")
        try:
            with file_lock.acquire():
                logger.success("File lock acquired.")
                if not os.path.exists(pyramid_image_location):
                    if os.path.isdir(file_temp):
                        shutil.rmtree(file_temp)
                    elif os.path.exists(file_temp):
                        os.remove(file_temp)
                    logger.success(f"==> Pyramid image is building...")
                    start_time = time.time()
                    run_generation_subprocess(
                        nifti_file_location,
                        file_temp,
                        time_axe,
                    )
                    logger.success("Process complete!")

                    end_time = time.time()
                    execution_time = end_time - start_time
                    os.replace(file_temp, pyramid_image_location)
                    logger.success(
                        f"{nifti_file_location} connected to ==> {pyramid_image_location}"
                    )
                    logger.success(
                        f"Pyramid image building complete {nifti_file_location} total execution time: {execution_time}"
                    )
                else:
                    logger.info("File detected!")
                    if os.path.exists(file_temp):
                        logger.warning('file_temp exist!')
                        shutil.rmtree(file_temp)
        except Exception as e:
            if os.path.isdir(file_temp):
                shutil.rmtree(file_temp)
            elif os.path.exists(file_temp):
                os.remove(file_temp)
            logger.exception(f"An error occurred during generation process: {e}")
            raise
        finally:
            if "data" in locals():
                del data
            gc.collect()
            logger.success("Resources cleaned up.")
            

    def zarr_store_type(self, path):
        """
        Return the appropriate Zarr store for the dataset.

        Args:
            path (str): Path to the dataset.

        Returns:
            zarr.storage: The Zarr store object.
        """
        return self.zarr_store(path)

    def change_resolution_lock(self, ResolutionLevelLock):
        """
        Update the resolution lock and associated metadata.

        Args:
            ResolutionLevelLock (int): The resolution level to lock.
        """
        self.ResolutionLevelLock = ResolutionLevelLock
        # self.shape = self.metaData[self.ResolutionLevelLock, 0, 0, "shape"]
        self.shape = (
            self.TimePoints,
            self.Channels,
            self.metaData[self.ResolutionLevelLock, 0, 0, 'shape'][-3],
            self.metaData[self.ResolutionLevelLock, 0, 0, 'shape'][-2],
            self.metaData[self.ResolutionLevelLock, 0, 0, 'shape'][-1]
        )
        self.ndim = len(self.shape)
        self.chunks = self.metaData[self.ResolutionLevelLock, 0, 0, "chunks"]
        self.resolution = self.metaData[self.ResolutionLevelLock, 0, 0, "resolution"]
        self.dtype = self.metaData[self.ResolutionLevelLock, 0, 0, "dtype"]

    def __getitem__(self, key):
        """
        Access a specific slice of the NIfTI dataset.

        Args:
            key (int, slice, or tuple): Slice specifying the data to access.

        Returns:
            np.ndarray: The requested data slice, optionally squeezed.
        """
        res = 0 if self.ResolutionLevelLock is None else self.ResolutionLevelLock
        logger.info(key)
        if isinstance(key, tuple) and len(key) == 6:
            res = key[0]
            if res >= self.ResolutionLevels:
                raise ValueError("Layer is larger than the number of ResolutionLevels")
            key = tuple([x for x in key[1::]])
        logger.info(res)
        logger.info(key)

        key = normalize_data_key(key, self.ndim)
        logger.info(key)

        array = self.getSlice(r=res, t=key[0], c=key[1], z=key[2], y=key[3], x=key[4])

        if self.squeeze:
            return np.squeeze(array)
        return array

    def _get_memorize_cache(
        self, name=None, typed=False, expire=None, tag=None, ignore=()
    ):
        if tag is None:
            tag = self.active_path
        return (
            self.cache.memorize(
                name=name, typed=typed, expire=expire, tag=tag, ignore=ignore
            )
            if self.cache is not None
            else lambda x: x
        )

    def getSlice(self, r, t, c, z, y, x):
        """
        Retrieve a 3D chunk of data for the specified coordinates.

        Args:
            r (int): Resolution level.
            t (slice): Time dimension slice.
            c (slice): Channel dimension slice.
            z (slice): Z-axis slice.
            y (slice): Y-axis slice.
            x (slice): X-axis slice.

        Returns:
            np.ndarray: The requested 3D chunk of data.
        """

        incomingSlices = (r, t, c, z, y, x)
        logger.info(incomingSlices)
        if self.cache is not None:
            key = loader_cache_key(self.source_ino, self.source_mtime, incomingSlices)
            result = self.cache.get(key, default=None, retry=True)
            if result is not None:
                logger.info(f"loader cache found")
                return result
        if self.is_native_nifti:
            if r != 0:
                raise ValueError("Native NIfTI exposes only resolution level 0.")
            plan = plan_tczyx_read(
                (t, c, z, y, x),
                self.shape,
                self.source_axes,
                self.source_reader.shape,
            )
            result = execute_array_read(
                self.source_reader.dataobj, plan, dtype=self.dtype
            )
            if self.cache is not None:
                self.cache.set(
                    key,
                    result,
                    expire=None,
                    tag=self.source_ino + self.source_mtime,
                    retry=True,
                )
                logger.info("native NIfTI loader cache saved")
            return result
        source_array = self.arrays[r]
        logical_shape = (
            self.TimePoints,
            self.Channels,
            *self.metaData[r, 0, 0, "shape"][-3:],
        )
        plan = plan_tczyx_read(
            (t, c, z, y, x),
            logical_shape,
            self.source_axes,
            source_array.shape,
        )
        structured_field = self._structured_fields[r]
        if plan.empty:
            result = empty_tczyx(plan, self.metaData[r, 0, 0, "dtype"])
        else:
            source_result = source_array[plan.read_key]
            if structured_field is not None:
                source_result = source_result[structured_field]
            result = finalize_tczyx(source_result, plan)
        # if len(result.shape) < 4:
        #     result = np.expand_dims(result, axis=0)
        # result = result.astype('uint16')
        logger.info(result.shape)
        if self.cache is not None:
            # print("Cache Status:")
            # shards_limit = self.cache.size_limit / (1024 * 1024 * 1024)  # Convert size_limit to GB
            # shards_len = len(self.cache._shards)  # Number of shards
            # total_size = shards_limit * shards_len  # Total size limit in GB
            # current_size = self.cache.volume() / (1024 * 1024 * 1024)  # Current size in GB

            # print(f"  Shards limit (per shard): {shards_limit} GB")
            # print(f"  Number of shards: {shards_len}")
            # print(f"  Total size limit: {total_size} GB")
            # print(f"  Current size: {current_size} GB\n") 
            self.cache.set(key, result, expire=None, tag=self.source_ino + self.source_mtime, retry=True)
            logger.info(f"loader cache saved")
            # test = True
            # while test:
            #     # logger.info('Caching slice')
            #     if result == self.getSlice(*incomingSlices):
            #         test = False

        return result


    def locationGenerator(self, res):
        """
        Generate the file path for a specific resolution level.

        Args:
            res (int): The resolution level.

        Returns:
            str: The file path corresponding to the resolution level.
        """
        return os.path.join(self.active_path, self.dataset_paths[res])

    def open_array(self, res):
        """
        Open the Zarr array for the specified resolution level.

        Args:
            res (int): The resolution level.

        Returns:
            zarr.core.Array: The Zarr array for the resolution level.
        """
        store = self.zarr_store_type(self.locationGenerator(res))
        logger.info("OPENING ARRAYS")
        #store = self.wrap_store_in_chunk_cache(store)
        # if self.cache is not None:
        #     logger.info('OPENING CHUNK CACHE ARRAYS')
        #     from zarr_stores.zarr_disk_cache import Disk_Cache_Store
        #     store = Disk_Cache_Store(store, unique_id=store.path, diskcache_object=self.cache, persist=False)
        # # try:
        # #     if self.cache is not None:
        # #         store = disk_cache_store(store=store, uuid=self.locationGenerator(res), diskcache_object=self.cache, persist=None, meta_data_expire_min=15)
        # # except Exception as e:
        # #     logger.info('Caught Exception')
        # #     logger.info(e)
        # #     pass
        return zarr.open(store)

    def wrap_store_in_chunk_cache(self, store):
        """
        Wrap the Zarr store with a chunk cache for efficient access.

        Args:
            store (zarr.storage): The Zarr store to wrap.

        Returns:
            zarr.storage: The wrapped Zarr store with chunk caching.
        """
        if self.cache is not None:
            logger.info("OPENING CHUNK CACHE ARRAYS")
            logger.info(store.path)
            from zarr_chunk_cache import disk_cache_store as Disk_Cache_Store

            store = Disk_Cache_Store(
                store, uuid=store.path, diskcache_object=self.cache, persist=True
            )
        return store


# uicontrol bool channel0_visable checkbox(default=true);

# uicontrol invlerp channel0_lut (range=[1.6502681970596313,180.84588623046875],window=[0,180.84588623046875]);

# uicontrol vec3 channel0_color color(default="green");

# vec3 channel0 = vec3(0);


# void main() {

# if (channel0_visable == true)
# channel0 = channel0_color *   channel0_lut();

# vec3 rgb = (channel0);

# vec3 render = min(rgb,vec3(1));

# emitRGB(render);
# }
