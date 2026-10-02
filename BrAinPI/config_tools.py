"""Application configuration and format-specific dataset loader dispatch.

The :class:`config` object owns the datasets opened by one application worker,
the persistent disk cache, and per-dataset reentrant locks. Local datasets are
selected by extension; public S3 Zarr datasets use a read-only Fsspec store.
"""

import os
import threading
import imaris_ims_file_reader as ims
# Import zarr stores
from zarr.storage import LocalStore
from zarr_stores.archived_nested_store import Archived_Nested_Store
from zarr_stores.h5_nested_store import H5_Nested_Store
import hashlib


_CONFIG_ENV = {
    "settings.ini": "BRAINPI_SETTINGS",
    "groups.ini": "BRAINPI_GROUPS",
}


def _resolve_config_path(file):
    """Resolve a configuration file, honoring container-friendly overrides."""
    if os.path.isabs(file):
        return file

    override_name = _CONFIG_ENV.get(file)
    override = os.environ.get(override_name) if override_name else None
    if override:
        return os.path.abspath(override)

    config_dir = os.environ.get("BRAINPI_CONFIG_DIR")
    if config_dir:
        return os.path.abspath(os.path.join(config_dir, file))

    dir_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(dir_path, file)

# def calculate_hash(input_string):
#     """
#     Calculate the SHA-256 hash of the input string
#     """
#     hash_result = hashlib.sha256(input_string.encode()).hexdigest()
#     return hash_result

def get_config(file='settings.ini',allow_no_value=True):
    """
    Load configuration settings from the requested INI file.

    Relative default filenames honor the corresponding ``BRAINPI_*`` path
    override and ``BRAINPI_CONFIG_DIR``. Missing files are rejected immediately;
    callers such as Sphinx must explicitly select a template configuration.

    Args:
        file (str, optional): The name of the INI file to load. Defaults to 'settings.ini'.
        allow_no_value (bool, optional): Whether to allow keys without values in the INI file. 
                                         Defaults to True.

    Returns:
        configparser.ConfigParser: A ConfigParser object containing the parsed configuration.
    """
    import configparser
    file_path = _resolve_config_path(file)
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Configured BrAinPI config file does not exist: {file_path}")
    config = configparser.ConfigParser(allow_no_value=allow_no_value)
    with open(file_path, encoding="utf-8") as config_file:
        config.read_file(config_file)

    # Keep secrets and public deployment URLs out of image layers and config
    # templates. Environment variables take precedence when supplied.
    environment_overrides = {
        "BRAINPI_SECRET_KEY": ("auth", "secret_key"),
        "BRAINPI_PUBLIC_URL": ("app", "url"),
        "BRAINPI_NG_PUBLIC_URL": ("neuroglancer", "url"),
        "BRAINPI_CACHE_DIR": ("disk_cache", "location_unix"),
    }
    for env_name, (section, option) in environment_overrides.items():
        value = os.environ.get(env_name)
        if value:
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, option, value)

    pyramid_root = os.environ.get("BRAINPI_PYRAMIDS_DIR")
    if pyramid_root:
        pyramid_root = os.path.abspath(pyramid_root)
        pyramid_locations = {
            "tif_loader": os.path.join(pyramid_root, "tif"),
            "nifti_loader": os.path.join(pyramid_root, "nifti"),
            "jp2_loader": os.path.join(pyramid_root, "jp2"),
        }
        for section, path in pyramid_locations.items():
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, "pyramids_images_store", path)
    return config


class config:
    """Manage open datasets, cache state, and initialization locks per worker.

    ``opendata`` maps stable dataset identities to loader instances. Calls to
    :meth:`loadDataset` for the same identity are serialized within a worker so
    concurrent requests cannot construct duplicate loaders.
    """

    def __init__(self):
        """
        evictionPolicy Options:
            "least-recently-stored" #R only
            "least-recently-used"  #R/W (maybe a performace hit but probably best cache option)
        Initialize the `config` object.

        This method loads settings and initializes a persistent cache. Generated
        pyramid paths are derived lazily from each source identity; startup does
        not scan the pyramid store.

        Args:
            opendata (dict): A dictionary to store open datasets, with keys as dataset identifiers and values as dataset objects.
            settings (configparser.ConfigParser): Loaded configuration settings from `settings.ini`.
            cache (diskcache.FanoutCache): A persistent cache object for managing dataset resources efficiently.
        """
        self.opendata = {}
        self.opendata_set = set()
        self._dataset_locks = {}
        self._dataset_locks_guard = threading.Lock()
        self.settings = get_config('settings.ini')
        from cache_tools import get_cache
        self.cache = get_cache()

    def __del__(self):
            if self.cache is not None:
                self.cache.close()

    def dataset_lock(self, key):
        """Return the reentrant lock associated with one dataset identity.

        Locks are local to the current process. Reentrancy is required because
        endpoint metadata initialization can occur while the loader call stack
        already owns the same dataset lock.

        Args:
            key: Stable dataset identity used in :attr:`opendata`.

        Returns:
            threading.RLock: Lock shared by loader and metadata initialization.
        """
        with self._dataset_locks_guard:
            return self._dataset_locks.setdefault(key, threading.RLock())

    def loadDataset(self, key: str, dataPath: str):
        """Load or reuse one dataset under its per-worker initialization lock.

        Args:
            key: Stable local inode/mtime identity or S3 URL.
            dataPath: Local filesystem path or supported ``s3://`` URL.

        Returns:
            str: ``key``, which indexes the loader in :attr:`opendata`.
        """
        with self.dataset_lock(key):
            return self._loadDataset(key, dataPath)

    def _loadDataset(self, key: str, dataPath: str):
        """
        Given the filesystem path to a file, open that file with the appropriate
        reader and store it in the opendata attribute with the hash of dataPath
        as the key

        If the key exists return
        Always return the hash of the dataPath

        Args:
            key (str): hash of dataPath
            dataPath (str): dataPath

        Returns:
            key (str): hash of dataPath
        """
        # print(dataPath , file_ino , modification_time)
        from logger_tools import logger
        if key in self.opendata:
            # logger.info(f'DATAPATH ENTRIES__{tuple(self.opendata.keys())}')
            logger.info(f'DATAPATH ENTRIES__{self.opendata_set}')
            return key
        if os.path.splitext(dataPath)[-1] == '.ims':

            logger.info('Creating ims object')
            self.opendata[key] = ims.ims(dataPath, squeeze_output=False)

            if self.opendata[key].hf is None or self.opendata[key].dataset is None:
                logger.info('opening ims object')
                self.opendata[key].open()
            self.opendata_set.add(dataPath)
                
        elif dataPath.endswith('.ome.zarr'):
            from ome_zarr_loader import ome_zarr_loader
            if dataPath.startswith('s3://'):
                from s3_utils import s3_fsspec_store
                zarr_store_type = s3_fsspec_store
            else:
                zarr_store_type = LocalStore
            self.opendata[key] = ome_zarr_loader(
                dataPath, 
                squeeze=False, 
                zarr_store_type=zarr_store_type,
                cache=self.cache
                )
            # self.opendata[dataPath].isomezarr = True
            self.opendata_set.add(dataPath)

        elif '.omezans' in os.path.split(dataPath)[-1]:
            from ome_zarr_loader import ome_zarr_loader
            self.opendata[key] = ome_zarr_loader(
                dataPath, 
                squeeze=False, 
                zarr_store_type=Archived_Nested_Store, 
                cache=self.cache
                )
            self.opendata_set.add(dataPath)
        elif '.omehans' in os.path.split(dataPath)[-1]:
            from ome_zarr_loader import ome_zarr_loader
            self.opendata[key] = ome_zarr_loader(
                dataPath, 
                squeeze=False, 
                zarr_store_type=H5_Nested_Store, 
                cache=self.cache
                )
            self.opendata_set.add(dataPath)
        elif dataPath.lower().endswith('tif') or dataPath.lower().endswith('tiff'):
            import tiff_loader
            self.opendata[key] = tiff_loader.tiff_loader(
                dataPath,
                pyramid_generation_allowed=True,
                pyramids_images_allowed_generation_size_gb=self.settings.get(
                    "tif_loader", "pyramids_images_allowed_generation_size_gb"
                ),
                pyramids_images_store=self.settings.get(
                    "tif_loader", "pyramids_images_store"
                ),
                extension_type=self.settings.get("tif_loader", "extension_type"),
                squeeze=False,
                cache=self.cache,
                )
            self.opendata_set.add(dataPath)
        elif dataPath.lower().endswith('.terafly'):
            import terafly_loader
            self.opendata[key] = terafly_loader.terafly_loader(
                dataPath, 
                squeeze=False,
                cache=self.cache
                )
            self.opendata_set.add(dataPath)
        elif dataPath.lower().endswith('.nii.zarr') or dataPath.lower().endswith('.nii.gz') or dataPath.lower().endswith('.nii'):
            import nifti_loader
            self.opendata[key] = nifti_loader.nifti_zarr_loader(
                dataPath,
                pyramid_generation_allowed=True,
                pyramids_images_allowed_generation_size_gb=self.settings.get(
                    "nifti_loader", "pyramids_images_allowed_generation_size_gb"
                ),
                pyramids_images_store=self.settings.get(
                    "nifti_loader", "pyramids_images_store"
                ),
                extension_type=self.settings.get("nifti_loader", "extension_type"),
                zarr_store_type=LocalStore,
                squeeze=False,
                cache=self.cache)
            self.opendata_set.add(dataPath)
        elif dataPath.lower().endswith('.jp2'):
            import jp2_loader
            self.opendata[key] = jp2_loader.jp2_loader(
                dataPath,
                pyramid_generation_allowed=True,
                pyramids_images_allowed_generation_size_gb=self.settings.get(
                    "jp2_loader", "pyramids_images_allowed_generation_size_gb"
                ),
                pyramids_images_store=self.settings.get(
                    "jp2_loader", "pyramids_images_store"
                ),
                extension_type=self.settings.get("jp2_loader", "extension_type"),
                squeeze=False,
                cache=self.cache
                )
            self.opendata_set.add(dataPath)
        elif dataPath.lower().endswith('.nd2'):
            import nd2_loader
            logger.info('Creating nd2 object')
            self.opendata[key] = nd2_loader.nd2_loader(dataPath, squeeze_output=False, cache=self.cache)
            self.opendata_set.add(dataPath)
        elif dataPath.lower().endswith('.pcd'):
            from ng_precomputed_loader import ng_precomputed_loader
            logger.info('Creating neuroglancer precomputed object')
            self.opendata[key] = ng_precomputed_loader(
                dataPath,
                squeeze=False,
                cache=self.cache,
            )
            self.opendata_set.add(dataPath)
        elif dataPath.endswith('.zarr'):
            # import s3fs
            # self.opendata[dataPath] = ome_zarr_loader(dataPath, squeeze=False, zarr_store_type=s3fs.S3Map,
            #                                           cache=self.cache)
            if dataPath.startswith('s3://'):
                from s3_utils import s3_fsspec_store
                self.opendata[key] = ome_zarr_loader(
                    dataPath, 
                    squeeze=False, 
                    zarr_store_type=s3_fsspec_store,
                    cache=self.cache
                    )
                self.opendata_set.add(dataPath)
            else:
                raise ValueError('Only s3 zarr loading is currently supported')
        ## Append extracted metadata as attribute to open dataset
        try:
            from utils import metaDataExtraction # Here to get around curcular import at BrAinPI init
            self.opendata[key].metadata = metaDataExtraction(self.opendata[key])
            logger.info(self.opendata[key].metadata)
        except Exception:
            pass

        return key
