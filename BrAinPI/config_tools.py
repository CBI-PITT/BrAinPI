
import os
import sys

from ome_zarr_loader import ome_zarr_loader

import imaris_ims_file_reader as ims
# Import zarr stores
from zarr.storage import NestedDirectoryStore
from zarr_stores.archived_nested_store import Archived_Nested_Store
from zarr_stores.h5_nested_store import H5_Nested_Store

def get_config(file='settings.ini',allow_no_value=True):
    import configparser
    # file = os.path.join(os.path.split(os.path.abspath(__file__))[0],file)
    file = os.path.join(sys.path[0], file)
    config = configparser.ConfigParser(allow_no_value=allow_no_value)
    config.read(file)
    return config


class config:
    '''
    This class will be used to manage open datasets and persistant cache
    '''

    def __init__(self):
        '''
        evictionPolicy Options:
            "least-recently-stored" #R only
            "least-recently-used"  #R/W (maybe a performace hit but probably best cache option)
        '''
        self.opendata = {}
        from cache_tools import get_cache
        print('INIT OF CACHE IN CONFIG CLASS')
        self.cache = get_cache()

        def __del__(self):
            if self.cache is not None:
                self.cache.close()

    def loadDataset(self, dataPath: str):

        '''
        Given the filesystem path to a file, open that file with the appropriate
        reader and store it in the opendata attribute with the dataPath as
        the key

        If the key exists return
        Always return the name of the dataPath
        '''

        print(dataPath)

        if dataPath in self.opendata:
            print(f'DATAPATH ENTRIES__{tuple(self.opendata.keys())}')
            return dataPath

        elif os.path.splitext(dataPath)[-1] == '.ims':

            print('Creating ims object')
            self.opendata[dataPath] = ims.ims(dataPath, squeeze_output=False)

            if self.opendata[dataPath].hf is None or self.opendata[dataPath].dataset is None:
                print('opening ims object')
                self.opendata[dataPath].open()


        elif dataPath.endswith('.ome.zarr'):
            self.opendata[dataPath] = ome_zarr_loader(dataPath, squeeze=False, zarr_store_type=NestedDirectoryStore, cache=self.cache)
            # self.opendata[dataPath].isomezarr = True

        elif '.omezans' in os.path.split(dataPath)[-1]:
            self.opendata[dataPath] = ome_zarr_loader(dataPath, squeeze=False, zarr_store_type=Archived_Nested_Store, cache=self.cache)

        elif '.omehans' in os.path.split(dataPath)[-1]:
            self.opendata[dataPath] = ome_zarr_loader(dataPath, squeeze=False, zarr_store_type=H5_Nested_Store, cache=self.cache)

        elif 's3://' in dataPath and dataPath.endswith('.zarr'):
            # import s3fs
            # self.opendata[dataPath] = ome_zarr_loader(dataPath, squeeze=False, zarr_store_type=s3fs.S3Map,
            #                                           cache=self.cache)
            from s3_utils import s3_boto_store
            self.opendata[dataPath] = ome_zarr_loader(dataPath, squeeze=False, zarr_store_type=s3_boto_store,
                                                      cache=self.cache)

        ## Append extracted metadata as attribute to open dataset
        try:
            from utils import metaDataExtraction # Here to get around curcular import at BrAinPI init
            self.opendata[dataPath].metadata = metaDataExtraction(self.opendata[dataPath])
        except Exception:
            pass

        return dataPath

