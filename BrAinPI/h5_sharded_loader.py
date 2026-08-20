# -*- coding: utf-8 -*-
"""Legacy loader for image pyramids stored as HDF5 virtual datasets."""
import os
import glob
from natsort.natsort import natsorted
import itertools
import dask
from dask.delayed import delayed
import dask.array as da
import h5py
# import hdf5plugin

if os.name == 'nt':
    location = r"z:/testData/bil/h5_shard"
else:
    location = "/CBI_FastStore/testData/bil/h5_shard"


# os.environ["HDF5_VDS_PREFIX"] = location
class h5_sharded:
    """Read a multiresolution image assembled from HDF5 virtual datasets.

    Each resolution is stored in a ``VDS_<level>.hf`` file containing a
    ``vdata`` dataset. The loader exposes the selected level's array metadata
    and delegates indexing to h5py.
    """
    
    def __init__(self, location, ResolutionLevelLock=None, squeeze=True, compute=True):
        
        self.location = location
        os.environ["HDF5_VDS_PREFIX"] = self.location
        self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock
        self.compute = compute
        self.squeeze = squeeze
        self.metaData = {}
        
        self.VDS_prefix = 'VDS'
        self.VDS_files = natsorted(glob.glob(os.path.join(self.location,'VDS*[0-9]*')))
        self.ResolutionLevels = len(self.VDS_files)
        
        # self.dataset = {}
        # for res in range(self.ResolutionLevels):
        #     print('Assembling Resolution Level {}'.format(res))
        #     self.dataset[res] = delayed(self.build_array_par)(location,res) # Works full parallel assembly
        # self.dataset = dask.compute(self.dataset)[0]
            
        
        shape = self.get_attr_from_vds(0,'shape')
        self.TimePoints = shape[0]
        self.Channels = shape[1]
        
        self.collect_metadata()
        self.change_resolution_lock(self.ResolutionLevelLock)
        
    def collect_metadata(self):
        """Populate per-resolution, time, and channel metadata from VDS files."""
        for res, t, c in itertools.product(range(self.ResolutionLevels), range(self.TimePoints),
                                         range(self.Channels)):
            
            self.metaData[(res,t,c,'chunks')] = self.get_attr_from_vds(res,'chunks')
            self.metaData[(res,t,c,'dtype')] = self.get_attr_from_vds(res,'dtype')
            self.metaData[(res,t,c,'shape')] = self.get_attr_from_vds(res,'shape')
            self.metaData[(res,t,c,'ndim')] = self.get_attr_from_vds(res,'ndim')
            self.metaData[(res,t,c,'size')] = self.get_attr_from_vds(res,'size')
            ## Need to extract resolution by some other means.  For now, default to 1,1,1 and divide by 2 for each series
            self.metaData[res,t,c,'resolution'] = tuple([x*(2**res) for x in (50,1,1)])#(1,1,1)(z,y,x)
            
    def change_resolution_lock(self,ResolutionLevelLock):
        """Select a default VDS resolution and refresh array-like attributes."""
        self.ResolutionLevelLock = ResolutionLevelLock
        self.shape = self.metaData[(self.ResolutionLevelLock,0,0,'shape')]
        self.size = self.metaData[(self.ResolutionLevelLock,0,0,'size')]
        self.ndim = self.metaData[(self.ResolutionLevelLock,0,0,'ndim')]
        self.chunks = self.metaData[(self.ResolutionLevelLock,0,0,'chunks')]
        self.dtype = self.metaData[(self.ResolutionLevelLock,0,0,'dtype')]
        
    def __getitem__(self,key):
        # print(key)
        res = self.ResolutionLevelLock
        
        if isinstance(key,tuple) and len(key) == 6:
            res = key[0]
            key = key[1:]
        
        return self.get_from_vds(res,key)
    
    # with h5py.File(os.path.join(self.out_location,'VDS_{}.hf'.format(res)), 'w', libver='latest') as f:
    #     print('Creating VDS')
        
    def VDS_file_namer(self,res):
        """Return the conventional virtual-dataset filename for a resolution."""
        name = os.path.join(self.location,'VDS_{}.hf'.format(res))
        print(name)
        return name
                            
    def get_from_vds(self,res,key):
        """Read ``key`` from the requested VDS resolution."""
        with h5py.File(self.VDS_file_namer(res), 'r') as f:
            print('opened dataset')
            return f['vdata'][key]
    
    def get_attr_from_vds(self,res,key):
        """Return a supported h5py dataset attribute for one resolution.

        Supported keys are ``shape``, ``dtype``, ``chunks``, ``ndim``,
        ``nbytes``, and ``size``.
        """
        with h5py.File(self.VDS_file_namer(res), 'r') as f:
            if key == 'shape':
                return f['vdata'].shape
            if key == 'dtype':
                return f['vdata'].dtype
            if key == 'chunks':
                return f['vdata'].chunks
            if key == 'ndim':
                return f['vdata'].ndim
            if key == 'nbytes':
                return f['vdata'].nbytes
            if key == 'size':
                return f['vdata'].size
            
