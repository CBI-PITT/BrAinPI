# # -*- coding: utf-8 -*-
# """
# Created on Tue Nov  2 14:12:11 2021

# @author: alpha
# """
# import itertools
# import io
# import xml.etree.ElementTree as ET

# from collections.abc import MutableMapping
# from zarr._storage.store import Store, BaseStore
# from typing import Union
# Path = Union[str, bytes, None]
# StoreLike = Union[BaseStore, Store, MutableMapping]

# ## pip install xmltodict
# inFile = '/CBI_Hive/CBI/Mike/Slide Scanner/Alan/Eye fluo scan/_Image_/stack1/frame_t_0.tif'
# # inFile = '/CBI_Hive/CBI/Mike/Slide Scanner/Alan/_Image_/stack10002/frame_t_0.tif'

# from tifffile import imread, imwrite, TiffFile, TiffWriter
# import zarr
# from ast import literal_eval

# ome_meta_keys = {

#     'shape':('SizeT','SizeC','SizeZ','SizeY','SizeX'),
#     'resolution':('PhysicalSizeZ', 'PhysicalSizeY', 'PhysicalSizeX'),
#     'resolution_unit':('PhysicalSizeZUnit','PhysicalSizeYUnit','PhysicalSizeXUnit'),
#     'dtype':'Type',
#     'order':'DimensionOrder',
#     'bitdepth':'SignificantBits',
#     'TimePoints':'SizeT',
#     'Channels':'SizeC'

# }

# to_um_conversion_fact = {
#     'km': 1e9,
#     'm': 1000000,
#     'dm': 100000,
#     'cm': 10000,
#     'mm': 1000,
#     'um': 1,
#     'nm': 0.001,
#     'pm': 1e-6
# }


# def get_ome_tiff_metadata(file_name):
#     with TiffFile(inFile) as tif_read:
#         seriesNum = len(tif_read.series)
#         ome_meta = tif_read.ome_metadata
#         f = io.StringIO(ome_meta)
#         tree = ET.parse(f)
#         root = tree.getroot()
#         tags = [elem.tag for elem in root.iter()]
#         img_tags = [x  for x in tags if 'Image' in x]
#         img_tags_dict_info = [x.attrib for x in root.iter(img_tags[0])]
#         print(f'{img_tags_dict_info=}')
#         px_tags = [x  for x in tags if 'Pixels' in x]
#         img_dict_info = [x.attrib for x in root.iter(px_tags[0])]
#         metadata = {}
#         for s in range(seriesNum):
#             current_info = img_dict_info[s]
#             TimePoints = literal_eval(current_info.get(ome_meta_keys['TimePoints']))
#             Channels = literal_eval(current_info.get(ome_meta_keys['Channels']))
#             with tif_read.series[s].aszarr() as img_store:
#                 img = zarr.open(img_store)
#                 print(f'{img}')
#                 tileshape = img.chunks
#                 while len(tileshape) < len(img.shape):
#                     tileshape = (1,) + tileshape
#                 print(f'{tileshape=}')
#             for t in range(TimePoints):
#                 for c in range(Channels):
#                     root_key = (s, t, c)
#                     for key, value in ome_meta_keys.items():
#                         if isinstance(value,str):
#                             tmp = current_info.get(ome_meta_keys[key])
#                             try:
#                                 tmp = literal_eval(tmp)
#                             except:
#                                 pass
#                             metadata[root_key + (key,)] = tmp
#                         if isinstance(value,tuple):
#                             out = tuple()
#                             for k in value:
#                                 tmp = current_info.get(k)
#                                 if tmp == 'µm':
#                                     tmp = 'um'
#                                 try:
#                                     tmp = literal_eval(tmp)
#                                 except:
#                                     pass
#                                 out += (tmp,)
#                             metadata[root_key + (key,)] = out
#                     metadata[root_key + ('chunks',)] = tileshape
#         metadata['TimePoints'] = TimePoints
#         metadata['Channels'] = Channels
#         metadata['ResolutionLevels'] = seriesNum
#         metadata = convert_metadata_calib_to_um(metadata)
#         return ome_meta, metadata

# def convert_metadata_calib_to_um(metadata):
#     rr, tt, cc = metadata.get('ResolutionLevels'), metadata.get('TimePoints'), metadata.get('Channels')
#     for r in range(rr):
#         for t in range(tt):
#             for c in range(cc):
#                 res_key = (r,t,c,'resolution')
#                 unit_key = (r, t, c, 'resolution_unit')
#                 if unit_key in metadata and res_key in metadata:
#                     res = metadata.get(res_key)
#                     unit = metadata.get(unit_key)
#                     new_res = tuple([r * to_um_conversion_fact.get(u) for r,u in zip(res,unit)])
#                     metadata[res_key] = new_res
#                     metadata[unit_key] = ('um',) * len(unit)
#     return metadata





# class multiscale_tiff_loader:
#     def __init__(self, location, ResolutionLevelLock=None, verbose=None, squeeze=True, cache=None):
#         # assert StoreLike is s3fs.S3Map or any([issubclass(zarr_store_type,x) for x in StoreLike.__args__]), 'zarr_store_type is not a zarr storage class'
#         self.location = location
#         self.s3 = False
#         # if 's3://' in location:
#         #     self.s3 = s3fs.S3FileSystem(anon=True)
#         self.ResolutionLevelLock = 0 if ResolutionLevelLock is None else ResolutionLevelLock
#         self.verbose = verbose
#         self.squeeze = squeeze
#         self.cache = cache
#         self.ome_metaData, self.metaData = get_ome_tiff_metadata(self.location)
#         self.ResolutionLevels = self.metaData['ResolutionLevels']
#         self.TimePoints = self.metaData['TimePoints']
#         self.Channels = self.metaData['Channels']
#         self.change_resolution_lock(self.ResolutionLevelLock)
#         self.arrays = {}
#         for res in range(self.ResolutionLevels):
#             self.arrays[res] = self.open_array(res)
#     def change_resolution_lock(self,ResolutionLevelLock):
#         self.ResolutionLevelLock = ResolutionLevelLock
#         self.shape = self.metaData[self.ResolutionLevelLock,0,0,'shape']
#         self.ndim = len(self.shape)
#         self.chunks = self.metaData[self.ResolutionLevelLock,0,0,'chunks']
#         self.resolution = self.metaData[self.ResolutionLevelLock,0,0,'resolution']
#         self.dtype = np.dtype(self.metaData[self.ResolutionLevelLock,0,0,'dtype'])
#     def open_array(self,res):
#         tif_read = TiffFile(self.location)
#         img_store = tif_read.series[res].aszarr()
#         img = zarr.open(img_store)
#         return img
    
#     def __getitem__(self,key):
        
#         res = 0 if self.ResolutionLevelLock is None else self.ResolutionLevelLock
#         print(key)
#         if isinstance(key,slice) == False and isinstance(key,int) == False and len(key) == 6:
#             res = key[0]
#             if res >= self.ResolutionLevels:
#                 raise ValueError('Layer is larger than the number of ResolutionLevels')
#             key = tuple([x for x in key[1::]])
#         print(res)
#         print(key)
        
#         if isinstance(key, int):
#             key = [slice(key,key+1)]
#             for _ in range(self.ndim-1):
#                 key.append(slice(None))
#             key = tuple(key)
            
#         if isinstance(key,tuple):
#             key = [slice(x,x+1) if isinstance(x,int) else x for x in key]
#             while len(key) < self.ndim:
#                 key.append(slice(None))
#             key = tuple(key)
        
#         print(key)
#         newKey = []
#         for ss in key:
#             if ss.start is None and isinstance(ss.stop,int):
#                 newKey.append(slice(ss.stop,ss.stop+1,ss.step))
#             else:
#                 newKey.append(ss)
                
#         key = tuple(newKey)
#         print(key)
        
        
#         array = self.getSlice(
#                         r=res,
#                         t = key[0],
#                         c = key[1],
#                         z = key[2],
#                         y = key[3],
#                         x = key[4]
#                         )
        
#         if self.squeeze:
#             return np.squeeze(array)
#         else:
#             return array
        
    
#     def _get_memorize_cache(self, name=None, typed=False, expire=None, tag=None, ignore=()):
#         if tag is None: tag = self.location
#         return self.cache.memorize(
#             name=name,
#             typed=typed,
#             expire=expire,
#             tag=tag,
#             ignore=ignore
#             ) if self.cache is not None else lambda x: x
    
#     def getSlice(self,r,t,c,z,y,x):
        
#         '''
#         Access the requested slice based on resolution level and 
#         5-dimentional (t,c,z,y,x) access to zarr array.
#         '''
        
#         incomingSlices = (r,t,c,z,y,x)
#         print(incomingSlices)
#         if self.cache is not None:
#             key = f'{self.location}_getSlice_{str(incomingSlices)}'
#             # key = self.location + '_getSlice_' + str(incomingSlices)
#             result = self.cache.get(key, default=None, retry=True)
#             if result is not None:
#                 print(f'Returned from cache: {incomingSlices}')
#                 return result
        
#         result = self.arrays[r][t,c,z,y,x]

#         if self.cache is not None:
#             self.cache.set(key, result, expire=None, tag=self.location, retry=True)
#             # test = True
#             # while test:
#             #     # print('Caching slice')
#             #     self.cache.set(key, result, expire=None, tag=self.location, retry=True)
#             #     if result == self.getSlice(*incomingSlices):
#             #         test = False

        
#         return result
#         # return self.open_array(r)[t,c,z,y,x]
    
    
#     def locationGenerator(self,res):
#         return os.path.join(self.location,self.dataset_paths[res])
    


# ########################
# ## TESTING
# ########################
    
# inFile = '/CBI_Hive/CBI/Mike/Slide Scanner/Alan/Eye fluo scan/_Image_/stack1/frame_t_0.tif'
# # inFile = '/CBI_Hive/CBI/Mike/Slide Scanner/Alan/_Image_/stack10002/frame_t_0.tif'
# inFile = '/CBI_Hive/CBI/Mike/Slide Scanner/Alan/big ol thing/_Image_01_01_/stack1/frame_t_0.tif'  # Axes IYXS
# inFile = '/CBI_Hive/CBI/Mike/Slide Scanner/Eickelberg/1ss53 more brightfield/_Image_02_03_/stack1/frame_t.tif' #Brightfield

# def collect_basic_image_info(inFile):
#     with TiffFile(inFile) as tif:
#         tiffData = {}
#         tiffData['ome_metadata'] = tif.ome_metadata  # This is XML format and can be directly written to disk
#         tiffData['ResolutionLevels'] = len(tif.series)
#         tiffData['shape'] = tif.series[0].shape
#         tiffData['dtype'] = tif.series[0].dtype
#         tiffData['axes'] = tif.series[0].axes
#         tiffData['ndim'] = tif.series[0].ndim
#     return tiffData

# image_info = collect_basic_image_info(inFile)

# axes = image_info['axes'].upper()
# if axes == 'RGB':
#     axes = 'YXS'
# XY_index = axes.index('YX')

# with imread(inFile, aszarr=True) as store:
#     tif = zarr.open(store, mode='r')
#     try:
#         tileshape = tif.chunks
#     except Exception:
#         tileshape = fallback_tileshape
