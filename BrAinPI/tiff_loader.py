# import tifffile
# import numpy as np
# from skimage import io, img_as_uint, img_as_ubyte, img_as_float32, img_as_float64
# import math
# from itertools import product
# from PIL import Image
# import io

# class tiff_loader:
#     def __init__(self, file_location) -> None:
#         self.location = file_location
#         self.image = tifffile.TiffFile(self.location)
#         self.tags = self.image.pages[0].tags

#         # Extract width and height
#         self.width = self.tags["ImageWidth"].value
#         self.height = self.tags["ImageLength"].value

#         self.dataType = self.image.pages[0].dtype
#         print(self.dataType)
#         self.resolutions = len(self.image.series)
#         self.channels = len(self.image.series[0])
#         if self.image.pages[0].is_tiled:
#             # Get the tile size
#             self.tile_size = (
#                 self.image.pages[0].tilewidth,
#                 self.image.pages[0].tilelength,
#             )
#         else:
#             self.tile_size = (512, 512)
#         # if self.image.pages[0].is_tiled:
#         #     # Get the tile size
#         #     self.tile_size = (
#         #         128,
#         #         128
#         #     )
#         # else:
#         #     self.tile_size = (128, 128)
#         # Extract image format

#         print("Width:", self.width)
#         print("Height:", self.height)
#         print("Tile Size:", self.tile_size)
#         self.array = {}

#         self.type = self.image.series[0].axes
#         dic = {"T": 0, "C": 1, "Z": 2, "Q": 2, "Y": 3, "X": 4, "S": 5}
#         if self.type.endswith("S"):
#             num_axes = 6
#         else:
#             num_axes = 5
#         print("resolutions", self.resolutions)
#         for r in range(self.resolutions):
#             temp = [1] * num_axes
#             nparray = self.image.series[r].asarray()
#             shape = nparray.shape
#             print("Original shape:", shape)
#             print(self.image.series[r].dims)
#             print(self.image.series[r].ndim)
#             print(self.image.series[r].get_shape)
#             print(self.image.series[r].shape)
#             type_str = self.image.series[r].axes
#             characters = list(type_str)
#             print("Axis characters:", characters)
#             for index, char in enumerate(characters):
#                 if char in dic:
#                     temp[dic[char]] = shape[index]
#             temp_tuple = tuple(temp)
#             nparray = nparray.reshape(temp_tuple)
#             print("Reshaped shape:", nparray.shape)
#             if (
#                 r != 0 and
#                 (nparray.shape[3] != self.array[r-1].shape[3] // 2
#                 or nparray.shape[4] != self.array[r-1].shape[4] // 2)
#             ):
#                 self.pyramid_inspector(r=r-1, num_axes=num_axes,width=self.array[r-1].shape[4] // 2,height=self.array[r-1].shape[3])
#             self.array[r] = nparray
#             # print('r = ',r)

#             if r == self.resolutions - 1:
#                 self.pyramid_inspector(r=r, num_axes=num_axes,
#                                        width = self.image.series[r].pages[0].tags["ImageWidth"].value,
#         height = self.image.series[r].pages[0].tags["ImageLength"].value
#                                        )

#         for i in self.array:
#             print("level: ", i)
#             print("shape: ", self.array[i].shape)
#         # numbers of channels and z using t = 0
#         self.channels = self.array[0][0].shape[0]
#         self.z = self.array[0][0].shape[1]

#     def pyramid_inspector(self, r, num_axes,width,height):
#         # width = self.image.series[r].pages[0].tags["ImageWidth"].value
#         # height = self.image.series[r].pages[0].tags["ImageLength"].value
#         if num_axes == 5:
#             cur = r
#             while self.pyramid_validator(width=width, height=height) != True:
#                 print("Grey-scale image pyramids bulding...")
#                 cur_shape = self.array[cur].shape
#                 next = cur + 1
#                 self.array[next] = np.empty(
#                     (
#                         cur_shape[0],
#                         cur_shape[1],
#                         cur_shape[2],
#                         cur_shape[3] // 2,
#                         cur_shape[4] // 2,
#                     ),
#                     dtype=self.dataType,
#                 )
#                 for t in range(cur_shape[0]):
#                     for c in range(cur_shape[1]):
#                         for z in range(cur_shape[2]):
#                             result = self.local_mean_downsample_grey(
#                                 image=self.array[cur][t, c, z],
#                                 dataType=self.array[cur][t, c, z].dtype,
#                             )
#                             self.array[next][t, c, z] = result
#                 cur = next
#                 height = result.shape[0]
#                 width = result.shape[1]
#         elif num_axes == 6:
#             cur = r
#             while self.pyramid_validator(width=width, height=height) != True:
#                 print("RGB image pyramids bulding...")
#                 cur_shape = self.array[cur].shape
#                 next = cur + 1
#                 self.array[next] = np.empty(
#                     (
#                         cur_shape[0],
#                         cur_shape[1],
#                         cur_shape[2],
#                         cur_shape[3] // 2,
#                         cur_shape[4] // 2,
#                         cur_shape[5],
#                     ),
#                     dtype=self.dataType,
#                 )
#                 for t in range(cur_shape[0]):
#                     for c in range(cur_shape[1]):
#                         for z in range(cur_shape[2]):
#                             print("========", self.array[cur][t, c, z].shape)
#                             print("dataType: ", self.array[cur][t, c, z].dtype)
#                             result = self.local_mean_downsample_rgb(
#                                 image=self.array[cur][t, c, z],
#                                 dataType=self.array[cur][t, c, z].dtype,
#                             )
#                             print("--------", result.shape)
#                             self.array[next][t, c, z] = result
#                 cur = next
#                 height = result.shape[0]
#                 width = result.shape[1]

#     def pyramid_validator(self, width, height):
#         if width > self.tile_size[0] or height > self.tile_size[1]:
#             return False
#         else:
#             return True

#     def local_mean_downsample_rgb(self, image, dataType, down_sample_ratio=(2, 2, 1)):
#         print("first int", dataType)
#         image = img_as_float32(image)
#         canvas = np.zeros(
#             (
#                 image.shape[0] // down_sample_ratio[0],
#                 image.shape[1] // down_sample_ratio[1],
#                 image.shape[2] // down_sample_ratio[2],
#             ),
#             dtype=np.dtype("float32"),
#         )

#         # print(canvas.shape)
#         for z, y, x in product(
#             range(down_sample_ratio[0]),
#             range(down_sample_ratio[1]),
#             range(down_sample_ratio[2]),
#         ):
#             tmp = image[
#                 z :: down_sample_ratio[0],
#                 y :: down_sample_ratio[1],
#                 x :: down_sample_ratio[2],
#             ][0 : canvas.shape[0], 0 : canvas.shape[1], 0 : canvas.shape[2]]
#             canvas[0 : tmp.shape[0], 0 : tmp.shape[1], 0 : tmp.shape[2]] += tmp

#         canvas /= math.prod(down_sample_ratio)
#         if dataType == np.dtype("uint16"):
#             return img_as_uint(canvas)

#         if dataType == np.dtype("ubyte"):
#             return img_as_ubyte(canvas)

#         if dataType == np.dtype("float32"):
#             return img_as_float32(canvas)

#         if dataType == np.dtype(float):
#             print("float64 checked")
#             return img_as_float64(canvas)

#     def local_mean_downsample_grey(self, image, dataType, down_sample_ratio=(2, 2)):

#         image = img_as_float32(image)
#         canvas = np.zeros(
#             (
#                 image.shape[0] // down_sample_ratio[0],
#                 image.shape[1] // down_sample_ratio[1],
#             ),
#             # dtype=np.dtype('float32')
#         )

#         # print(canvas.shape)
#         for y, x in product(range(down_sample_ratio[0]), range(down_sample_ratio[1])):
#             tmp = image[y :: down_sample_ratio[0], x :: down_sample_ratio[1]][
#                 0 : canvas.shape[0], 0 : canvas.shape[1]
#             ]
#             canvas[0 : tmp.shape[0], 0 : tmp.shape[1]] += tmp

#         canvas /= math.prod(down_sample_ratio)

#         if dataType == np.dtype("uint16"):
#             print("uint16")
#             return img_as_uint(canvas)

#         if dataType == np.dtype("ubyte"):
#             print("ubyte")
#             return img_as_ubyte(canvas)

#         if dataType == np.dtype("float32"):
#             print("float32")
#             return img_as_float32(canvas)

#         if dataType == np.dtype(float):
#             print("float")
#             return img_as_float64(canvas)


import tifffile
import numpy as np
from skimage import io, img_as_uint, img_as_ubyte, img_as_float32, img_as_float64
import math
from itertools import product
from PIL import Image
import io
import zarr
import os
import config_tools
import time
import hashlib

def calculate_hash(input_string):
    # Calculate the SHA-256 hash of the input string
    hash_result = hashlib.sha256(input_string.encode()).hexdigest()
    return hash_result

class tiff_loader:
    def __init__(self, file_location, pyramid_images_connection) -> None:
        # print('pyramid_images_connection',pyramid_images_connection)
        self.settings = config_tools.get_config('settings.ini')
        self.location = file_location
        # print(self.location)
        self.image = tifffile.TiffFile(self.location)
        self.filename, self.filename_extension = self.file_extension_split()
        self.tags = self.image.pages[0].tags
        self.photometric = self.image.pages[0].photometric
        self.compression = self.image.pages[0].compression
        # Extract width and height
        self.height = self.tags["ImageLength"].value
        self.width = self.tags["ImageWidth"].value

        self.dataType = self.image.pages[0].dtype
        print(self.dataType)
        self.series = len(self.image.series)
        print("series", self.series)
        print("levels", len(self.image.series[0].levels))
        if self.image.pages[0].is_tiled:
            # Get the tile size
            self.tile_size = (
                self.image.pages[0].tilelength,
                self.image.pages[0].tilewidth,
            )
        else:
            print("Assigning tile size (512, 512)")
            self.tile_size = (512, 512)

        print("Height:", self.height)
        print("Width:", self.width)
        print("Tile Size:", self.tile_size)

        # self.array = {}
        self.arrays = {}

        self.type = self.image.series[0].axes
        self.axes_pos_dic = self.axes_char_extract(self.type)
        self.axes_value_dic = self.axes_value_extract(
            self.type, self.image.series[0].shape
        )
        self.channels = (
            self.axes_value_dic.get("C") if self.axes_value_dic.get("C") else 0
        )
        self.z = self.axes_value_dic.get("Z") if self.axes_value_dic.get("Z") else self.axes_value_dic.get("Q") if self.axes_value_dic.get("Q") else 0
        self.t = self.axes_value_dic.get("T") if self.axes_value_dic.get("T") else 0
        
        self.pyramid_dic = pyramid_images_connection
        print(self.type)
        print('axes_pos_dic',self.axes_pos_dic)
        print('axes_value_dic',self.axes_value_dic)

        # if already pyramid image --> building the arrays
        # elif no pyramid but connection exist --> replace the location, building the arrays
        # elif no prramid and no connection --> pyramid image generation, building connection using hash func and replace location, building arrays 
        self.pyramid_validators(self.image)
        if self.location.endswith('.ome.tif'):
            store = tifffile.imread(self.location, aszarr=True, series=0)
            zarr_store = zarr.open(store)
            print("zarr.group")
            for r in range(len(zarr_store)):
                self.arrays[r] = zarr_store[r]
                # print(self.arrays[r].shape)
               
        elif self.location.endswith('.tif'):
            print("zarr.core.Array")
            for r in range(len(self.image.series)):
                store = tifffile.imread(self.location, aszarr=True, series=r)
                print(type(store))
                zarr_store = zarr.open(store)
                if isinstance(zarr_store, zarr.core.Array):
                    self.arrays[r] = zarr.open(store)
                    # print(self.arrays[r].shape)           
        print(self.arrays)
        print("Arrays building complete")

        # type_str = self.image.series[0].axes
        # characters = list(type_str)
        # shape = self.image.series[0].shape
        # for index, value in enumerate(characters):
        #     if char in dic:
        #         dic[char] = shape(index)

        # if self.type.endswith("S"):
        #     num_axes = 6
        # else:
        #     num_axes = 5

        # for r in range(self.resolutions):
        #     temp = [1] * num_axes
        #     nparray = self.image.series[r].asarray()
        #     shape = nparray.shape
        #     print("Original shape:", shape)
        #     print(self.image.series[r].dims)
        #     print(self.image.series[r].ndim)
        #     print(self.image.series[r].shape)
        #     # print(self.image.series[r][0][0][0])
        #     type_str = self.image.series[r].axes
        #     characters = list(type_str)
        #     print("Axis characters:", characters)
        #     for index, char in enumerate(characters):
        #         if char in dic:
        #             temp[dic[char]] = shape[index]
        #     temp_tuple = tuple(temp)
        #     nparray = nparray.reshape(temp_tuple)
        #     print("Reshaped shape:", nparray.shape)
        #     if (
        #         r != 0 and
        #         (nparray.shape[3] != self.array[r-1].shape[3] // 2
        #         or nparray.shape[4] != self.array[r-1].shape[4] // 2)
        #     ):
        #         self.pyramid_inspector(r=r-1, num_axes=num_axes,width=self.array[r-1].shape[4] // 2,height=self.array[r-1].shape[3])
        #     self.array[r] = nparray
        #     # print('r = ',r)

        #     if r == self.resolutions - 1:
        #         self.pyramid_inspector(r=r, num_axes=num_axes,
        #                                width = self.image.series[r].pages[0].tags["ImageWidth"].value,
        # height = self.image.series[r].pages[0].tags["ImageLength"].value
        #                                )

        # for i in self.array:
        #     print("level: ", i)
        #     print("shape: ", self.array[i].shape)
        # # numbers of channels and z using t = 0

    def __getitem__(self, key):
        "r t c z y x (s)"
        r = int(key[0])
        t = int(key[1]) if self.axes_value_dic.get("T") else None
        c = int(key[2]) if self.axes_value_dic.get("C") else None
        z = int(key[3]) if self.axes_value_dic.get("Z") or self.axes_value_dic.get("Q") else None
        y = int(key[4])
        x = int(key[5])
        tile_size = int(self.tile_size[0])
        tp = tuple(filter(lambda x: x is not None, (t, c, z)))
        # print("tp",tp)
        
        return self.arrays[r][*tp, y * tile_size:(y + 1) * tile_size, x * tile_size:(x + 1) * tile_size]
        numpy_array = np.random.randint(0, 255, size=(256, 256, 3), dtype=np.uint8)
        return numpy_array

    def pyramid_validators(self, tif):
        inspector_result = self.pyramid_inspectors(tif)
        print(f"inspector_result: {inspector_result}")
        if inspector_result:
            return 
        else:
            self.pyramid_builders(tif)
            return 

    def pyramid_inspectors(self, tif):
        series = tif.series[0]
        if tif.filename.endswith("ome.tif"):
            if series.is_pyramidal:
                # may need more check opeation
                return True
            else:
                return False
        elif tif.filename.endswith(".tif"):
            for r in range(len(tif.series)):
                if r != 0 and (
                    tif.series[r].shape[self.axes_pos_dic.get("Y")]
                    != tif.series[r - 1].shape[self.axes_pos_dic.get("Y")] // 2
                    or tif.series[r].shape[self.axes_pos_dic.get("Y")]
                    != tif.series[r - 1].shape[self.axes_pos_dic.get("Y")] // 2
                ):
                    return False
                if r == len(tif.series) - 1:
                    if (
                        tif.series[r].pages[0].imagelength > self.tile_size[0]
                        or tif.series[r].pages[0].imagewidth > self.tile_size[1]
                    ):
                        return False
                    else:
                        return True
            return True
    def file_extension_split(self):
        file = None
        extension = None
        if self.location.endswith('.ome.tif'):
            extension_index = self.location.rfind('.ome.tif')
            file = self.location[:extension_index]
            extension = '.ome.tif'
        elif self.location.endswith('.tif'):
            extension_index = self.location.rfind('.tif')
            file = self.location[:extension_index]
            extension = '.tif'
        return [file,extension]
    def pyramid_builders(self, tif):
        hash_value = calculate_hash(self.location)
        if self.pyramid_dic.get(hash_value):
            self.location = self.pyramid_dic.get(hash_value)
            print('Location replaced by generated pyramid image')
            # return tifffile.TiffFile(self.pyramid_dic.get(self.location))
        else:
            if tif.filename.endswith("ome.tif"):
                # write ome.tif pyramids
                self.pyramid_building_process(tif.series[0].level[0],2,hash_value)
                print('pyramid image building complete!')
            elif tif.filename.endswith("tif"):
                print('pyramid image is building...')
                start_time = time.time()
                # write tif pyramids
                self.pyramid_building_process(tif.series[0],2,hash_value)
                end_time = time.time()
                execution_time = end_time - start_time
                print(f'pyramid image building complete, execution time: {execution_time}')
                # pyramid_images = tifffile.imread(self.pyramid_dic.get(self.filename),aszarr=True)
                # return tif
    def pyramid_building_process(self, first_series,factor,hash_value):
        subresolutions = self.divide_time(first_series.shape,factor,self.tile_size)
        data = first_series.asarray()
        pixelsize = 0.29  # micrometer
        # prefix = 'py_'
        suffix = '.ome.tif'
        pyramids_images_store = self.settings.get('tif_loader', 'pyramids_images_store')
        pyramid_image_location = pyramids_images_store  + calculate_hash(self.location) + suffix
        with tifffile.TiffWriter(pyramid_image_location, bigtiff=True) as tif:
            metadata={
                'axes': self.type,
                'SignificantBits': 10,
                'TimeIncrement': 0.1,
                'TimeIncrementUnit': 's',
                'PhysicalSizeX': pixelsize,
                'PhysicalSizeXUnit': 'Âµm',
                'PhysicalSizeY': pixelsize,
                'PhysicalSizeYUnit': 'Âµm',
        #          'Channel': {'Name': ['Channel 1', 'Channel 2']},
        #          'Plane': {'PositionX': [0.0] * 16, 'PositionXUnit': ['Âµm'] * 16}
            }
            options = dict(
                photometric=self.photometric,
                tile=self.tile_size,
                compression=self.compression,
                resolutionunit='CENTIMETER'
            )
            tif.write(
                data,
                subifds=subresolutions,
                resolution=(1e4 / pixelsize, 1e4 / pixelsize),
                metadata=metadata,
                **options
            )
            # write pyramid levels to the two subifds
            # in production use resampling to generate sub-resolution images
            for level in range(subresolutions):
                mag = factor**(level + 1)
                if(self.type.endswith('S')):
                    tif.write(
                        data[..., ::mag, ::mag, :],
                        subfiletype=1,
                        resolution=(1e4 / mag / pixelsize, 1e4 / mag / pixelsize),
                        **options
                    )
                else:
                    tif.write(
                        data[..., ::mag, ::mag],
                        subfiletype=1,
                        resolution=(1e4 / mag / pixelsize, 1e4 / mag / pixelsize),
                        **options
                    )
        self.pyramid_dic[hash_value] = pyramid_image_location
        self.location = pyramid_image_location
        
    def divide_time(self,shape, factor, tile_size):
        max_axes = max(shape[self.axes_pos_dic.get('Y')],shape[self.axes_pos_dic.get('X')])
        times = 0 
        while max_axes>tile_size[0]:
            max_axes = max_axes // factor
            times = times + 1
        return times
    def axes_char_extract(self, axes):
        dic = {
            "T": None,
            "C": None,
            "Z": None,
            "Q": None,
            "Y": None,
            "X": None,
            "S": None,
        }
        characters = list(axes)
        # print("Axis characters:", characters)
        for index, char in enumerate(characters):
            if char in dic:
                dic[char] = index
        return dic

    def axes_value_extract(self, axes, shape):
        dic = {"T": None, "C": None, "Z": None, "Q":None}
        characters = list(axes)
        # print("Axis characters:", characters)
        for index, char in enumerate(characters):
            if char in dic:
                dic[char] = shape[index]
        return dic


    # def pyramid_inspector(self, r, num_axes, width, height):
    #     # width = self.image.series[r].pages[0].tags["ImageWidth"].value
    #     # height = self.image.series[r].pages[0].tags["ImageLength"].value
    #     if num_axes == 5:
    #         cur = r
    #         while self.pyramid_validator(width=width, height=height) != True:
    #             print("Grey-scale image pyramids bulding...")
    #             cur_shape = self.array[cur].shape
    #             next = cur + 1
    #             self.array[next] = np.empty(
    #                 (
    #                     cur_shape[0],
    #                     cur_shape[1],
    #                     cur_shape[2],
    #                     cur_shape[3] // 2,
    #                     cur_shape[4] // 2,
    #                 ),
    #                 dtype=self.dataType,
    #             )
    #             for t in range(cur_shape[0]):
    #                 for c in range(cur_shape[1]):
    #                     for z in range(cur_shape[2]):
    #                         result = self.local_mean_downsample_grey(
    #                             image=self.array[cur][t, c, z],
    #                             dataType=self.array[cur][t, c, z].dtype,
    #                         )
    #                         self.array[next][t, c, z] = result
    #             cur = next
    #             height = result.shape[0]
    #             width = result.shape[1]
    #     elif num_axes == 6:
    #         cur = r
    #         while self.pyramid_validator(width=width, height=height) != True:
    #             print("RGB image pyramids bulding...")
    #             cur_shape = self.array[cur].shape
    #             next = cur + 1
    #             self.array[next] = np.empty(
    #                 (
    #                     cur_shape[0],
    #                     cur_shape[1],
    #                     cur_shape[2],
    #                     cur_shape[3] // 2,
    #                     cur_shape[4] // 2,
    #                     cur_shape[5],
    #                 ),
    #                 dtype=self.dataType,
    #             )
    #             for t in range(cur_shape[0]):
    #                 for c in range(cur_shape[1]):
    #                     for z in range(cur_shape[2]):
    #                         print("========", self.array[cur][t, c, z].shape)
    #                         print("dataType: ", self.array[cur][t, c, z].dtype)
    #                         result = self.local_mean_downsample_rgb(
    #                             image=self.array[cur][t, c, z],
    #                             dataType=self.array[cur][t, c, z].dtype,
    #                         )
    #                         print("--------", result.shape)
    #                         self.array[next][t, c, z] = result
    #             cur = next
    #             height = result.shape[0]
    #             width = result.shape[1]

    # def pyramid_validator(self, width, height):
    #     if width > self.tile_size[0] or height > self.tile_size[1]:
    #         return False
    #     else:
    #         return True

    # def local_mean_downsample_rgb(self, image, dataType, down_sample_ratio=(2, 2, 1)):
    #     print("first int", dataType)
    #     image = img_as_float32(image)
    #     canvas = np.zeros(
    #         (
    #             image.shape[0] // down_sample_ratio[0],
    #             image.shape[1] // down_sample_ratio[1],
    #             image.shape[2] // down_sample_ratio[2],
    #         ),
    #         dtype=np.dtype("float32"),
    #     )

    #     # print(canvas.shape)
    #     for z, y, x in product(
    #         range(down_sample_ratio[0]),
    #         range(down_sample_ratio[1]),
    #         range(down_sample_ratio[2]),
    #     ):
    #         tmp = image[
    #             z :: down_sample_ratio[0],
    #             y :: down_sample_ratio[1],
    #             x :: down_sample_ratio[2],
    #         ][0 : canvas.shape[0], 0 : canvas.shape[1], 0 : canvas.shape[2]]
    #         canvas[0 : tmp.shape[0], 0 : tmp.shape[1], 0 : tmp.shape[2]] += tmp

    #     canvas /= math.prod(down_sample_ratio)
    #     if dataType == np.dtype("uint16"):
    #         return img_as_uint(canvas)

    #     if dataType == np.dtype("ubyte"):
    #         return img_as_ubyte(canvas)

    #     if dataType == np.dtype("float32"):
    #         return img_as_float32(canvas)

    #     if dataType == np.dtype(float):
    #         print("float64 checked")
    #         return img_as_float64(canvas)

    # def local_mean_downsample_grey(self, image, dataType, down_sample_ratio=(2, 2)):

    #     image = img_as_float32(image)
    #     canvas = np.zeros(
    #         (
    #             image.shape[0] // down_sample_ratio[0],
    #             image.shape[1] // down_sample_ratio[1],
    #         ),
    #         # dtype=np.dtype('float32')
    #     )

    #     # print(canvas.shape)
    #     for y, x in product(range(down_sample_ratio[0]), range(down_sample_ratio[1])):
    #         tmp = image[y :: down_sample_ratio[0], x :: down_sample_ratio[1]][
    #             0 : canvas.shape[0], 0 : canvas.shape[1]
    #         ]
    #         canvas[0 : tmp.shape[0], 0 : tmp.shape[1]] += tmp

    #     canvas /= math.prod(down_sample_ratio)

    #     if dataType == np.dtype("uint16"):
    #         print("uint16")
    #         return img_as_uint(canvas)

    #     if dataType == np.dtype("ubyte"):
    #         print("ubyte")
    #         return img_as_ubyte(canvas)

    #     if dataType == np.dtype("float32"):
    #         print("float32")
    #         return img_as_float32(canvas)

    #     if dataType == np.dtype(float):
    #         print("float")
    #         return img_as_float64(canvas)
