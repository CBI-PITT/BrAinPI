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
from flask import render_template
from filelock import FileLock
from pathlib import Path
import shutil
import sys
from pympler import asizeof
from zarr.storage import KVStore
import gc


def calculate_hash(input_string):
    # Calculate the SHA-256 hash of the input string
    hash_result = hashlib.sha256(input_string.encode()).hexdigest()
    return hash_result


def get_directory_size(directory):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def delete_oldest_files(directory, size_limit):
    items = sorted(Path(directory).glob("*"), key=os.path.getctime)
    total_size = get_directory_size(directory)

    # Delete oldest items until the total size is within the size limit
    for item in items:
        if total_size <= size_limit:
            break
        if item.is_file():
            item_size = os.path.getsize(item)
            os.remove(item)
            total_size -= item_size
            print(f"Deleted file {item} of size {item_size} bytes")
        elif item.is_dir():
            dir_size = get_directory_size(item)
            shutil.rmtree(item)
            total_size -= dir_size
            print(f"Deleted directory {item} of size {dir_size} bytes")


class tiff_loader:
    def __init__(
        self, file_location, pyramid_images_connection, cache, settings
    ) -> None:
        # print('pyramid_images_connection',pyramid_images_connection)
        self.cache = cache
        self.settings = settings
        self.datapath = file_location
        self.location = file_location
        self.metaData = {}
        self.file_stat = os.stat(file_location)
        self.file_ino = str(self.file_stat.st_ino)
        self.modification_time = str(self.file_stat.st_mtime)
        self.file_size = self.file_stat.st_size
        self.allowed_file_size_gb = int(
            self.settings.get("tif_loader", "pyramids_images_allowed_store_size_gb")
        )
        self.allowed_file_size_byte = self.allowed_file_size_gb * 1024 * 1024 * 1024
        # with tifffile.TiffFile(self.datapath) as tif:
        #     self.image = tif
        self.image = tifffile.TiffFile(self.datapath)
        # self.image_ori = tifffile.TiffFile(self.datapath)
        self.filename, self.filename_extension = self.file_extension_split()
        self.tags = self.image.pages[0].tags
        self.photometric = self.image.pages[0].photometric
        self.compression = self.image.pages[0].compression
        # Extract width and height
        self.height = self.tags["ImageLength"].value
        self.width = self.tags["ImageWidth"].value
        # print(self.width,self.height)
        self.dataType = self.image.pages[0].dtype
        self.metaData["dataType"] = self.dataType
        # print(self.dataType)
        self.series = len(self.image.series)
        self.is_pyramidal = self.image.series[0].is_pyramidal
        # print("series", self.series)
        # print("levels", len(self.image.series[0].levels))
        if self.image.pages[0].is_tiled:
            # Get the tile size
            self.tile_size = (
                self.image.pages[0].tilelength,
                self.image.pages[0].tilewidth,
            )
        else:
            print("Assigning tile size (128, 128)")
            self.tile_size = (128, 128)

        self.arrays = {}

        self.type = self.image.series[0].axes
        self.axes_pos_dic = self.axes_char_extract(self.type)
        self.axes_value_dic = self.axes_value_extract(
            self.type, self.image.series[0].shape
        )
        self.channels = (
            self.axes_value_dic.get("C") if self.axes_value_dic.get("C") else 0
        )
        self.z = self.axes_value_dic.get("Z") if self.axes_value_dic.get("Z") else 0
        self.t = (
            self.axes_value_dic.get("T")
            if self.axes_value_dic.get("T")
            else (
                self.axes_value_dic.get("Q")
                if self.axes_value_dic.get("Q")
                else self.axes_value_dic.get("I") if self.axes_value_dic.get("I") else 0
            )
        )

        self.pyramid_dic = pyramid_images_connection
        print(self.type)
        print("axes_pos_dic", self.axes_pos_dic)
        print("axes_value_dic", self.axes_value_dic)
        for i_s, s in enumerate(self.image.series):
            print(f"Series {i_s}: {s}")
            for i_l, level in enumerate(s.levels):
                print(f"Level {i_l}: {level}")
                self.metaData[f"Series:{i_s}, Level:{i_l}"] = str(level)
        # if already pyramid image --> building the arrays
        # elif no pyramid but connection exist --> replace the location, building the arrays
        # elif no pyramid and no connection --> pyramid image generation, building connection using hash func and replace location, building arrays
        self.pyramid_validators(self.image)

        # del self.image
        # gc.collect()

        # if self.datapath.endswith(".ome.tif"):
        #     store = self.load_data(series=0)
        #     print(type(store))
        #     zarr_store = zarr.open(store, mode="r")
        #     for r in range(len(zarr_store)):
        #         self.arrays[r] = zarr_store[r]
        #         # print(self.arrays[r].shape)

        # elif self.datapath.endswith(".tif"):
        #     print("zarr.core.Array")
        #     for r in range(len(self.image.series)):
        #         store = self.load_data(series=r)
        #         print(type(store))
        #         # print("Memory consumption of store:", asizeof.asizeof(store), "bytes")
        #         zarr_store = zarr.open(store, mode="r")
        #         # print("Memory consumption of zarr_store:", asizeof.asizeof(zarr_store), "bytes")
        #         self.arrays[r] = zarr_store
        #         # print(self.arrays[r].shape)
        # print(self.arrays)
        # print("Arrays building complete")

    # def load_data(self, series):

    #     store = tifffile.imread(self.datapath, aszarr=True, series=series)
    #     return store

    # def __getitem__(self, key):
    #     "r t c z y x (s)"
    #     r = int(key[0])
    #     t = int(key[1]) if self.axes_value_dic.get("T") else None
    #     c = int(key[2]) if self.axes_value_dic.get("C") else None
    #     z = (
    #         int(key[3])
    #         if self.axes_value_dic.get("Z") or self.axes_value_dic.get("Q")
    #         else None
    #     )
    #     y = int(key[4])
    #     x = int(key[5])
    #     tile_size = int(self.tile_size[0])
    #     tp = tuple(filter(lambda x: x is not None, (t, c, z)))
    #     # print("tp",tp)
    #     result = self.arrays[r][
    #         *tp,
    #         y * tile_size : (y + 1) * tile_size,
    #         x * tile_size : (x + 1) * tile_size,
    #     ]
    #     cache_key = f"opsd_{self.file_ino + self.modification_time}-{key[0]}-{key[1]}-{key[2]}-{key[3]}-{key[4]}--{key[5]}"
    #     if self.cache is not None:
    #         self.cache.set(
    #             cache_key, result, expire=None, tag=self.datapath, retry=True
    #         )
    #     return result
    #     numpy_array = np.random.randint(0, 255, size=(256, 256, 3), dtype=np.uint8)
    #     return numpy_array

    def __getitem__(self, key):

        # r = int(key[0])
        # t = int(key[1]) if self.axes_value_dic.get("T") else None
        # c = int(key[2]) if self.axes_value_dic.get("C") else None
        # z = (
        #     int(key[3])
        #     if self.axes_value_dic.get("Z") or self.axes_value_dic.get("Q")
        #     else None
        # )
        # y = int(key[4])
        # x = int(key[5])
        # tile_size = int(self.tile_size[0])
        # tp = tuple(filter(lambda x: x is not None, (t, c, z)))
        # # print("tp",tp)
        # # series = self.image.series[r]
        # zarr_array = None
        # if self.filename_extension in['.ome.tif','.ome-tif','.ome.tiff','.ome-tiff']:
        #     zarr_array = tifffile.imread(self.datapath,series=0, level=r,selection=(slice(c,c+1),slice(y *tile_size,(y+1)* tile_size), slice(x* tile_size, (x+1)*tile_size)))
        #     zarr_array = np.squeeze(zarr_array)
        #     # zarr_array = tifffile.imread(self.datapath,aszarr=True,series=0,level=r)
        # elif self.filename_extension in['.tif','.tiff']:
        #     zarr_array = tifffile.imread(self.datapath,series=r, selection=(slice(c,c+1),slice(z,z+1),slice(y *tile_size,(y+1)* tile_size), slice(x* tile_size, (x+1)*tile_size)))
        #     zarr_array = np.squeeze(zarr_array)
        #     # zarr_array = tifffile.imread(self.datapath,aszarr=True,series=r)
        #     print(zarr_array.shape)
        #     print(asizeof.asizeof(zarr_array))
        # return zarr_array
        # numpy_array = np.random.randint(0, 255, size=(256, 256, 3), dtype=np.uint8)
        # return numpy_array
        # result = self.arrays[r][
        #     *tp,
        #     y * tile_size : (y + 1) * tile_size,
        #     x * tile_size : (x + 1) * tile_size,
        # ]
        # cache_key = f"opsd_{self.file_ino + self.modification_time}-{key[0]}-{key[1]}-{key[2]}-{key[3]}-{key[4]}--{key[5]}"
        # if self.cache is not None:
        #     self.cache.set(
        #         cache_key, result, expire=None, tag=self.datapath, retry=True
        #     )
        # return result

        # # "r t c z y x (s)"
        r = int(key[0])
        t = (
            int(key[1])
            if self.axes_value_dic.get("T")
            or self.axes_value_dic.get("Q")
            or self.axes_value_dic.get("I")
            else None
        )
        c = int(key[2]) if self.axes_value_dic.get("C") else None
        z = int(key[3]) if self.axes_value_dic.get("Z") else None
        y = int(key[4])
        x = int(key[5])
        tile_size_heiht = int(self.tile_size[0])
        tile_size_width = int(self.tile_size[1])
        tp = tuple(filter(lambda x: x is not None, (t, c, z)))
        # print("tp",tp)
        zarr_array = None
        if self.is_pyramidal:
            zarr_array = self.image.aszarr(series=0, level=r)
        else:
            zarr_array = self.image.aszarr(series=r, level=0)
        # if self.datapath.endswith((".ome.tif", ".ome-tif", ".ome.tiff", ".ome-tiff")):
        #     zarr_array = self.image.aszarr(series=0, level=r)
        #     # zarr_array = tifffile.imread(self.datapath,aszarr=True,series=0,level=r)
        # elif self.datapath.endswith((".tif", ".tiff")):
        #     zarr_array = self.image.aszarr(series=0, level=r)
        #     # zarr_array = tifffile.imread(self.datapath,aszarr=True,series=r)
        zarr_store = zarr.open(zarr_array)
        result = zarr_store[
            *tp,
            y * tile_size_heiht : (y + 1) * tile_size_heiht,
            x * tile_size_width : (x + 1) * tile_size_width,
        ]

        # numpy_array = np.random.randint(0, 255, size=(256, 256, 3), dtype=np.uint8)
        # return numpy_array

        cache_key = f"opsd_{self.file_ino + self.modification_time}-{key[0]}-{key[1]}-{key[2]}-{key[3]}-{key[4]}--{key[5]}"
        if self.cache is not None:
            self.cache.set(
                cache_key, result, expire=None, tag=self.datapath, retry=True
            )
        return result

    def pyramid_validators(self, tif):
        inspector_result = self.pyramid_inspectors(tif)
        # inspector_result = False
        print(f"inspector_result: {inspector_result}")
        if inspector_result:
            return
        else:
            self.pyramid_builders(tif)
            return       
    def pyramid_inspectors(self, tif):
        
        
        if self.is_pyramidal:
            # may need more check opeation
            return True
        else:
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
        image_name = self.image.filename
        # print(f'image name',{image_name})
        if image_name.endswith(".ome.tif"):
            extension_index = image_name.rfind(".ome.tif")
            file = image_name[:extension_index]
            extension = ".ome.tif"
        elif image_name.endswith(".ome.tiff"):
            extension_index = image_name.rfind(".ome.tiff")
            file = image_name[:extension_index]
            extension = ".ome.tiff"
        elif image_name.endswith(".ome-tiff"):
            extension_index = image_name.rfind(".ome-tiff")
            file = image_name[:extension_index]
            extension = ".ome-tiff"
        elif image_name.endswith(".ome-tif"):
            extension_index = image_name.rfind(".ome-tif")
            file = image_name[:extension_index]
            extension = ".ome-tif"
        elif image_name.endswith(".tif"):
            extension_index = image_name.rfind(".tif")
            file = image_name[:extension_index]
            extension = ".tif"
        elif image_name.endswith(".tiff"):
            extension_index = image_name.rfind(".tiff")
            file = image_name[:extension_index]
            extension = ".tiff"

        return [file, extension]

    def pyramid_builders(self, tif):
        hash_value = calculate_hash(self.file_ino + self.modification_time)
        pyramids_images_store = self.settings.get("tif_loader", "pyramids_images_store")
        pyramids_images_store_dir = (
            pyramids_images_store + hash_value[0:2] + "/" + hash_value[2:4] + "/"
        )
        suffix = ".ome.tif"
        pyramid_image_location = pyramids_images_store_dir + hash_value + suffix
        if self.pyramid_dic.get(hash_value) and os.path.exists(pyramid_image_location):
            self.datapath = self.pyramid_dic.get(hash_value)
            # self.image = tifffile.TiffFile(pyramid_image_location)
            print("Location replaced by generated pyramid image")
        else:
            # Avoid other gunicore workers to build pyramids images
            if os.path.exists(pyramid_image_location):
                print(
                    "Pyramid image was already built by first worker and picked up now by others"
                )
                self.pyramid_dic[hash_value] = pyramid_image_location
                self.datapath = pyramid_image_location
                # self.image = tifffile.TiffFile(pyramid_image_location)
            # 1 hash exists but the pyramid images(not loaded) are deleted during server running
            # 2 no hash and no pyramid images (first time generation)
            else:
                if tif.filename.endswith("ome.tif"):
                    # write pyramids based on ome.tif
                    self.pyramid_building_process(
                        tif.series[0].levels[0],
                        2,
                        hash_value,
                        pyramids_images_store_dir,
                        pyramid_image_location,
                    )
                elif tif.filename.endswith(".tif") or tif.filename.endswith(".tiff"):
                    # write pyramids based on tif
                    self.pyramid_building_process(
                        tif.series[0],
                        2,
                        hash_value,
                        pyramids_images_store,
                        pyramids_images_store_dir,
                        pyramid_image_location,
                    )
        self.image = tifffile.TiffFile(pyramid_image_location)
        self.is_pyramidal = True

    def pyramid_building_process(
        self,
        first_series,
        factor,
        hash_value,
        pyramids_images_store,
        pyramids_images_store_dir,
        pyramid_image_location,
    ):
        os.makedirs(pyramids_images_store_dir, exist_ok=True)
        file_temp = pyramid_image_location.replace(hash_value, "temp_" + hash_value)
        file_temp_lock = file_temp + ".lock"
        file_lock = FileLock(file_temp_lock)
        try: 
            with file_lock.acquire():
                print("File lock acquired.")
                if not os.path.exists(pyramid_image_location):
                    print(f"==> pyramid image is building...")
                    start_time = time.time()
                    subresolutions = self.divide_time(
                        first_series.shape, factor, self.tile_size
                    )
                    start_load = time.time()
                    data = first_series.asarray()
                    # data = tifffile.imread(self.location)
                    end_load = time.time()
                    load_time = end_load - start_load
                    print(
                        f"----------\nloading first series or level\n{self.datapath}\ntime: {load_time}\n----------"
                    )
                    pixelsize = 0.29  # micrometer
                    # prefix = 'py_'
                    # suffix = '.ome.tif'
                    # pyramids_images_store = self.settings.get('tif_loader', 'pyramids_images_store')
                    # pyramid_image_location = pyramids_images_store  + hash_value + suffix
                    with tifffile.TiffWriter(file_temp, bigtiff=True) as tif:

                        metadata = {
                            "axes": (
                                self.type.replace("I", "T")
                                if "I" in self.type
                                else self.type
                            ),
                            "SignificantBits": 10,
                            "TimeIncrement": 0.1,
                            "TimeIncrementUnit": "s",
                            "PhysicalSizeX": pixelsize,
                            "PhysicalSizeXUnit": "Âµm",
                            "PhysicalSizeY": pixelsize,
                            "PhysicalSizeYUnit": "Âµm",
                            #          'Channel': {'Name': ['Channel 1', 'Channel 2']},
                            #          'Plane': {'PositionX': [0.0] * 16, 'PositionXUnit': ['Âµm'] * 16}
                        }
                        options = dict(
                            photometric=self.photometric,
                            tile=self.tile_size,
                            compression=self.compression,
                            resolutionunit="CENTIMETER",
                        )

                        tif.write(
                            data,
                            subifds=subresolutions,
                            resolution=(1e4 / pixelsize, 1e4 / pixelsize),
                            metadata=metadata,
                            **options,
                        )
                        # in production use resampling to generate sub-resolution images
                        for level in range(subresolutions):
                            mag = factor ** (level + 1)
                            if self.type.endswith("S"):
                                tif.write(
                                    data[..., ::mag, ::mag, :],
                                    subfiletype=1,
                                    resolution=(
                                        1e4 / mag / pixelsize,
                                        1e4 / mag / pixelsize,
                                    ),
                                    **options,
                                )
                            else:
                                tif.write(
                                    data[..., ::mag, ::mag],
                                    subfiletype=1,
                                    resolution=(
                                        1e4 / mag / pixelsize,
                                        1e4 / mag / pixelsize,
                                    ),
                                    **options,
                                )
                    end_time = time.time()
                    execution_time = end_time - start_time
                    print(
                        f"----------\nactual pyramid generation\n{self.datapath}\ntime:{execution_time - load_time}\n----------"
                    )
                    print(
                        f"----------\npyramid image building complete\n{self.datapath}\ntotal execution time: {execution_time}\n----------"
                    )
                    os.rename(file_temp, pyramid_image_location)
                    print(
                        f"----------\n{self.datapath} connected to ==> {pyramid_image_location}\n----------"
                    )
                    if (
                        get_directory_size(pyramids_images_store)
                        > self.allowed_file_size_byte
                    ):
                        delete_oldest_files(
                            pyramids_images_store, self.allowed_file_size_byte
                        )
                else:
                    print("file detected!")
                    if os.path.exists(file_temp):
                        os.remove(file_temp)
            self.pyramid_dic[hash_value] = pyramid_image_location
            self.datapath = pyramid_image_location
        except Exception as e:
            print(f"An error occurred during generation process: {e}")
        finally:
        # self.image = tifffile.TiffFile(pyramid_image_location)
            # Ensure any allocated memory or resources are released
            if 'data' in locals():
                del data
            print("Resources cleaned up.")

    def divide_time(self, shape, factor, tile_size):
        # max_axes = max(
        #     shape[self.axes_pos_dic.get("Y")], shape[self.axes_pos_dic.get("X")]
        # )
        shape_y = shape[self.axes_pos_dic.get("Y")]
        shape_x = shape[self.axes_pos_dic.get("X")]
        times = 0
        while shape_y > tile_size[0] or shape_x > tile_size[1]:
            shape_y = shape_y // factor
            shape_x = shape_x // factor
            times = times + 1
        return times

    def axes_char_extract(self, axes):
        dic = {
            "T": None,
            "C": None,
            "Z": None,
            "Q": None,
            "I": None,
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
        dic = {"T": None, "C": None, "Z": None, "Q": None, "I": None}
        characters = list(axes)
        # print("Axis characters:", characters)
        for index, char in enumerate(characters):
            if char in dic:
                dic[char] = shape[index]
        return dic


class tif_file_precheck:
    def __init__(self, file_location) -> None:
        self.location = file_location
        self.image = tifffile.TiffFile(file_location)
        self.file_stat = os.stat(file_location)
        self.size = self.file_stat.st_size
        self.metaData = {}
        self.dataType = str(self.image.pages[0].dtype)
        self.metaData["dataType"] = self.dataType
        if self.image.pages[0].is_tiled:
            # Get the tile size
            self.tile_size = (
                self.image.pages[0].tilelength,
                self.image.pages[0].tilewidth,
            )
        else:
            print("Assigning tile size (128, 128)")
            self.tile_size = (128, 128)
        self.type = self.image.series[0].axes
        self.is_pyramidal = self.image.series[0].is_pyramidal
        self.axes_pos_dic = self.axes_char_extract(self.type)
        self.inspectors_result = self.pyramid_inspectors(self.image)
        self.metaData["inspectors_result"] = self.inspectors_result
        for i_s, s in enumerate(self.image.series):
            print(f"Series {i_s}: {s}")
            for i_l, level in enumerate(s.levels):
                print(f"Level {i_l}: {level}")
                self.metaData[f"Series:{i_s}, Level:{i_l}"] = str(level)
        del self.image
        gc.collect()
        # print('precheck size',asizeof.asizeof(self.image))

    def pyramid_inspectors(self, tif):
        if self.is_pyramidal:
            # may need more check opeation
            return True
        else:
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

    def axes_char_extract(self, axes):
        dic = {
            "T": None,
            "C": None,
            "Z": None,
            "Q": None,
            "I": None,
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
