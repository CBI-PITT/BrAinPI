# import utils
# import os
# from flask import (
#     render_template,
#     request,
#     send_file,
#     redirect,
#     jsonify,
#     make_response,
#     Response,
# )
# import numpy as np
# from PIL import Image
# import io
# from flask_cors import cross_origin
# import tiff_loader


# def openseadragon_dtypes():
#     return [".tif"]


# openSeadragonPath = "/op_seadragon/"
# # dzi_template = """<?xml version="1.0" encoding="UTF-8"?>
# # <Image xmlns="http://schemas.microsoft.com/deepzoom/2008"
# #   Format="{format}"
# #   Overlap="1"
# #   TileSize="{tile_size}"
# # >
# #   <Size 
# #     Height="{height}"
# #     Width="{width}"
# #   />
# # </Image>"""
# # def dzi_exist_check(numpy_tif):
# #     if not hasattr(numpy_tif, "dzi_file"):
# #         # print("NOOOO dzi")
# #         numpy_tif.dzi_file= dzi_generation(numpy_tif)
# #         return numpy_tif.dzi_file
# #     else:
# #         # print("has dzi")
# #         return numpy_tif.dzi_file
# # def dzi_generation(numpy_tif):
# #     height = numpy_tif.height
# #     width = numpy_tif.width
# #     tile_size = numpy_tif.tile_size
# #     image_format = numpy_tif.format

# #     # DZI XML template

# #     # Format the XML template with dynamic values
# #     dzi_xml = dzi_template.format(
# #         height=height, width=width, tile_size=tile_size, format=image_format
# #     )
# #     return dzi_xml


# def setup_openseadragon(app, config):

#     # Establish highly used functions as objects to improve speed
#     get_html_split_and_associated_file_path = (
#         utils.get_html_split_and_associated_file_path
#     )

#     def openseadragon_entry(req_path):
#         path_split, datapath = get_html_split_and_associated_file_path(config, request)
#         print(path_split, datapath)
        
#         if utils.split_html(datapath)[-1].endswith(tuple(openseadragon_dtypes())):
#             datapath = config.loadDataset(datapath)
#             numpy_tif = config.opendata[datapath]
#             print(type(numpy_tif.height))
#             # consider no tile_size
#             # print(type(numpy_tif.tile_size[0]))
#             return render_template("openseadragon_temp.html",height=int(numpy_tif.height), width = int(numpy_tif.width),tileSize = int(numpy_tif.tile_size[0]), parent_url= '/'.join(path_split), resolutions = numpy_tif.resolutions, value = numpy_tif.channels, z_stack= numpy_tif.z)
#         elif utils.split_html(datapath)[-1].endswith('.png'):
#             # print('end with jpeg')
            
#             # NumPy array 
#             # to be optimized!!!
#             datapath_split = datapath.split('/')
#             datapath = config.loadDataset('/'+os.path.join(*datapath_split[:-6]))
#             numpy_tif = config.opendata[datapath]
#             key = datapath_split[-6:-1]
#             # print(key,'--------------')
#             slice = get_slice(numpy_tif, key)
#             # Convert the NumPy array to a PIL image
#             pil_image = Image.fromarray(slice)

#             # Create an in-memory byte stream to store the image data
#             image_stream = io.BytesIO()

#             # Save the PIL image as JPEG to the in-memory byte stream
#             pil_image.save(image_stream, format='png')

#             # Seek to the beginning of the stream (important)
#             image_stream.seek(0)

#             # Return the image data as a response with appropriate MIME type
#             return Response(image_stream, mimetype='image/png')
#         else:
#             return 'No end point recognized!'

#     openseadragon_entry = cross_origin(allow_headers=["Content-Type"])(openseadragon_entry)
#     openseadragon_entry = app.route(openSeadragonPath + "<path:req_path>")(openseadragon_entry)
#     openseadragon_entry = app.route(openSeadragonPath, defaults={"req_path": ""})(
#         openseadragon_entry
#     )
#     def get_slice(numpy_tif, key):
#          "r t c z y x (e)"
#         #  # symmetric 0 --> 8
#          r = int(key[0])
#          c = int(key[1])
#          z = int(key[2])
#          y = int(key[3])
#          x = int(key[4])
#          tile_size = int(numpy_tif.tile_size[0])
#         #  return((numpy_tif.array[r][0,0,0]))
#         #  print(numpy_tif.array[r][0,c,0,y*tile_size:(y+1)*tile_size,x*tile_size:(x+1)*tile_size].shape)
#          return numpy_tif.array[r][0,c,z,y*tile_size:(y+1)*tile_size,x*tile_size:(x+1)*tile_size]
#          numpy_array = np.random.randint(0, 255, size=(256, 256, 3), dtype=np.uint8)
#          return numpy_array

import utils
import os
from flask import (
    render_template,
    request,
    send_file,
    redirect,
    jsonify,
    make_response,
    Response,
)
import numpy as np
from PIL import Image
import io
from flask_cors import cross_origin
import tiff_loader


def openseadragon_dtypes():
    return [".tif",
            ".ome.tif"]


openSeadragonPath = "/op_seadragon/"
# dzi_template = """<?xml version="1.0" encoding="UTF-8"?>
# <Image xmlns="http://schemas.microsoft.com/deepzoom/2008"
#   Format="{format}"
#   Overlap="1"
#   TileSize="{tile_size}"
# >
#   <Size 
#     Height="{height}"
#     Width="{width}"
#   />
# </Image>"""
# def dzi_exist_check(numpy_tif):
#     if not hasattr(numpy_tif, "dzi_file"):
#         # print("NOOOO dzi")
#         numpy_tif.dzi_file= dzi_generation(numpy_tif)
#         return numpy_tif.dzi_file
#     else:
#         # print("has dzi")
#         return numpy_tif.dzi_file
# def dzi_generation(numpy_tif):
#     height = numpy_tif.height
#     width = numpy_tif.width
#     tile_size = numpy_tif.tile_size
#     image_format = numpy_tif.format

#     # DZI XML template

#     # Format the XML template with dynamic values
#     dzi_xml = dzi_template.format(
#         height=height, width=width, tile_size=tile_size, format=image_format
#     )
#     return dzi_xml


def setup_openseadragon(app, config):

    # Establish highly used functions as objects to improve speed
    get_html_split_and_associated_file_path = (
        utils.get_html_split_and_associated_file_path
    )

    def openseadragon_entry(req_path):
        path_split, datapath = get_html_split_and_associated_file_path(config, request)
        # print(path_split, datapath)
        
        if utils.split_html(datapath)[-1].endswith(tuple(openseadragon_dtypes())):
            datapath = config.loadDataset(datapath)
            tif_obj = config.opendata[datapath]
           
            return render_template("openseadragon_temp.html",height=int(tif_obj.height), width = int(tif_obj.width),tileSize = int(tif_obj.tile_size[0]), parent_url= '/'.join(path_split), resolutions = tif_obj.series, t_point = tif_obj.t, value = tif_obj.channels, z_stack= tif_obj.z)
        elif utils.split_html(datapath)[-1].endswith('.png'):
            # print('end with png')
            
            datapath_split = datapath.split('/')
            datapath = config.loadDataset('/'+os.path.join(*datapath_split[:-7]))
            # print('datapath', datapath)
            tif_obj = config.opendata[datapath]
            
            key = datapath_split[-7:-1]
            # return get_slice(tif_obj,key)
            cache_key = f'opsd_{datapath}-{key[0]}-{key[1]}-{key[2]}-{key[3]}-{key[4]}--{key[5]}'
            slice = None
            if config.cache is not None:
                # print("cache not none")
                slice = config.cache.get(cache_key, default=None, retry=True)
                if slice:
                    print('return from cache')
            if slice is None:
                # retrieve from the zarr 
                slice = tif_obj[key]

                pil_image = Image.fromarray(slice)

                # Create an in-memory byte stream to store the image data
                image_stream = io.BytesIO()

                # Save the PIL image as png to the in-memory byte stream
                pil_image.save(image_stream, format='png')

                # Seek to the beginning of the stream (important
                image_stream.seek(0)
                slice = image_stream
                print('return from disk')
                if config.cache is not None:
                    config.cache.set(cache_key, slice, expire=None, tag=datapath, retry=True)
            return Response(slice, mimetype='image/png')
            
        else:
            return 'No end point recognized!'

    openseadragon_entry = cross_origin(allow_headers=["Content-Type"])(openseadragon_entry)
    openseadragon_entry = app.route(openSeadragonPath + "<path:req_path>")(openseadragon_entry)
    openseadragon_entry = app.route(openSeadragonPath, defaults={"req_path": ""})(
        openseadragon_entry
    )
    # def get_slice(tif_obj, key):
    #     "r t c z y x (e)"
    # #  # symmetric 0 --> 8
    #     r = int(key[0])
    #     c = int(key[1])
    #     z = int(key[2])
    #     y = int(key[3])
    #     x = int(key[4])
    #     tile_size = int(tif_obj.tile_size[0])
    # #  return((numpy_tif.array[r][0,0,0]))
    # #  print(numpy_tif.array[r][0,c,0,y*tile_size:(y+1)*tile_size,x*tile_size:(x+1)*tile_size].shape)
    # #  return tif_obj.array[r][0,c,z,y*tile_size:(y+1)*tile_size,x*tile_size:(x+1)*tile_size]
    #     numpy_array = np.random.randint(0, 255, size=(256, 256, 3), dtype=np.uint8)
    #     slice = numpy_array
    #     pil_image = Image.fromarray(slice)

    #     # Create an in-memory byte stream to store the image data
    #     image_stream = io.BytesIO()

    #     # Save the PIL image as JPEG to the in-memory byte stream
    #     pil_image.save(image_stream, format='png')

    #     # Seek to the beginning of the stream (important)
    #     image_stream.seek(0)
    #     slice = image_stream
    #     return Response(slice, mimetype='image/png')

