import utils
import os
from flask import (
    render_template,
    request,
    redirect,
    jsonify,
    Response,
)
from PIL import Image
import io
from flask_cors import cross_origin

import hashlib
import tifffile
from tiff_loader import tif_file_precheck
import gc
from pympler import asizeof


def openseadragon_dtypes():
    return [".tif", ".tiff", ".ome.tif", ".ome.tiff", ".ome-tif", ".ome-tiff"]


def calculate_hash(input_string):
    # Calculate the SHA-256 hash of the input string
    hash_result = hashlib.sha256(input_string.encode()).hexdigest()
    return hash_result


openSeadragonPath = "/op_seadragon/"


def setup_openseadragon(app, config):
    allowed_file_size_gb = int(
        config.settings.get("tif_loader", "pyramids_images_allowed_generation_size_gb")
    )
    allowed_file_size_byte = allowed_file_size_gb * 1024 * 1024 * 1024
    # print('allowed_file_size',allowed_file_size_byte)

    get_html_split_and_associated_file_path = (
        utils.get_html_split_and_associated_file_path
    )

    def openseadragon_entry(req_path):
        path_split, datapath = get_html_split_and_associated_file_path(config, request)
        print(path_split, datapath)

        if utils.split_html(datapath)[-1].endswith(tuple(openseadragon_dtypes())):
            datapath_split = datapath.split("/")
            file_name = datapath_split[-1]

            view_path = request.path + "/view"
            file_precheck_info = ""
            try:
                file_precheck_info = tif_file_precheck(datapath)
                print(
                    f"----------\n file precheck info, is_pyramidal: {file_precheck_info.is_pyramidal}, inspector result: {file_precheck_info.inspectors_result}\n----------"
                )
            except Exception as e:
                return render_template(
                    "file_read_exception.html",
                    gtag=config.settings.get("GA4", "gtag"),
                    exception=e,
                )
            inspector_result = file_precheck_info.inspectors_result
            file_size = file_precheck_info.size
            del file_precheck_info
            gc.collect()
            # return 'checking'
            if inspector_result:
                return render_template(
                    "file_loading.html",
                    gtag=config.settings.get("GA4", "gtag"),
                    redirect_url=view_path,
                    redirect_name="OpenSeadragon",
                    description=datapath,
                    file_name=file_name,
                )
            else:
                if file_size > allowed_file_size_byte:
                    return render_template(
                        "file_size_warning.html",
                        gtag=config.settings.get("GA4", "gtag"),
                        variable=allowed_file_size_gb,
                    )
                else:
                    return render_template(
                        "py_generation.html",
                        gtag=config.settings.get("GA4", "gtag"),
                        redirect_url=view_path,
                        redirect_name="OpenSeadragon",
                        description=datapath,
                        file_name=file_name,
                    )

        elif utils.split_html(datapath)[-1].endswith("view"):
            path_split_list = list(path_split)
            path_split_list.remove("view")
            path_split_tuple = tuple(path_split_list)
            datapath = datapath.replace("/view", "")
            stat = os.stat(datapath)
            file_ino = str(stat.st_ino)
            modification_time = str(stat.st_mtime)
            datapath_key = config.loadDataset(file_ino + modification_time, datapath)
            tif_obj = config.opendata[datapath_key]
            #   further check if the file has been deleted during server runing
            #   mainly used for the generated pyramid images
            if not os.path.exists(tif_obj.datapath):
                print("may delete")
                del config.opendata[file_ino + modification_time]
                datapath_key = config.loadDataset(
                    file_ino + modification_time, datapath
                )
                tif_obj = config.opendata[datapath_key]
            return render_template(
                "openseadragon_temp.html",
                height=int(tif_obj.height),
                width=int(tif_obj.width),
                tileSize=int(tif_obj.tile_size[0]),
                host=config.settings.get("app", "url"),
                parent_url="/".join(path_split_tuple),
                t_point=tif_obj.t,
                value=tif_obj.channels,
                z_stack=tif_obj.z,
            )

        elif utils.split_html(datapath)[-1].endswith(".png"):
            # return 'break point'
            datapath_split = datapath.split("/")
            # The actual path excluded the r-t-c-z-y-x parameters
            datapath = "/" + os.path.join(*datapath_split[:-7])
            stat = os.stat(datapath)
            file_ino = str(stat.st_ino)
            modification_time = str(stat.st_mtime)
            datapath_key = config.loadDataset(file_ino + modification_time, datapath)
            # print('datapath', datapath)

            tif_obj = config.opendata[datapath_key]

            key = datapath_split[-7:-1]
            # return get_slice(tif_obj,key)
            cache_key = f"opsd_{datapath_key}-{key[0]}-{key[1]}-{key[2]}-{key[3]}-{key[4]}--{key[5]}"
            slice = None
            if config.cache is not None:
                # print("cache not none")
                slice = config.cache.get(cache_key, default=None, retry=True)
                if slice is not None:
                    print("return from cache")
            if slice is None:
                slice = tif_obj[key]
                print("return from disk")
            pil_image = Image.fromarray(slice)

            # Create an in-memory byte stream to store the image data
            image_stream = io.BytesIO()

            # Save the PIL image as png to the in-memory byte stream
            pil_image.save(image_stream, format="png")

            # Seek to the beginning of the stream (important
            image_stream.seek(0)
            slice = image_stream

            # if config.cache is not None:
            #     config.cache.set(cache_key, slice, expire=None, tag=datapath, retry=True)
            return Response(slice, mimetype="image/png")
        elif utils.split_html(datapath)[-1].endswith("info"):
            datapath = datapath.replace("/info", "")
            print(datapath)

            # stat = os.stat(datapath)
            # file_ino = str(stat.st_ino)
            # modification_time = str(stat.st_mtime)
            # datapath_key = str(config.loadDataset(file_ino + modification_time, datapath))
            # tif_obj = config.opendata[datapath_key]

            file_precheck_info = tif_file_precheck(datapath)
            meta_data_info = file_precheck_info.metaData
            # print(asizeof.asizeof(file_precheck_info))
            del file_precheck_info
            gc.collect()
            return jsonify(meta_data_info)
        else:
            return "No end point recognized!"

    openseadragon_entry = cross_origin(allow_headers=["Content-Type"])(
        openseadragon_entry
    )
    openseadragon_entry = app.route(openSeadragonPath + "<path:req_path>")(
        openseadragon_entry
    )
    # Not sure if it should be included or not
    openseadragon_entry = app.route(openSeadragonPath, defaults={"req_path": ""})(
        openseadragon_entry
    )
