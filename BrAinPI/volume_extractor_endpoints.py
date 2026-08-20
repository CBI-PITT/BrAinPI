"""Flask endpoints for extracting and downloading bounded image volumes."""

from skimage import io, img_as_float32, img_as_uint, img_as_ubyte
from skimage.transform import rescale
import tifffile
import io
from ast import literal_eval
import dask
from dask import delayed

import os, glob
from flask import (
    render_template,
    request,
    send_file,
    redirect,
    jsonify,
    url_for
    )
from flask_cors import cross_origin


from utils import (conv_np_dtypes)

from utils import (from_path_to_html,
                   get_path_map,
                   dict_key_value_match,
                   from_path_to_browser_html,
                   strip_leading_trailing_slash,
                   fix_special_characters_in_html,
                   strip_trailing_new_line,
                   exists
                   )

def dset_z_chunk_info(dataset, resolution_level, time_point, channel):
    """
    Retrieve chunk shape and calculate the number of chunks along the z-axis for the dataset.

    This function extracts the shape and chunk size for the last three dimensions 
    from the dataset metadata, then calculates the total number of chunks along the z-axis given the chunk size.

    Parameters:
        dataset: An object representing the dataset. It should support indexing into its
                 metaData with keys formatted as (resolution_level, time_point, channel, key).
        resolution_level (int): The resolution level from which to extract metadata.
        time_point (int): The time point index.
        channel (int): The channel index.

    Returns:
        tuple: A tuple containing:
            - chunks (tuple): The shape of chunks along each spatial axis.
            - max_chunks (int): The total number of chunks along the z-axis.
    """
    shape = dataset.metaData[resolution_level, time_point, channel,'shape'][-3:]
    chunks = dataset.metaData[resolution_level, time_point, channel, 'chunks'][-3:]
    # shape = dataset.shape[-3:]
    # chunks = dataset.chunks[-3:]

    last_chunk_size = shape[0] % chunks[0]
    max_chunks = shape[0] // chunks[0]
    if last_chunk_size > 0:
        max_chunks += 1

    return chunks, max_chunks

def yx_downsamp_by_z_chunk(dataset,res,t, c, yx_downsamp,chunk, anti_aliasing=True):
    """
    Take a 3d dataset and downsample in 2d (xy) in z chunks as stored on disk
    Always return a float becasuse the output is intended to be further
    downsampled z.  Called from func: get_Volume_At_Specific_Resolution

    Parameters:
        dataset: The dataset object containing the data and metadata.
        res (int): The resolution level index.
        t (int): The time point index.
        c (int): The channel index.
        yx_downsamp (tuple): The downsampling factors for the y and x dimensions.
        chunk (int): The index of the z-chunk to process.
        anti_aliasing (bool, optional): Whether to apply anti-aliasing during rescaling. Defaults to True.

    Returns:
        ndarray: The downsampled 2D image slice (or set of slices) from the specified chunk.
    """

    shape = dataset.shape[-3:]
    # chunks = dataset.chunks[-3:]
    #
    # last_chunk_size = shape[0]%chunks[0]
    # max_chunks = shape[0]//chunks[0]
    # if last_chunk_size > 0:
    #     max_chunks += 1

    chunks, max_chunks = dset_z_chunk_info(dataset, res, t, c)

    assert chunk <= max_chunks, f'Chunk {chunk} was specified but only {max_chunks} chunks exist'

    start = chunk * chunks[0]
    stop = start + chunks[0]
    if stop > shape[0]:
        stop = shape[0]

    print(f'{start=}')
    print(f'{stop=}')
    print(f'{chunks=}')

    current_set = img_as_float32(dataset[res, t, c, start:stop, :, :])
    current_set = current_set.squeeze()
    downsamp_set = rescale(current_set, (1,*yx_downsamp), anti_aliasing=anti_aliasing)

    return downsamp_set


def get_Volume_At_Specific_Resolution(
        dataset, output_resolution=(100, 100, 100), time_point=0, channel=0, anti_aliasing=False
):
    """
    This function extracts a  time_point and channel at a specific resolution.
    The function extracts the whole volume at the highest resolution_level without
    going below the designated output_resolution.  It then resizes to the volume
    to the specified resolution by using the skimage rescale function.

    The option to turn off anti_aliasing during skimage.rescale (anti_aliasing=False) is provided.
    anti_aliasing can be very time consuming when extracting large resolutions.

    Everything is completed in RAM, very high resolutions may cause a crash.

    Parameters:
        dataset: The dataset object containing the volume data and associated metadata.
        output_resolution (tuple, optional): The target resolution as a tuple (z, y, x). 
                                               Defaults to (100, 100, 100).
        time_point (int, optional): The time point index to extract. Defaults to 0.
        channel (int, optional): The channel index to extract. Defaults to 0.
        anti_aliasing (bool, optional): Whether to apply anti-aliasing during rescaling. 
                                        Defaults to False.

    Returns:
        ndarray: The volume data resized to the specified output resolution and cast to the dataset's data type.

    Raises:
        Exception: May raise exceptions if issues occur during reading or rescaling of the volume data.
    """

    # Find ResolutionLevel that is closest in size but larger
    resolutionLevelToExtract = 0
    print(f'{dataset.resolution=}')
    for res in range(dataset.ResolutionLevels):
        currentResolution = dataset.metaData[res ,time_point ,channel ,'resolution']
        print(f'{currentResolution=}')
        resCompare = [x <= y for x ,y in zip(currentResolution ,output_resolution)]
        resEqual = [x == y for x ,y in zip(currentResolution ,dataset.resolution)]
        if all(resCompare) == True or (all(resCompare) == False and any(resEqual) == True):
            resolutionLevelToExtract = res

    workingVolumeResolution = dataset.metaData[resolutionLevelToExtract ,time_point ,channel ,'resolution']
    print('Reading ResolutionLevel {}'.format(resolutionLevelToExtract))
    print(workingVolumeResolution)
    # workingVolume = dataset.get_Resolution_Level(resolutionLevelToExtract ,time_point=time_point ,channel=channel)

    print('Resizing volume from resolution in microns {} to {}'.format(str(workingVolumeResolution),
                                                                       str(output_resolution)))
    rescaleFactor = tuple([round(x / y, 5) for x, y in zip(workingVolumeResolution, output_resolution)])
    print('Rescale Factor = {}'.format(rescaleFactor))

    try:
        workingVolume = dataset[resolutionLevelToExtract, time_point, channel,:,:,:]
        workingVolume = workingVolume.squeeze()
        print(workingVolume.shape)

        workingVolume = img_as_float32(workingVolume)
        workingVolume = rescale(workingVolume, rescaleFactor, anti_aliasing=anti_aliasing)

        return conv_np_dtypes(workingVolume, dataset.dtype)

    except: # Numpy memory error (todo catch specific)
        import numpy as np
        print('Downsampling by plane because volume is too large')
        working_vol_shape = dataset.metaData[resolutionLevelToExtract ,time_point ,channel ,'shape']
        print(f'{working_vol_shape=}')
        working_vol_chunks = dataset.metaData[resolutionLevelToExtract, time_point, channel, 'chunks']
        print(f'{working_vol_chunks=}')
        # Read 1 plane at a time and downsample in 2d THEN downsample in Z
        # Read firt plane downsamp

        chunks, max_chunks = dset_z_chunk_info(dataset, resolutionLevelToExtract, time_point, channel)

        to_process = []
        for c in range(max_chunks):
            print(f'Processing chunk {c+1} of {max_chunks}')
            out = delayed(yx_downsamp_by_z_chunk)(dataset,resolutionLevelToExtract,time_point, channel, rescaleFactor[1:],c, anti_aliasing=anti_aliasing)
            to_process.append(out)
            # shape = out.shape
            # print(f'{shape=}')
        to_process = dask.compute(to_process)[0]
        to_process = np.stack(to_process)

        to_process = rescale(to_process, (rescaleFactor[0],1,1), anti_aliasing=anti_aliasing)

        return conv_np_dtypes(to_process, dataset.dtype)


def setup_extractor_endpoint(app, config):
    """
    Set up the Flask endpoint for volume extraction based on a specified resolution.

    This function defines and registers a Flask route (/get_resolution/) that takes 
    GET requests to extract a volume from the dataset at a specified resolution. 
    It reads query parameters (path, time_point, channel, resolution), loads the dataset, 
    extracts the volume using get_Volume_At_Specific_Resolution, and returns the 
    resulting image as an OME-TIFF file.

    Parameters:
        app: A Flask application instance.
        config: A configuration object with methods including:
                - loadDataset: To load the dataset given a path.
                - opendata: A dictionary holding opened dataset objects.

    Returns:
        The Flask application instance with the new route registered.
    """
    @app.route('/get_resolution/', methods=['GET'])
    @cross_origin(allow_headers=['Content-Type'])
    def get_resolution():
        """
        Flask endpoint to get the volume at a specific resolution.

        Query Parameters:
            path (str): The file path to the dataset.
            time_point (int, optional): The time point to extract (default is 0).
            channel (int, optional): The channel index (default is 0).
            resolution (str or tuple, optional): The desired output resolution, formatted as 
                                                 a tuple string like "(100, 100, 100)".
        
        Returns:
            A Flask response containing the OME-TIFF file of the processed volume, 
            or an error message if the path parameter is missing.
        """
        print(request.args)
        if 'path' not in request.args:
            return 'A path to a compatible dataset must be specified'

        datapath = request.args['path']
        time_point = request.args.get('time_point', 0)
        channel = request.args.get('channel', 0)
        resolution = request.args.get('resolution', '(100, 100, 100)')
        resolution = literal_eval(resolution)
        print(type(resolution))
        if isinstance(resolution, tuple):
            assert len(resolution) == 3, 'The resolution must be specified as 3 values along axes (z,y,x)'
            pass
        elif isinstance(resolution, (int,float)):
            resolution = (resolution,resolution,resolution)

        datapath = config.loadDataset(datapath,datapath)

        output = get_Volume_At_Specific_Resolution(
            config.opendata[datapath], output_resolution=resolution, time_point=time_point, channel=channel, anti_aliasing=False
        )

        img_ram = io.BytesIO()
        ## TODO: Build to include metadata into TIFF file
        tifffile.imwrite(img_ram, output, ome=True) # Write ome-tiff
        img_ram.seek(0)

        return send_file(
            img_ram,
            as_attachment=True,
            ## TODO: dynamic naming of file (specifc request or based on region of request)
            download_name='out.tiff',
            mimetype='image/tiff'
        )

    return app

