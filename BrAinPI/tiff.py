"""
Describes an endpoint for access of datasets as tiff files including whole datasets at specified resolutions
"""
import tifffile
from itertools import product
import io
import ast
import json
import re
import numpy as np
import os

import neuroglancer

from functools import lru_cache


## Project imports
# from dataset_info import dataset_info
import utils

from flask import (
    render_template,
    request,
    send_file,
    redirect,
    jsonify
    )

from flask_login import login_required
from flask_cors import cross_origin

def setup_tiff(app, config):
    # A route to return specific dataset chunks.

    requiredParam = (
        'dset',
        'res',
        'tstart', 'tstop', 'tstep',
        'cstart', 'cstop', 'cstep',
        'zstart', 'zstop', 'zstep',
        'ystart', 'ystop', 'ystep',
        'xstart', 'xstop', 'xstep',
        'ftype'
    )

    @app.route('/img', methods=['GET'])
    def tiff():
        '''
        Retrieve a tiff file for a specified array

        tstart,tstop,tstep
        cstart,cstop,cstep
        zstart,zstop,zstep
        ystart,ystop,ystep
        xstart,xstop,xstep

        Returns
        -------
        Image file of the specified array

        TEST:
            1000x1000px image
            http://127.0.0.1:5000/api/img/tiff?dset=5&res=0&tstart=0&tstop=1&tstep=1&cstart=0&cstop=1&cstep=1&zstart=0&zstop=1&zstep=1&ystart=0&ystop=1000&ystep=1&xstart=0&xstop=1000&xstep=1
            http://136.142.29.160:5000/api/img/tiff?dset=3&res=0&tstart=0&tstop=1&tstep=1&cstart=0&cstop=1&cstep=1&zstart=200&zstop=201&zstep=1&ystart=1000&ystop=2000&ystep=1&xstart=1000&xstop=2000&xstep=1

            Pretty large test:
            http://127.0.0.1:5000/api/img/tiff?dset=5&res=0&tstart=0&tstop=1&tstep=1&cstart=1&cstop=2&cstep=1&zstart=0&zstop=1&zstep=1&ystart=0&ystop=15000&ystep=1&xstart=0&xstop=15000&xstep=1
            http://136.142.29.160:5000/api/img/tiff?dset=3&res=0&tstart=0&tstop=1&tstep=1&cstart=1&cstop=2&cstep=1&zstart=200&zstop=201&zstep=1&ystart=0&ystop=15000&ystep=1&xstart=0&xstop=15000&xstep=1
        '''


        datapath = config.loadDataset(dset)

        print(request.args)
        if all((x in request.args for x in requiredParam)):
            pass
        else:
            args_needed = [x if x in request.args for x in requiredParam]
        else:
            return 'A required data set, resolution level or (t,c,z,y,x) start/stop/step parameter is missing'

        intArgs = {}
        for x in request.args:
            intArgs[x] = ast.literal_eval(request.args[x])

        # dataPath = dataset_info()[intArgs['dset']][1]
        datapath = config.loadDataset(intArgs['dset'])

        # Attempt to convert to dask array for parallel read
        # May need to do a deep copy so not to alter main class

        # tmpArray = config.opendata[datapath]
        # tmpArray.change_resolution_lock(intArgs['res'])
        # tmpArray = da.from_array(tmpArray, chunks=tmpArray.chunks)

        # t,c,z,y,x = makesSlices(intArgs)
        # out = tmpArray[t,c,z,y,x].compute()

        ###  End dask attempt

        out = grabArray(datapath, intArgs)
        print(out)

        img_ram = io.BytesIO()
        ## TODO: Build to include metadata into TIFF file
        tf.imwrite(img_ram, out)
        img_ram.seek(0)

        # img_ram = bytearray(img_ram.getvalue())
        # img_ram = io.BytesIO(img_ram)
        # tf.imread(img_ram)

        return send_file(
            img_ram,
            as_attachment=True,
            ## TODO: dynamic naming of file (specific request or based on region of request)
            download_name='out.tiff',
            mimetype='image/tiff'
        )

    return app
