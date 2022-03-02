# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 11:06:07 2021

@author: alpha
"""

import flask, json, zarr, os, ast
from flask import request, Response
import numpy as np

from bil_api.dataset_info import dataset_info
# from bil_api import config
from bil_api import utils
from bil_api import zarrLoader
import imaris_ims_file_reader as ims

from weave.weave_read import weave_read

'''
To run w/ gunicorn:  gunicorn -w 1 -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app
To run w/ gunicorn:  gunicorn -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app -w 1 --threads 12
To run w/ gunicorn:  gunicorn -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app -w 2 --threads 12
To run w/ gunicorn:  gunicorn -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app -w 4 --threads 6

To run development:  python -i /CBI_FastStore/cbiPythonTools/bil_api/bil_api/flaskAPI_gunicorn.py
'''

cacheLocation = r'c:\code\testCache'
cacheLocation = '/CBI_FastStore/tmpCache/bil_api'
cacheSizeGB=100
evictionPolicy='least-recently-used'
timeout=0.010

# Instantiate class that will manage all open datasets
# This will remain in the global env and be accessed by multiple route methods
config = utils.config(
    cacheLocation=cacheLocation,
    cacheSizeGB=cacheSizeGB,
    evictionPolicy=evictionPolicy,
    timeout=timeout
    )


app = flask.Flask(__name__)
# app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Brain Image Library Archive API</h1>
<p>A prototype API for chunked loading of large Brain Image Library datasets.</p>'''

##############################################################################

@app.route('/api/available-datasets', methods=['GET'])
def datasets():
    return json.dumps(dataset_info())

##############################################################################

# Return descriptive information about a given dataset
@app.route('/api/metadata', methods=['GET'])
def meta():
    print(request.args)
    if 'id' in request.args:
        dsetNum = int(request.args['id'])
        dsetPath = dataset_info()[dsetNum][1]
        print(dsetPath)
        print(os.path.split(dsetPath)[1])
        
        if os.path.splitext(dsetPath)[1] == '.ims':
            z = ims.ims(dsetPath)
        elif os.path.splitext(dsetPath)[1] == '.zarr':
            z = zarrLoader.zarrSeries(dsetPath)
        elif os.path.exists(os.path.join(dsetPath,'weave.json')):
            z = weave_read(dsetPath)
            z.metaData = z.meta
        else:
            print('API can currently only load Zarr and IMS datasets')
            return
            
        metadata = {'shape':z.shape,
                           'chunks':z.chunks,
                           'dtype':str(z.dtype),
                           'ndim':z.ndim,
                           'ResolutionLevels':z.ResolutionLevels,
                           'TimePoints':z.TimePoints,
                           'Channels':z.Channels
                           }
        
        if os.path.splitext(dsetPath)[1] == '.ims' or \
        os.path.splitext(dsetPath)[1] == '.zarr' or \
        os.path.exists(os.path.join(dsetPath,'weave.json')):
            
            newMetaDict = {}
            for key in z.metaData:
                newMetaDict[str(key)] = z.metaData[key] if isinstance(z.metaData[key],np.dtype) == False else str(z.metaData[key])
            print(newMetaDict)
            metadata.update(newMetaDict)
        
        return json.dumps(metadata) 
    else:
        return "No dataset id was provided"
    

###############################################################################

# A route to return specific dataset chunks.
@app.route('/api/fmostCompress', methods=['GET'])
def fmostCompress():
    '''
    Retrieve a slice: resolutionLevel, (t,c,z,y,x) specified with argments as int or None
    
    tstart,tstop,tstep
    cstart,cstop,cstep
    zstart,zstop,zstep
    ystart,ystop,ystep
    xstart,xstop,xstep

    Returns
    -------
    Bytestring of compresed numpy array

    '''
    
    requiredParam = (
        'dset',
        'res',
        'tstart','tstop','tstep',
        'cstart','cstop','cstep',
        'zstart','zstop','zstep',
        'ystart','ystop','ystep',
        'xstart','xstop','xstep'
        )
    
    print(request.args)
    if all((x in request.args for x in requiredParam)):
        pass
    else:
        return 'A required data set, resolution level or (t,c,z,y,x) start/stop/step parameter is missing'
    
    intArgs = {}
    for x in request.args:
        intArgs[x] = ast.literal_eval(request.args[x])
    # print(intArgs)
    
    
    
    # dataPath = dataset_info()[intArgs['dset']][1]
    datapath = config.loadDataset(intArgs['dset'])
    
    # if os.path.splitext(dataPath)[1] == '.ims':
    #     z = ims.ims(dataPath)
    
    if config.cache is None:
        t,c,z,y,x = makesSlices(intArgs)
        # No Caching
        out = config.opendata[datapath][intArgs['res'],t,c,z,y,x]
    else:
        # Cache
        # out = grabArrayCache(datapath,str(intArgs)) #use when using lru_cache
        out = grabArrayCache(datapath,intArgs)
        # print(out)
    
    # out = z[intArgs['res'],tslice,cslice,zslice,yslice,xslice]
    # print(out.max())
    # out = np.zeros((5,5,5))
    out,_,_ = utils.compress_np(out)
    return Response(response=out, status=200,
                    mimetype="application/octet_stream")



##############################################################################



# A route to return specific dataset chunks.
@app.route('/api/img', methods=['GET'])
def fmostCompress():
    '''
    Retrieve an image file for a specified array
    
    tstart,tstop,tstep
    cstart,cstop,cstep
    zstart,zstop,zstep
    ystart,ystop,ystep
    xstart,xstop,xstep

    Returns
    -------
    Image file of the specified array

    '''
    
    requiredParam = (
        'dset',
        'res',
        'tstart','tstop','tstep',
        'cstart','cstop','cstep',
        'zstart','zstop','zstep',
        'ystart','ystop','ystep',
        'xstart','xstop','xstep'
        )
    
    print(request.args)
    if all((x in request.args for x in requiredParam)):
        pass
    else:
        return 'A required data set, resolution level or (t,c,z,y,x) start/stop/step parameter is missing'
    
    intArgs = {}
    for x in request.args:
        intArgs[x] = ast.literal_eval(request.args[x])
    # print(intArgs)
    
    
    
    # dataPath = dataset_info()[intArgs['dset']][1]
    datapath = config.loadDataset(intArgs['dset'])
    
    # if os.path.splitext(dataPath)[1] == '.ims':
    #     z = ims.ims(dataPath)





##############################################################################

def makesSlices(intArgs):
    tslice = slice(intArgs['tstart'],intArgs['tstop'],intArgs['tstep'])
    cslice = slice(intArgs['cstart'],intArgs['cstop'],intArgs['cstep'])
    zslice = slice(intArgs['zstart'],intArgs['zstop'],intArgs['zstep'])
    yslice = slice(intArgs['ystart'],intArgs['ystop'],intArgs['ystep'])
    xslice = slice(intArgs['xstart'],intArgs['xstop'],intArgs['xstep'])
    
    return tslice, cslice, zslice, yslice, xslice

##############################################################################

if config.cache is not None:
    @config.cache.memoize()
    def grabArrayCache(datapath,intArgs):
        
        # intArgs = eval(intArgs)
        '''
        intArgs = eval(intArgs) was used to deal with lru_cache which did not 
        like tuples as arguments.  However, diskcache.memorize() works fine.
        '''
        t,c,z,y,x = makesSlices(intArgs)
        out = config.opendata[datapath][intArgs['res'],t,c,z,y,x]
        return out



# def launchAPI():
#     app.run(threaded=True,host='0.0.0.0')



# if __name__ == '__main__':
#     app.run(threaded=True,host='0.0.0.0')
    
if __name__ == '__main__':
    app.run()


    
    
    
    
    
    
    
    
    
