# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 11:06:07 2021

@author: alpha
"""

import flask, json, zarr, os, ast, functools
from flask import request, Response
import numpy as np

from bil_api.dataset_info import dataset_info
from bil_api import config
from bil_api import utils
from bil_api import zarrLoader
import imaris_ims_file_reader as ims


# import cProfile, pstats, io
# from pstats import SortKey


# def profile(func):
#     def wrapper(*args, **kwargs):
#         pr = cProfile.Profile()
#         pr.enable()
#         retval = func(*args, **kwargs)
#         pr.disable()
#         s = io.StringIO()
#         sortby = SortKey.CUMULATIVE  # 'cumulative'
#         ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
#         ps.print_stats()
#         print(s.getvalue())
#         return retval

#     return wrapper



config.opendata = {}

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Brain Image Library Archive API</h1>
<p>A prototype API for chunked loading of large Brain Image Library datasets.</p>'''



@app.route('/api/available-datasets', methods=['GET'])
def datasets():
    
    return json.dumps(dataset_info())




def mountDataset(name,storeType):
    
    dataSets = {
        'fmost':(r'H:\globus\pitt\bil\c01_0.zarr','zarrNested'),
        }
    
    if dataSets[name][1] == 'zarrNested':
        store = zarr.NestedDirectoryStore(dataSets[name][0])
        return zarr.open(store, mode='r')




# Return crucial information about a given dataset
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
        os.path.splitext(dsetPath)[1] == '.zarr':
            
            newMetaDict = {}
            for key in z.metaData:
                newMetaDict[str(key)] = z.metaData[key] if isinstance(z.metaData[key],np.dtype) == False else str(z.metaData[key])
            print(newMetaDict)
            metadata.update(newMetaDict)
        
        return json.dumps(metadata) 
    else:
        return "No dataset id was provided"
    



@functools.lru_cache(maxsize=10000, typed=False)
def grabArrayCache(datapath,intArgs):
    intArgs = eval(intArgs)
    
    tslice = slice(intArgs['tstart'],intArgs['tstop'],intArgs['tstep'])
    cslice = slice(intArgs['cstart'],intArgs['cstop'],intArgs['cstep'])
    zslice = slice(intArgs['zstart'],intArgs['zstop'],intArgs['zstep'])
    yslice = slice(intArgs['ystart'],intArgs['ystop'],intArgs['ystep'])
    xslice = slice(intArgs['xstart'],intArgs['xstop'],intArgs['xstep'])
    
    out = globals()['config'].opendata[datapath][intArgs['res'],tslice,cslice,zslice,yslice,xslice]
    
    
    return out



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
        
    cache = True
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
    print(intArgs)
    
    
    
    # dataPath = dataset_info()[intArgs['dset']][1]
    datapath = utils.loadDataset(intArgs['dset'])
    
    # if os.path.splitext(dataPath)[1] == '.ims':
    #     z = ims.ims(dataPath)
    
    if cache == False:
        tslice = slice(intArgs['tstart'],intArgs['tstop'],intArgs['tstep'])
        cslice = slice(intArgs['cstart'],intArgs['cstop'],intArgs['cstep'])
        zslice = slice(intArgs['zstart'],intArgs['zstop'],intArgs['zstep'])
        yslice = slice(intArgs['ystart'],intArgs['ystop'],intArgs['ystep'])
        xslice = slice(intArgs['xstart'],intArgs['xstop'],intArgs['xstep'])
    
        # No Caching
        out = globals()['config'].opendata[datapath][intArgs['res'],tslice,cslice,zslice,yslice,xslice]
    else:
        
        ## Cache
        out = grabArrayCache(datapath,str(intArgs))
    
    # out = z[intArgs['res'],tslice,cslice,zslice,yslice,xslice]
    # print(out.max())
    # out = np.zeros((5,5,5))
    out,_,_ = utils.compress_np(out)
    return Response(response=out, status=200,
                    mimetype="application/octet_stream")



# def launchAPI():
#     app.run(threaded=True,host='0.0.0.0')



if __name__ == '__main__':
    app.run(threaded=True,host='0.0.0.0')


    
    
    
    
    
    
    
    
    
