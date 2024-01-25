# -*- coding: utf-8 -*-
"""
Created on Sun May 15 17:26:23 2022

@author: awatson
"""

from flask import request, Response, send_file, render_template, jsonify
import ast

import utils

requiredParam = (
    'dset',
    'res',
    'tstart','tstop','tstep',
    'cstart','cstop','cstep',
    'zstart','zstop','zstep',
    'ystart','ystop','ystep',
    'xstart','xstop','xstep'
    )

def initiate_get_array(app, config):
    
    def makesSlices(intArgs):
        tslice = slice(intArgs['tstart'],intArgs['tstop'],intArgs['tstep'])
        cslice = slice(intArgs['cstart'],intArgs['cstop'],intArgs['cstep'])
        zslice = slice(intArgs['zstart'],intArgs['zstop'],intArgs['zstep'])
        yslice = slice(intArgs['ystart'],intArgs['ystop'],intArgs['ystep'])
        xslice = slice(intArgs['xstart'],intArgs['xstop'],intArgs['xstep'])
        
        return tslice, cslice, zslice, yslice, xslice
    
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
        
        
    # A route to return specific dataset chunks.
    @app.route('/array', methods=['GET'])
    def get_array():
        '''
        Retrieve a slice: resolutionLevel, (t,c,z,y,x) specified with arguments as int or None
        
        tstart,tstop,tstep
        cstart,cstop,cstep
        zstart,zstop,zstep
        ystart,ystop,ystep
        xstart,xstop,xstep
    
        Returns
        -------
        Bytestring of compressed numpy array
    
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
        
        # for x in request.args:
        #     print(x)
        # print(jsonify(request.args))
        intArgs = {}
        for x in request.args:
            # print(x)
            if x == 'dset':
                # print(x + ' in dset')
                intArgs[x] = request.args[x]
            else:
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
    
    
