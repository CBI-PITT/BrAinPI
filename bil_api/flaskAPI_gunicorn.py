# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 11:06:07 2021

@author: alpha
"""

import flask, json, zarr, os, ast, re, io,sys
from flask import request, Response, send_file
from flask_cors import CORS,cross_origin
import numpy as np
import dask.array as da

from bil_api.dataset_info import dataset_info
# from bil_api import config
from bil_api import utils

import tifffile as tf
import io


from bil_api import zarrLoader
from bil_api import neuroGlancer
import imaris_ims_file_reader as ims


# from weave.weave_read import weave_read

'''
To run w/ gunicorn:  gunicorn -w 1 -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app
To run w/ gunicorn:  gunicorn -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app -w 1 --threads 12
To run w/ gunicorn:  gunicorn -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app -w 2 --threads 12
To run w/ gunicorn:  gunicorn -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app -w 4 --threads 6

To run development:  python -i /CBI_FastStore/cbiPythonTools/bil_api/bil_api/flaskAPI_gunicorn.py
'''

if os.name == 'nt':
    cacheLocation = r'c:\code\testCache'
elif 'c00' in os.uname()[1]:
    cacheLocation = '/scratch/api_cache'
else:
    cacheLocation = '/CBI_FastStore/tmpCache/bil_api'

cacheSizeGB=100
evictionPolicy='least-recently-used'
shards = 16
timeout=0.010

# Instantiate class that will manage all open datasets
# This will remain in the global env and be accessed by multiple route methods
config = utils.config(
    cacheLocation=cacheLocation,
    cacheSizeGB=cacheSizeGB,
    evictionPolicy=evictionPolicy,
    shards=shards,
    timeout=timeout
    )


app = flask.Flask(__name__)
# app.config["DEBUG"] = True

# cors = CORS(app, resources={r"/api/ng/*": {"origins": "*"}})
# app.config['CORS_HEADERS'] = 'Content-Type'


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
            
        metadata = utils.metaDataExtraction(z,strKey=True)
        
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
@app.route('/api/img/tiff', methods=['GET'])
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
    
    out = grabArrayCache(datapath,intArgs)
    print(out)
    
    img_ram = io.BytesIO()
    ## TODO: Build to include metadata into TIFF file
    tf.imwrite(img_ram,out)
    img_ram.seek(0)
    
    # img_ram = bytearray(img_ram.getvalue())
    # img_ram = io.BytesIO(img_ram)
    # tf.imread(img_ram)
    
    

    return send_file(
        img_ram,
        as_attachment=True,
        ## TODO: dynamic naming of file (specifc request or based on region of request)
        download_name='out.tiff',
        mimetype='image/tiff'
    )

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



from flask import render_template

fakePaths = ['/test/test/test.txt', '/test/test/test2.txt','/test.txt','/test/test']
ngPath = '/api/ng/' #<--- final slash is required for proper navigation through dir tree
@app.route(ngPath, defaults={'req_path': ''})
@app.route(ngPath + '<path:req_path>')
@cross_origin(allow_headers=['Content-Type'])
def dir_listing(req_path):
    # return str(request.url.split('/')[-2])
    
    ## Show / select available datasets in /ng
    if req_path == '':
        dsetNums = []
        dsetNames = []
        for items in dataset_info():
            dsetNums.append(str(items))
            dsetNames.append(dataset_info()[items][0])
        return render_template('ns_datasets.html', dsets=zip(dsetNums,dsetNames))
    
    
    # Display base path of specifc /ng dataset
    url_split = request.url.split(ngPath)[-1]
    url_path_split = url_split.split('/')
    url_path_split = [x for x in url_path_split if x != '']
    if url_split[-1] != '':
        if isinstance(re.match('[0-9]',url_path_split[0]),re.Match):
            dsetNum = int(url_path_split[0])
        else:
            return 'Path must be of style {}{} where {} refers to a specific dataset found at {}'.format(ngPath,'integer','integer',ngPath)
        
        datapath = config.loadDataset(dsetNum)
        
        if hasattr(config.opendata[datapath],'ng_files') == False or \
            hasattr(config.opendata[datapath],'ng_json') == False:
                
                # config.opendata[datapath].ng_files = \
                #     neuroGlancer.ng_files(config.opendata[datapath])
                    
                config.opendata[datapath].ng_json = \
                    neuroGlancer.ng_json(config.opendata[datapath],file='dict')
        
        # # Build appropriate File List in base path
        # if len(url_path_split) == 1:
        #     res_files = list(config.opendata[datapath].ng_files.keys())
        #     # return str(res_files)
        #     files = ['info', *res_files]
        #     files = [str(x) for x in files]
        #     path = [request.script_root]
        #     return render_template('vfs_bil.html', path=path, files=files)
        
        # Return 'info' json
        if len(url_path_split) == 2 and url_path_split[-1] == 'info':
            # return 'in'
            b = io.BytesIO()
            b.write(json.dumps(config.opendata[datapath].ng_json, indent=2, sort_keys=False).encode())
            b.seek(0)
            
            # resp = flask.Response(send_file(
            #         b,
            #         as_attachment=False,
            #         download_name='info',
            #         mimetype='application/json'
            #     ))
            # resp.headers['Access-Control-Allow-Origin'] = '*'
            # resp.headers['Access-Control-Allow-Headers']='Content-Type' 
            # return resp
            return send_file(
                b,
                as_attachment=False,
                download_name='info',
                mimetype='application/json'
            )
            
        if len(url_path_split) == 2 and isinstance(re.match('[0-9]+',url_path_split[-1]),re.Match):
            res = int(url_path_split[-1])
            files = config.opendata[datapath].ng_files[res]
            path = [request.script_root]
            return render_template('vfs_bil.html', path=path, files=files)
        
        file_name_template = '{}-{}_{}-{}_{}-{}'
        file_pattern = file_name_template.format('[0-9]+','[0-9]+','[0-9]+','[0-9]+','[0-9]+','[0-9]+')
        if len(url_path_split) == 3 and isinstance(re.match(file_pattern,url_path_split[-1]),re.Match):
            
            print(request.path + '\n')
            # print(url_path_split[-1] + '\n')
            
            x,y,z = url_path_split[-1].split('_')
            x = x.split('-')
            y = y.split('-')
            z = z.split('-')
            x = [int(x) for x in x]
            y = [int(x) for x in y]
            z = [int(x) for x in z]
            # print(x)
            # print(y)
            # print(z)
            img = config.opendata[datapath][
                int(url_path_split[-2]),
                slice(0),
                slice(None),
                slice(z[0],z[1]),
                slice(y[0],y[1]),
                slice(x[0],x[1])
                ]
            
            while img.ndim > 4:
                img = np.squeeze(img,axis=0)
                
            print(img.shape, file=sys.stderr)
            
            img = neuroGlancer.encode_ng_file(img, config.opendata[datapath].ng_json['num_channels'])
            # Flask return of bytesIO as file
            
            # resp = flask.Response(send_file(
            #     img,
            #     as_attachment=True,
            #     ## TODO: dynamic naming of file (specifc request or based on region of request)
            #     download_name=url_path_split[-1], # name needs to match chunk
            #     mimetype='application/octet-stream'
            # ))
            # resp.headers['Access-Control-Allow-Origin'] = '*'
            # return resp
            return send_file(
                img,
                as_attachment=True,
                ## TODO: dynamic naming of file (specifc request or based on region of request)
                download_name=url_path_split[-1], # name needs to match chunk
                mimetype='application/octet-stream'
            )
        
        
            
        
        # return str(config.opendata[datapath].ng_files[0][0:10])
        # return str(config.opendata[datapath].ng_json)
    

    # Show directory contents
    # path = [os.path.split(x)[0] for x in fakePaths]
    # files = [os.path.split(x)[1] for x in fakePaths]
    # return render_template('vfs_bil.html', path=path, files=files)



# if __name__ == '__main__':
#     app.run(threaded=True,host='0.0.0.0')
    
if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5001)


    
    
    
    # Request parameters
    # path             /foo/page.html
    # full_path        /foo/page.html?x=y
    # script_root      /myapplication
    # base_url         http://www.example.com/myapplication/foo/page.html
    # url              http://www.example.com/myapplication/foo/page.html?x=y
    # url_root         http://www.example.com/myapplication/
    
    
    
    
