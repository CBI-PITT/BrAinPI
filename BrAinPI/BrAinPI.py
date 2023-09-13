# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 11:06:07 2021

@author: alpha
"""

import flask, json, os, ast, re, io, sys
from flask import request, Response, send_file, render_template, jsonify,url_for, redirect
from flask_cors import cross_origin, CORS
import numpy as np
from datetime import datetime, timedelta, timezone
# import dask.array as da

## Project imports
import auth
import utils
from utils import compress_flask_response
import coordination_endpoints


## File-type handler imports (some are project specific)
import tifffile as tf
import zarrLoader
import zarr_zip_sharded_loader
import imaris_ims_file_reader as ims
from ome_zarr_loader import ome_zarr_loader

from functools import lru_cache




os.chdir(sys.path[0])

# from weave.weave_read import weave_read

'''
https://blog.miguelgrinberg.com/post/the-flask-mega-tutorial-part-i-hello-world

To run w/ gunicorn:  gunicorn -w 1 -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app
To run w/ gunicorn:  gunicorn -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app -w 1 --threads 12
To run w/ gunicorn:  gunicorn -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app -w 2 --threads 12
To run w/ gunicorn:  gunicorn -b 0.0.0.0:5000 --chdir /CBI_FastStore/cbiPythonTools/bil_api/bil_api wsgi:app -w 4 --threads 6

To run development:  python -i /CBI_FastStore/cbiPythonTools/bil_api/bil_api/flaskAPI_gunicorn.py
'''
## Grab settings information from config.ini file
settings = utils.get_config('settings.ini')

## Setup cache location based on OS type
## Optional situations like machine name can be used to customize
if os.name == 'nt':
    cacheLocation = settings.get('disk_cache','location_win')
elif 'c00' in os.uname()[1]:
    cacheLocation = settings.get('disk_cache','location_unix')
else:
    cacheLocation = settings.get('disk_cache','location_unix')
    #cacheLocation = None

# Instantiate class that will manage all open datasets
# This will remain in the global env and be accessed by multiple route methods
config = utils.config(
    cacheLocation=cacheLocation,
    cacheSizeGB=settings.getint('disk_cache','cacheSizeGB'),
    evictionPolicy=settings.get('disk_cache','evictionPolicy'),
    shards=settings.getint('disk_cache','shards'),
    timeout=settings.getfloat('disk_cache','timeout')
    )

config.settings = utils.get_config('settings.ini') #<-- need to add this to config class so each chunk access doesn't require a read of settings file

TEMPLATE_DIR = os.path.abspath(settings.get('app','templates_location'))
STATIC_DIR = os.path.abspath(settings.get('app','static_location'))
# LOGO = os.path.abspath(settings.get('app','logo'))

app = flask.Flask(__name__,template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
app.config["DEBUG"] = settings.getboolean('app','debug')

# cors = CORS(app, resources={r"/api/ng/*": {"origins": "*"}})
# app.config['CORS_HEADERS'] = 'Content-Type'

## Import auth routes
from auth import setup_auth
app,login_manager = setup_auth(app)

## Initiate browser functionality
from fs_browse import initiate_browseable
app = initiate_browseable(app,config)

from ome_zarr import setup_omezarr
app = setup_omezarr(app,config)

from coordination_endpoints import inititate
app = inititate(app, config)

# ## Initiate development_browser functionality
# from fs_browse_new import initiate_browseable
# app = initiate_browseable(app)

@app.route('/', methods=['GET'])
def home():
    return render_template('home.html',
                           user=auth.user_info(),
                           app_name=settings.get('app','name'),
                           app_description=settings.get('app','description'),
                           app_motto=settings.get('app','motto'),
                           app_logo=settings.get('app','logo'),
                           page_name='Home',
                           gtag=settings.get('GA4','gtag'))
#     return '''<h1>Brain Image Library Archive API</h1>
# <p>A prototype API for chunked loading of large Brain Image Library datasets.</p>'''

##############################################################################

@app.route('/curated-datasets', methods=['GET'])
def datasets():
    return json.dumps(dataset_info())

##############################################################################
###############################################################################

# Return descriptive information about arrays within a given dataset
meta_base = '/metadata/'
@app.route(meta_base + '<path:req_path>')
@app.route(meta_base, defaults={'req_path': ''})
def meta(req_path):
    print(request.args)
    try:
        # dsetNum = int(request.args['id'])
        # dsetPath = dataset_info()[dsetNum][1]
        _, datapath = utils.get_html_split_and_associated_file_path(config, request)
        dsetPath = config.loadDataset(datapath)
        # dsetPath = request.args['id']
        print(dsetPath)
        print(os.path.split(dsetPath)[1])

        try:
            print('Trying metadata extraction')
            metadata = config.opendata[dsetPath].metadata
            print('Successful metadata extraction')
            print(metadata)
            print(isinstance(metadata,dict))
        except:
            print('An error occurred while trying to get metadata')
            return

        # Remove any tuples or lists as keys to enable jsonify
        new_meta = {}
        for key, value in metadata.items():
            if isinstance(key,(tuple,list)):
                new_meta[str(key)] = value
            else:
                new_meta[key] = value

        return jsonify(new_meta)

    except Exception as e:
        print(e)
        return "Dataset was not found"

##############################################################################
##############################################################################

print('Importing Neuroglancer endpoints')
import neuroGlancer
app = neuroGlancer.setup_neuroglancer(app, config)

import gzip
@app.after_request
def add_header(response):
    '''
    Add cache-control headers to all responses to reduce burden on server
    Changing seconds object will determine how long the response is valid
    '''
    # print(request.headers)
    seconds = 864000 # 10 days
    # then = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    # response.headers.add('Expires', then.strftime("%a, %d %b %Y %H:%M:%S GMT"))
    # print(response.headers)
    added_headers = False
    content_type = response.headers.get('Content-Type')
    if 'octet_stream' in content_type:
        # Cache but don't gzip
        # Gzip can be handled by the specific process sending these data
        # Neuroglancer data is not compressed natively so it is enabled
        # However, ome-zarr is compressed, so gzip would not be helpful
        response.headers.add('Cache-Control', f'public,max-age={seconds}')

    elif 'application/json' in content_type or 'html' in content_type:
        '''
        ################################################
        ## ADD HEADERS TO DISABLE CACHE ON JSON/HTML  ##
        ## In general these docs may update often, so ##
        ## caching may break the program              ##
        ################################################
        Cache-Control: no-cache, no-store, must-revalidate
        Pragma: no-cache
        Expires: 0
        '''
        response.headers.add('Cache-Control', 'no-cache,no-store,must-revalidate,max-age=0')
        response.headers.add('Pragma', 'no-cache')
        response.headers.add('Expires', '0')

        response = compress_flask_response(response,request,9)

    else:
        # Everything else is cached
        response.headers.add('Cache-Control', f'public,max-age={seconds}')
        response = compress_flask_response(response, request, 9)

    return response

# @app.route('/test/route/**/', methods=['GET'])
# def fmostCompress():


# CORS(app)

# if __name__ == '__main__':
#     app.run(threaded=True,host='0.0.0.0')
    
if __name__ == '__main__':
    if os.name == 'nt':
        app.run(host='0.0.0.0',port=5000)
    elif 'c00' in os.uname()[1]:
        app.run(host='0.0.0.0',port=5001)
    else:
        app.run(host='0.0.0.0',port=5000)

    


    
    
    
    # Request parameters
    # path             /foo/page.html
    # full_path        /foo/page.html?x=y
    # script_root      /myapplication
    # base_url         http://www.example.com/myapplication/foo/page.html
    # url              http://www.example.com/myapplication/foo/page.html?x=y
    # url_root         http://www.example.com/myapplication/
    
    
    
    
