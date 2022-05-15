# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 13:28:24 2022

@author: awatson
"""

import flask, json, zarr, os, ast
from flask import request, Response, send_file
import numpy as np
import dask.array as da

from bil_api.dataset_info import dataset_info
# from bil_api import config
from bil_api import utils

import tifffile as tf
import io


from bil_api import zarrLoader
import imaris_ims_file_reader as ims

# from flaskAPI_gunicorn import app
# from flask import render_template


fakePaths = ['c:/test/test/test.txt', 'c:/test/test/test2.txt']

# @app.route('/', defaults={'req_path': ''})
@app.route('/api/vfs', defaults={'req_path': ''})
@app.route('/api/vfs/<path:req_path>')
def dir_listing(req_path):

    # Show directory contents
    files = fakePaths
    return render_template('vfs.html', files=files)





# <!doctype html>
# <ul>
#     {% for file in files %}
#     <li>
#         <a href="{{ (request.path + '/' if request.path != '/' else '') + file }}">
#             {{ (request.path + '/' if request.path != '/' else '') + file }}
#         </a>
#     </li>
#     {% endfor %}
# </ul>