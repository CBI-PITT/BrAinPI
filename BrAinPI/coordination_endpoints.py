# -*- coding: utf-8 -*-
"""
Created on Wed Mar  22 11:12:07 2023

@author: alpha
"""
import os
from flask import (
    render_template,
    request,
    send_file,
    redirect,
    jsonify,
    url_for
    )

from file_type_support import ng_links
from neuroGlancer import neuroglancer_dtypes
from utils import from_path_to_html, get_path_map, dict_key_value_match, from_path_to_browser_html, strip_leading_training_slash

def inititate(app,config):
    settings = config.settings
    @app.route('/ng_supported_filetypes/', methods=['GET'])
    def neuroglancer_support():
        return jsonify(neuroglancer_dtypes())

    @app.route('/path_to_html_options/', methods=['GET'])
    def html_options():
        print(request.remote_addr)
        assert(isinstance(request.args, dict)), 'Expects a dictionary'
        assert 'path' in request.args, 'Expects a path key'
        return path_to_html_options(request.args['path'])
    def path_to_html_options(path):
        path_map = get_path_map(settings,
                                user_authenticated=True)  # <-- Force user_auth=True to get all possible paths, in this way all ng links will be shareable to anyone
        key = path
        if len(key) > 1 and key[-1] == '/':
            key = key[:-1]

        if len(key.strip()) == 0 or len(key.strip()) == '/':
            key = '/'

        paths = {}
        paths['path'] = key

        paths['neuroglancer'] = None
        paths['neuroglancer_metadata'] = None

        paths['omezarr'] = None
        paths['omezarr_neuroglancer_optimized'] = None
        paths['omezarr_8bit'] = None
        paths['omezarr_8bit_neuroglancer_optimized'] = None
        paths['omezarr_metadata'] = None
        paths['omezarr_8bit_metadata'] = None
        paths['omezarr_validator'] = None
        paths['omezarr_8bit_validator'] = None
        paths['omezarr_neuroglancer_optimized'] = None
        paths['omezarr_8bit_neuroglancer_optimized_validator'] = None

        if not os.path.exists(paths['path']):
            return jsonify(paths)

        html_base = settings.get('app','url')
        html_base = strip_leading_training_slash(html_base)
        print('html_base',html_base)
        html_path = from_path_to_browser_html(paths['path'],path_map, html_base)

        if html_path is not None:
            req_path = html_path.replace(html_base, '')
            print(req_path)
            ng_link = ng_links('/' + req_path)
            print('ng_link', ng_link)
            if ng_link is not None:
                validator_url = 'https://ome.github.io/ome-ngff-validator'
                validator_url = validator_url + '/?source='

                ng_link = html_base + strip_leading_training_slash(ng_link)

                paths['neuroglancer'] = ng_link
                paths['neuroglancer_metadata'] = paths['neuroglancer'] + '/info'

                omezarr_entry = paths['neuroglancer'].replace(url_for('neuro_glancer_entry'),url_for('omezarr_entry'))

                paths['omezarr'] = omezarr_entry + '.ome.zarr'
                paths['omezarr_neuroglancer_optimized'] = omezarr_entry + '.ng.ome.zarr'
                paths['omezarr_8bit'] = omezarr_entry + '.8bit.ome.zarr'
                paths['omezarr_8bit_neuroglancer_optimized'] = omezarr_entry + '.ng.8bit.ome.zarr'
                paths['omezarr_metadata'] = omezarr_entry + '.ome.zarr/.zattrs'
                paths['omezarr_8bit_metadata'] = omezarr_entry + '.8bit.ome.zarr/.zattrs'
                paths['omezarr_validator'] = validator_url + omezarr_entry + '.ome.zarr'
                paths['omezarr_8bit_validator'] = validator_url + omezarr_entry + '.8bit.ome.zarr'
                paths['omezarr_neuroglancer_optimized_validator'] = validator_url + omezarr_entry + '.ng.ome.zarr'
                paths['omezarr_8bit_neuroglancer_optimized_validator'] = validator_url + omezarr_entry + '.ng.8bit.ome.zarr'

        return jsonify(paths)

    return app

# path, path_map, req_path, entry_point