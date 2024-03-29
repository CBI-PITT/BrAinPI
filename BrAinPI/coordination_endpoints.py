# -*- coding: utf-8 -*-
"""
Created on Wed Mar  22 11:12:07 2023

@author: alpha
"""
import os, glob
from flask import (
    render_template,
    request,
    send_file,
    redirect,
    jsonify,
    url_for
    )

# from urllib.parse import quote

from file_type_support import ng_links
from neuroGlancer import neuroglancer_dtypes
from utils import (from_path_to_html,
                   get_path_map,
                   dict_key_value_match,
                   from_path_to_browser_html,
                   strip_leading_trailing_slash,
                   fix_special_characters_in_html,
                   strip_trailing_new_line
                   )

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
        return jsonify(path_to_html_options(request.args['path']))

    def path_to_html_options(path):
        '''
        Takes the path (on disk) to a potential BrAinPI compatible dataset and return all possible links in a dictionary
        '''
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
            return paths

        html_base = settings.get('app','url')
        html_base = strip_leading_trailing_slash(html_base)
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

                ng_link = html_base + strip_leading_trailing_slash(ng_link)

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

                # Replace values that don't translate directly to url
                # Probably others that are missing
                for key,value in paths.items():
                    # Replace space with %20 (' ')
                    if value.startswith('http'):
                        paths[key] = fix_special_characters_in_html(value)

        return paths

    # @app.route('/curated_datasets/', methods=['GET'])
    # def curated_datasets():
    #     locations = settings['curated_datasets']
    #     locations = dict(locations)
    #
    #     if not hasattr(config,'curated_datasets'):
    #         datasets = {}
    #         for set_name, file in locations.items():
    #             datasets[set_name] = {}
    #             with open(file, 'r') as f:
    #                 for line in f.readlines():
    #                     if line[-1] == '\n':
    #                         l = line[:-1]
    #                     else:
    #                         l = line
    #                     options = path_to_html_options(l)
    #                     name = os.path.split(line)[-1]
    #                     datasets[set_name][name] = options
    #
    #         # from pprint import pprint as print
    #         print(datasets)
    #
    #         # Cache curated_datasets in the config object (doesn't allow for dynamic updates) but is better performance
    #         # Commenting below turns off caching in the config object so each time the files are reloaded
    #         # (allows for dynamic updates to curated datasets)
    #         # config.curated_datasets = datasets
    #     else:
    #         datasets = config.curated_datasets
    #
    #     return jsonify(datasets)

    # @app.route('/curated_datasets_2/', methods=['GET'])
    # def curated_datasets_2():
    #
    #     html_base = settings.get('app', 'url')
    #     html_base = strip_leading_trailing_slash(html_base)
    #
    #     html_options_url = url_for("html_options")
    #
    #     # Locations are directories which contain files or files which have each line pointing to a dataset on disk
    #     locations = settings['curated_datasets']
    #     locations = dict(locations)
    #
    #     datasets = {}
    #     for set_name, file in locations.items():
    #         datasets[set_name] = {}
    #         with open(file, 'r') as f:
    #             for line in f.readlines():
    #                 l = line
    #                 while l[-1] == '\n':
    #                     l = l[:-1]
    #                 # options = path_to_html_options(l)
    #                 query_to_path_to_html_options = f'{html_base}{html_options_url}?path={l}'
    #                 name = os.path.split(line)[-1]
    #                 datasets[set_name][name] = fix_special_characters_in_html(query_to_path_to_html_options)
    #
    #         # from pprint import pprint as print
    #         print(datasets)
    #
    #         # Cache curated_datasets in the config object (doesn't allow for dynamic updates) but is better performance
    #         # Commenting below turns off caching in the config object so each time the files are reloaded
    #         # (allows for dynamic updates to curated datasets)
    #         # config.curated_datasets = datasets
    #
    #     return jsonify(datasets)

    @app.route('/curated_datasets/', methods=['GET'])
    def curated_datasets_3():

        html_base = settings.get('app', 'url')
        html_base = strip_leading_trailing_slash(html_base)

        html_options_url = url_for("html_options")

        # Locations are directories which contain files or files which have each line pointing to a dataset on disk
        locations = settings['curated_datasets']
        locations = dict(locations)

        datasets = {'collections': {}}
        for set_name, file in locations.items():
            datasets['collections'][set_name] = []
            with open(file, 'r') as f:
                for line in f.readlines():
                    l = strip_trailing_new_line(line)
                    query_to_path_to_html_options = f'{html_base}{html_options_url}?path={l}'
                    name = os.path.split(line)[-1]
                    dataset = {
                        'name':strip_trailing_new_line(name),
                        'links':fix_special_characters_in_html(query_to_path_to_html_options)
                    }
                    datasets['collections'][set_name].append(dataset)

            # from pprint import pprint as print
            print(datasets)

            # Cache curated_datasets in the config object (doesn't allow for dynamic updates) but is better performance
            # Commenting below turns off caching in the config object so each time the files are reloaded
            # (allows for dynamic updates to curated datasets)
            # config.curated_datasets = datasets

        return jsonify(datasets)

    return app

# path, path_map, req_path, entry_point