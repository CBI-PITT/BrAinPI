# -*- coding: utf-8 -*-
"""
Created on Mon Mar 28 11:47:11 2022

@author: awatson
"""

'''
Make a browseable filesystem that limits paths to those configured in 
settings.ini and according to authentication / groups.ini
'''

import traceback
from flask_login import (
                         current_user,
                         login_required,
                         logout_user
                         )

from flask import render_template,request, flash, url_for, redirect
import glob, os
from natsort import natsorted
from flask import jsonify
import utils

# url_to_path

def initiate_browseable(app):
    from flaskAPI_gunicorn import login_manager
    
    # base entrypoint must always begin and end with '/' --> /my_entry/
    base = '/browser_new/'
    @app.route(base + '<path:req_path>')
    @app.route(base, defaults={'req_path': ''})
    def browse_fs_new(req_path):

        print(request.path)
        
        # Split the requested path to a tuple that can be reused below
        html_path_split = utils.split_html(request.path)
        print(html_path_split)
        # print(url_for(browse_fs_new))
        
        # Extract settings information that can be reused
        # Doing this here allows changes to paths to be dynamic (ie changes can be made while server is live)
        # May want to change this so that each browser access does not require access to settings file on disk.
        ## DEFAULT ## utils.get_config(file='settings.ini',allow_no_value=True)
        settings =  utils.get_config()
        
        # Determine what directories that users are allowed to browse 
        # based on authentication status and boot them if the path is not valid
        path_map =  utils.get_path_map(settings,current_user.is_authenticated)
        
        if current_user.is_authenticated:
            
            # Read in group information
            groups = utils.get_config('groups.ini',allow_no_value=True)
            
            # Build a list of allowed folders
            allowed_list = [current_user.id.lower()]
            for ii in groups: # Group names
                if ii.lower() == 'all':
                    continue
                for oo in groups[ii]: # Users in each group
                    if current_user.id.lower() == oo.lower(): # Current user matches the user in the group
                        print('Line 168')
                        allowed_list.append(ii.lower())
            print(allowed_list)
        
        
        # If browsing the root gather current_path info in a special way
        if len(html_path_split) == 1:
            
            to_browse = [x for x in path_map]
            to_browse = natsorted(to_browse)
            
            current_path = {}
            # current_path['root'] = base[:-1]
            current_path['files'] = []
            current_path['current_path'] = request.path
            current_path['parent_path'] = base[:-1]
            current_path['parent_is_root'] = True
            current_path['parent_folder_name'] = base[1:-1]
            
            current_path['dirs'] = [path_map[x] for x in to_browse] #converts to real path
            print(current_path['dirs'])
            # current_path['dirs_stat'] = [os.stat(x) for x in current_path['dirs']]
            # print(current_path['dirs_stat'])
            current_path['files_stat'] = []
            current_path['files_size'] = []
            
            current_path['dirs_entries'] = [utils.num_dirs_files(x) for x in current_path['dirs']]
            print(current_path['dirs_entries'])
            ## Reconstruct html_paths
            current_path['dirs'] = to_browse
            current_path['files'] = []
            
            print(current_path['dirs'])
            # return render_template('vfs_bil.html', path=path, files=files)
            return jsonify(current_path)
        
        else:
            
            try:
                print('Line 73')
                
                if html_path_split[1] not in path_map:
                    flash('You are not authorized to browse to path {}'.format(request.path))
                    return redirect(url_for('login'))
                
                
                
                ## Boot user if they are gaining access to an inappropriate folder
                ## Retain access to anon folders
                ##Retain access to everything if in 'all' group
                ## Determine if folders should be restricted to user-name-matched only
                ## Retain if folder name is in allowed_list of usernames / groups
                if len(html_path_split) >= 3 and \
                    not html_path_split[1].lower() in [x.lower() for x in settings['dir_anon']] and \
                    not current_user.id.lower() in [x.lower() for x in groups['all']] and \
                    settings.getboolean('auth', 'restrict_paths_to_matched_username') and \
                    not html_path_split[2].lower() in allowed_list: 
                    
                    flash('You are not authorized to browse to path {}'.format(request.path))
                    return redirect(url_for('login'))
               
               
               
                # Construct real paths from names in path_map dict
                to_browse = utils.from_html_to_path(request.path, path_map)
                print('Live 110')
                print(to_browse)
            
                
                # Get current directory listing by Files, Directories and stats on each
                current_path = {}
                current_path['current_path'] = request.path
                current_path['parent_path'] = '/' + os.path.join(*html_path_split[:-1]) if len(html_path_split) > 2 else base[:-1]
                current_path['parent_is_root'] = False if len(html_path_split) > 2 else True
                current_path['parent_folder_name'] = html_path_split[-2]
                if os.path.isdir(to_browse):
                    # current_path['isfile'] = False
                    for root, dirs, files in os.walk(to_browse):
                        # current_path['root'] = root
                        current_path['dirs'] = dirs
                        current_path['files'] = files
                        break
                    current_path['dirs'] = [os.path.join(root,x) for x in current_path['dirs']]
                    current_path['files'] = [os.path.join(root,x) for x in current_path['files']]
                    
                    #keep only directories that have the correct user/group names
                    if not html_path_split[1].lower() in [x.lower() for x in settings['dir_anon']] and \
                        current_user.is_authenticated and \
                        not current_user.id.lower() in [x.lower() for x in groups['all']]:
                            
                            if settings.getboolean('auth', 'restrict_paths_to_matched_username'):
                                current_path['dirs'] = [x for x in current_path['dirs'] if utils.split_html(x)[2].lower() in allowed_list]
                            
                            if settings.getboolean('auth','restrict_files_to_listed_file_types'):
                                to_view = []
                                for ii in settings['file_types']:
                                    # Further limit to the file type being filtered
                                    current_files = [ x for x in current_path['files'] if utils.is_file_type(settings['file_types'][ii], utils.split_html(x)[-1]) ]#<-- add 'a' to make split consistent when files start with '.'
                                    to_view = to_view + current_files
                                current_path['files'] = to_view
                    
                    current_path['dirs'] = natsorted(current_path['dirs'])
                    current_path['files'] = natsorted(current_path['files'])
                    
                    # current_path['dirs_stat'] = [os.stat(x) for x in current_path['dirs']]
                    current_path['files_stat'] = [os.stat(x) for x in current_path['files']]
                    current_path['files_size'] = [utils.get_file_size(x.st_size) for x in current_path['files_stat']]  
                    
                    current_path['dirs_entries'] = [utils.num_dirs_files(x) for x in current_path['dirs']]
                    
                    ## Reconstruct html_paths
                    current_path['dirs'] = [utils.from_path_to_html(x,path_map,request.path,base) for x in current_path['dirs']]
                    current_path['files'] = [utils.from_path_to_html(x,path_map,request.path,base) for x in current_path['files']]
                    
                    return jsonify(current_path)
                
                elif os.path.isfile(to_browse):
                    current_path['isFile'] = True
                    return jsonify(current_path)
                
                else:
                    # If a non-file / dir is passed, move backward to the nearest file/dir
                    return redirect(current_path['parent_path'])
                
                    # return render_template('vfs_bil.html', path=path, files=files)
            
            except Exception:
                flash('You must not be authorized to browse to path {}'.format(request.path))
                print(traceback.format_exc())
                return redirect(url_for('login'))
            
            
    
    return app
    
    
