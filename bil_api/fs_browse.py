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

import utils


def initiate_browseable(app):
    from flaskAPI_gunicorn import login_manager
    
    base = '/browser/'
    @app.route(base + '<path:req_path>')
    @app.route(base, defaults={'req_path': ''})
    def browse_fs(req_path):

        print(request.path)
        
        # Split the requested path to a tuple that can be reused below
        html_path_split = utils.split_html(request.path)
        print(html_path_split)
        
        # Extract settings information that can be reused
        # Doing this here allows changes to paths to be dynamic (ie changes can be made while server is live)
        # May want to change this so that each browser access does not require access to settings file on disk.
        ## DEFAULT ## utils.get_config(file='settings.ini',allow_no_value=True)
        settings =  utils.get_config()
        
        if len(html_path_split) == 1:
            
            to_browse = utils.get_base_paths(settings,current_user.is_authenticated)
            
            to_browse = sorted(to_browse)
            path = [base[:-1] + os.path.split(x)[0] for x in to_browse] if len(to_browse) > 0 else ['/']
            files = [os.path.split(x)[1] for x in to_browse]
            
            return render_template('vfs_bil.html', path=path, files=files)
        
        else:
            
            try:
                print('Line 88')
                # Determine what directories that users are allowed to browse 
                # based on authentication status and boot them if the path is not valid
                path_map =  utils.get_path_map(settings,current_user.is_authenticated)
                
                if html_path_split[1] not in path_map:
                    flash('You are not authorized to browse to path {}'.format(request.path))
                    return redirect(url_for('login'))
                
                
                if current_user.is_authenticated:
                    
                    # Read in group information
                    groups = utils.get_config('groups.ini',allow_no_value=True)
                    
                    # Build a list of allowed folders
                    allowed_list = [current_user.id.lower()]
                    for ii in groups: # Group names
                        for oo in groups[ii]: # Users in each group
                            if current_user.id.lower() == oo.lower(): # Current user matches the user in the group
                                print('Line 168')
                                allowed_list.append(ii.lower())
                    
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
                print('Live 98')
                print(to_browse)
            
                
                # Get current directory listing by Files, Directories and stats on each
                current_path = {}
                if os.path.isdir(to_browse):
                    current_path['isfile'] = False
                    for root, dirs, files in os.walk(to_browse):
                        current_path['root'] = root
                        current_path['dirs'] = dirs
                        current_path['files'] = files
                        break
                    current_path['dirs'] = [os.path.join(root,x) for x in current_path['dirs']]
                    current_path['files'] = [os.path.join(root,x) for x in current_path['files']]
                    
                    current_path['dirs_stat'] = [os.stat(x) for x in current_path['dirs']]
                    current_path['files_stat'] = [os.stat(x) for x in current_path['files']]
                    
                    current_path['dirs_num_entries'] = [len(os.listdir(x)) for x in current_path['dirs']]
                    
                    ## Reconstruct html_paths
                    current_path['dirs'] = [utils.from_path_to_html(x,path_map,request.path,base) for x in current_path['dirs']]
                    current_path['files'] = [utils.from_path_to_html(x,path_map,request.path,base) for x in current_path['files']]
                elif os.path.isfile(to_browse):
                    current_path['isfile'] = True
                
                print(current_path)
                
                
                # Find all available files in the resquested path
                to_browse = glob.glob(to_browse + '/*')
                
                
                ## We now have a list (to_browse) of all files in the requested path
                # Now we need to extract only paths that the user is allowed to see
                
                # If the user is anonymous, then there is no additional filtering to do
                if not current_user.is_authenticated:
                    print('Line 128')
                    pass
                
                elif current_user.is_authenticated:
                    print('Line 132')
                    
                    if current_user.id.lower() in [x.lower() for x in groups['all']]:
                        print('Line 139')
                        pass
                    
                    else:
                        print('Line 143')
                        
                        # All paths +1 above those listed in 'setting.ini' needs to be filtered for 
                        # username_or_group affiliation.  (ie base/path_in_settings.ini/username_or_group/anything_else/doesnt_matter)
                        if settings.getboolean('auth', 'restrict_paths_to_matched_username'):
                            print('Line 148')
                            
                            ## to_view will collect all OK paths for viewing
                            to_view = []
                            
                            # Retain all anon paths
                            for ii in settings['dir_anon']:
                                tmp = [x for x in to_browse if ii.lower() == utils.split_html(x)[1].lower()]
                                to_view = to_view + tmp
                            # print(to_view)
                            # Keep only those paths from to_browse which contain 'username' at html_path[2]<-- may need to change this to filter at level html_path[2]
                            # print(current_user.id.lower())
                            # print(to_browse[0:10])
                            # to_view = to_view + [x for x in to_browse if current_user.id.lower() == split_html(x)[2].lower()]
                            # print(to_view)
                            
                            
                            
                            
                            to_view = to_view + [x for x in to_browse if utils.split_html(x)[2].lower() in allowed_list] # Retain group folder names '/group_name/'
                                    
                            # Reset to_browse to be only paths which are included
                            to_browse = to_view
                            print(to_browse)
                        
                        
                        ## Limit files that are seen to those in settings.ini 'file_types'
                        if settings.getboolean('auth','restrict_files_to_listed_file_types'):
                            print(178)
                            print(to_browse)
                            print(path_map)
                            ## to_view will collect all OK paths for viewing
                            to_view = []
                            
                            # paths = [ utils.from_html_to_path(x, path_map) for x in to_browse ]
                            paths = to_browse
                            print(184)
                            print(paths[0:10])
                            
                            # First find all directories (these will not be filtered out)
                            dirs = [ x for x,y in zip(to_browse,paths) if os.path.isdir(y) ]
                            # dirs = [ x for x in to_browse if os.path.isdir(from_html_to_path(x, path_map)) ]
                            print(dirs)
                            # Now find all files that match an allowed file type
                            # First get a list of all files
                            files = [ x for x,y in zip(to_browse,paths) if os.path.isfile(y) ]
                            # files = [ x for x in to_browse if os.path.isfile(from_html_to_path(x, path_map)) ]
                            for ii in settings['file_types']:
                                # Further limit to the file type being filtered
                                current_files = [ x for x in files if utils.is_file_type(settings['file_types'][ii], utils.split_html(x)[-1]) ]#<-- add 'a' to make split consistent when files start with '.'
                                to_view = to_view + current_files
                            to_view = dirs + to_view
                            
                            # Reset to_browse to be only paths which are included
                            to_browse = to_view
                    
                
                # print(to_browse)
                # # Format paths to align with the browser representation and thus disguise real paths
                # to_browse = [x.replace(path_map[html_path[1]],'/' + html_path[1]) for x in to_browse]
                
                print(to_browse)
                
                # Sort for easy browsing
                to_browse = sorted(to_browse)
                
                # Get path / file info to pass to html template
                path = [base[:-1] + os.path.split(x)[0] for x in to_browse] if len(to_browse) > 0 else ['/']
                files = [os.path.split(x)[1] for x in to_browse] #if len(to_browse) > 0 else ['']
            
                return render_template('vfs_bil.html', path=path, files=files)
            
            except Exception:
                flash('You must not be authorized to browse to path {}'.format(request.path))
                print(traceback.format_exc())
                return redirect(url_for('login'))
            
            
    
    return app
    
    
    
        # # Not necessary with config.opendata[datapath].ng_files not being built
        # # Build appropriate File List in base path
        # if len(url_path_split) == 1:
        #     res_files = list(config.opendata[datapath].ng_files.keys())
        #     # return str(res_files)
        #     files = ['info', *res_files]
        #     files = [str(x) for x in files]
        #     path = [request.script_root]
        #     return render_template('vfs_bil.html', path=path, files=files)
        
        # # Not necessary with config.opendata[datapath].ng_files not being built
        # # Build html to display all ng_files chunks
        # if len(url_path_split) == 2 and isinstance(re.match('[0-9]+',url_path_split[-1]),re.Match):
        #     res = int(url_path_split[-1])
        #     files = config.opendata[datapath].ng_files[res]
        #     path = [request.script_root]
        #     return render_template('vfs_bil.html', path=path, files=files)
    
    
    
    
    
    
    
