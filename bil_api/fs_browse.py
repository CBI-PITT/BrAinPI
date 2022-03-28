# -*- coding: utf-8 -*-
"""
Created on Mon Mar 28 11:47:11 2022

@author: awatson
"""

'''
Make a browseable filesystem that limits paths to those configured in 
settings.ini and according to authentication / groups.ini
'''

from flask_login import (
                         current_user,
                         login_required,
                         logout_user
                         )

from flask import render_template,request, flash, url_for, redirect
import glob, os

def initiate_browseable(app):
    from flaskAPI_gunicorn import login_manager
    from utils import get_config
    
    base = '/browser/'
    @app.route(base + '<path:req_path>')
    @app.route(base, defaults={'req_path': ''})
    def browse_fs(req_path):

        print(request.path)
        
        html_path = request.path.split('/')
        html_path = [x for x in html_path if x != '' ]
        
        print(html_path)
        
        
        settings = get_config() #<-- doing this here allows changes to paths to be dynamic
        if len(html_path) == 1:
            
            ## Grab anon paths from settings file
            paths_anon = []
            for ii in settings['dir_anon']:
                paths_anon.append(ii)
           
            ## Grab auth paths from settings file
            paths_auth = []
            for ii in settings['dir_auth']:
                paths_auth.append(ii)
            
            to_browse = paths_anon
            if current_user.is_authenticated:
                to_browse =  paths_auth + to_browse
            
            to_browse = sorted(to_browse)
            path = [base[:-1] + os.path.split(x)[0] for x in to_browse] if len(to_browse) > 0 else ['/']
            files = [os.path.split(x)[1] for x in to_browse]
            
            return render_template('vfs_bil.html', path=path, files=files)
        
        else:
            # if current_user.is_authenticated and current_user.id != html_path[2]:
            #     flash('You are not authorized to browse to path {}'.format(request.path))
            #     return redirect(url_for('login'))
            
            
            path_map = {}
            ## Collect anon paths
            for ii in settings['dir_anon']:
                path_map[ii] = settings['dir_anon'][ii]
            print(path_map)
            
            ## Collect auth paths
            if current_user.is_authenticated:
                for ii in settings['dir_auth']:
                    path_map[ii] = settings['dir_auth'][ii]
            print(path_map)
           
            # Construct real paths 
            to_browse = os.path.join(
                path_map[html_path[1]],
                *html_path[2:])
        
            # List all available files in the resquested path
            to_browse = glob.glob(to_browse + '/*')
            
            # Restrict extry to shared paths to folder named like the username
            

            
            # Designed to directly filter 1 level above paths designated in settins.ini
            if len(html_path) == 2 and \
                settings.getboolean('auth', 'restrict_paths_to_matched_username') and \
                    current_user.is_authenticated:
                
                groups = get_config('groups.ini',allow_no_value=True)
                
                if current_user.id.lower() not in [x.lower() for x in groups['all']]:
                    
                    to_view = []
                    
                    # Retain all anon paths
                    for ii in settings['dir_anon']:
                        tmp = [x for x in to_browse if settings['dir_anon'][ii] in x]
                        to_view = to_view + tmp
                    
                    # Keep only those paths with username
                    to_view = to_view + [x for x in to_browse if '/' + current_user.id.lower() in x.lower()]
                    print(to_view)
                    
                    # Keep paths named for the groups a user belongs to
                    for ii in groups: # Group names
                        for oo in groups[ii]: # Users in each group
                            if current_user.id.lower() == oo.lower(): # Current user matches the user in the group
                                to_view = to_view + [x for x in to_browse if '/' + ii.lower() in x.lower()] # Retain group folder names
                            
                    to_browse = to_view
            
            
            ## Limit files that are seen to those in settings.ini 'file_types'
            groups = get_config('groups.ini',allow_no_value=True)
            if current_user.id.lower() not in [x.lower() for x in groups['all']]:
                if settings.getboolean('auth','restrict_files_to_listed_file_types'):
                    to_view = []
                    dirs = [x for x in to_browse if os.path.isdir(x)]
                    for ii in settings['file_types']:
                        files = [ x for x in to_browse if (os.path.isfile(x) and settings['file_types'][ii] == os.path.splitext('a'+ x)[-1]) ]#<-- add 'a' to make split consistance for files starting with '.'
                        to_view = to_view + files
                    to_view = dirs + to_view
                    to_browse = to_view
            
            
            # Format path to align with the browser and disguise real paths
            to_browse = [x.replace(path_map[html_path[1]],'/' + html_path[1]) for x in to_browse]
            
            # Sort for easy browsing
            to_browse = sorted(to_browse)
            
            # Get path / file info to pass to html template
            path = [base[:-1] + os.path.split(x)[0] for x in to_browse] if len(to_browse) > 0 else ['/']
            files = [os.path.split(x)[1] for x in to_browse]
            
            return render_template('vfs_bil.html', path=path, files=files)
            
            
    
    
    
    
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
    
    
    
    
    
    
    
