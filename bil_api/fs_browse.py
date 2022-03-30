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

def split_html(req_path):
    html_path = req_path.split('/')
    return [x for x in html_path if x != '' ]

def from_html_to_path(req_path, path_map):
    html_path = split_html(req_path)
    return os.path.join(
        path_map[html_path[1]], # returns the true FS path
        *html_path[2:]) # returns a unpacked list of all subpaths from html_path[1]

def from_path_to_html(path, path_map, req_path):
    html_path = split_html(req_path)
    return path.replace(path_map[html_path[1]],'/' + html_path[1])

def is_file_type(file_type, path):
    '''
    file_type is file extension starting with '.'
    Examples: '.ims', '.tiff', '.nd2'
    
    return bool
    '''
    return file_type.lower() == os.path.splitext('a'+ path)[-1].lower()
    

def initiate_browseable(app):
    from flaskAPI_gunicorn import login_manager
    from utils import get_config
    
    base = '/browser/'
    @app.route(base + '<path:req_path>')
    @app.route(base, defaults={'req_path': ''})
    def browse_fs(req_path):

        print(request.path)
        
        html_path = split_html(request.path)
        
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
            
            try:
                print('Line 65')
                # Determine what directories that anon users are allowed to browse 
                # and kick them if the path is not valid
                path_map = {}
                ## Collect anon paths
                for ii in settings['dir_anon']:
                    path_map[ii] = settings['dir_anon'][ii]
                print(path_map)
                
                if not current_user.is_authenticated and html_path[1] not in path_map:
                    flash('Anonymous users are not authorized to browse to path {}'.format(request.path))
                    return redirect(url_for('login'))
                
                
                # Determine what paths authenticated users may browse
                # and kick them if the path is not valid
                if current_user.is_authenticated:
                    print('Line 82')
                    for ii in settings['dir_auth']:
                        path_map[ii] = settings['dir_auth'][ii]
                print(path_map)
                
                if html_path[1] not in path_map:
                    flash('You are not authorized to browse to path {}'.format(request.path))
                    return redirect(url_for('login'))
               
                # Construct real paths from names in path_map dict
                to_browse = from_html_to_path(request.path, path_map)
            
                # Find all available files in the resquested path
                to_browse = glob.glob(to_browse + '/*')
                
                ## Reconstruct html_paths
                to_browse = [from_path_to_html(x,path_map,request.path) for x in to_browse]
                
                ## We now have a list (to_browse) of all files in the requested path
                # Now we need to extract only paths that the user is allowed to see
                
                # If the user is anonymous, then there is no additional filtering to do
                if current_user.is_authenticated == False:
                    print('Line 107')
                    pass
                
                elif current_user.is_authenticated:
                    print('Line 111')
                    
                    # Grab groups from groups.ini file
                    # 'All' group is assumed to have NO restrctions, so all filters are bypassed for this group
                    groups = get_config('groups.ini',allow_no_value=True)
                    
                    if current_user.id.lower() in [x.lower() for x in groups['all']]:
                        print('Line 117')
                        pass
                    
                    else:
                        print('Line 122')
                        
                        # All paths +1 above those listed in 'setting.ini' needs to be filtered for 
                        # username_or_group affiliation.  (ie base/path_in_settings.ini/username_or_group/anything_else/doesnt_matter)
                        if settings.getboolean('auth', 'restrict_paths_to_matched_username'):
                            print('Line 125')
                            
                            ## to_view will collect all OK paths for viewing
                            to_view = []
                            
                            # Retain all anon paths
                            for ii in settings['dir_anon']:
                                tmp = [x for x in to_browse if settings['dir_anon'].lower() == split_html(x)[1].lower()]
                                to_view = to_view + tmp
                            
                            # Keep only those paths from to_browse which contain 'username' at html_path[2]<-- may need to change this to filter at level html_path[2]
                            print(current_user.id.lower())
                            print(to_browse[0:10])
                            to_view = to_view + [x for x in to_browse if current_user.id.lower() == split_html(x)[2].lower()]
                            print(to_view)
                            
                            # Keep only those paths from to_browse named for the groups a user belongs to at html_path[2]
                            for ii in groups: # Group names
                                for oo in groups[ii]: # Users in each group
                                    if current_user.id.lower() == oo.lower(): # Current user matches the user in the group
                                        print('Line 143')
                                        to_view = to_view + [x for x in to_browse if ii.lower() == split_html(x)[2].lower()] # Retain group folder names '/group_name/'
                                    
                            # Reset to_browse to be only paths which are included
                            to_browse = to_view
                        
                        
                        ## Limit files that are seen to those in settings.ini 'file_types'
                        if settings.getboolean('auth','restrict_files_to_listed_file_types'):
                            
                            ## to_view will collect all OK paths for viewing
                            to_view = []
                            
                            paths = [ x for x in to_browse if from_html_to_path(x, path_map) ]
                            
                            
                            # First find all directories (these will not be filtered out)
                            dirs = [ x for x,y in zip(to_browse,paths) if os.path.isdir(y) ]
                            # dirs = [ x for x in to_browse if os.path.isdir(from_html_to_path(x, path_map)) ]
                            
                            # Now find all files that match an allowed file type
                            # First get a list of all files
                            files = [ x for x,y in zip(to_browse,paths) if os.path.isfile(y) ]
                            # files = [ x for x in to_browse if os.path.isfile(from_html_to_path(x, path_map)) ]
                            for ii in settings['file_types']:
                                # Further limit to the file type being filtered
                                current_files = [ x for x in files if is_file_type(settings['file_types'][ii], split_html(x)[-1]) ]#<-- add 'a' to make split consistent when files start with '.'
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
    
    
    
    
    
    
    
