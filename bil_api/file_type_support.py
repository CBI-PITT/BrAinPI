# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 15:49:10 2022

@author: awatson
"""

'''
Attempt at creating file type hooks to produce links for opening files in other
applications like neuroglancer or napari
'''

import utils
import neuroGlancer
from flask import url_for

def ng_links(req_path):
    '''
    If the file type is supported for neuroglancer
    return the ng file entrypoint
    '''
    
    file_types = neuroGlancer.neuroglancer_dtypes()
    
    file_type_supported = utils.is_file_type(file_types, req_path)
    
    if file_type_supported:
        new_path = req_path.replace(url_for('browse_fs'),url_for('neuro_glancer_entry'),1)
        return new_path
    
    else:
        return None


