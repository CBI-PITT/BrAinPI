# -*- coding: utf-8 -*-
"""File-type predicates and browser-to-viewer URL transformations.

The helpers determine whether a browser path can be opened by Neuroglancer,
OpenSeadragon, or the virtual OME-Zarr endpoint and construct the corresponding
Flask route path.
"""

import utils
from flask import url_for
import neuroGlancer
import openSeadragon
def ng_links(req_path):
    """Convert a supported browser path to its Neuroglancer route.

    Args:
        req_path: Path beginning at the Flask filesystem-browser endpoint.

    Returns:
        str or None: Neuroglancer route, or ``None`` for an unsupported type.
    """
    
    file_types = neuroGlancer.neuroglancer_dtypes()
    
    file_type_supported = utils.is_file_type(file_types, req_path)
    
    if file_type_supported:
        # print('neuroglancer supported',req_path)
        new_path = req_path.replace(url_for('browse_fs'),url_for('neuro_glancer_entry'),1)
        # print('neuroglancer link', new_path)
        return new_path
    
    else:
        return None

def opsd_links(req_path):
    """Convert a supported browser path to its OpenSeadragon route.

    Args:
        req_path: Path beginning at the Flask filesystem-browser endpoint.

    Returns:
        str or None: OpenSeadragon route, or ``None`` when unsupported.
    """
    file_types = openSeadragon.openseadragon_dtypes()
    file_type_supported = utils.is_file_type(file_types, req_path)
    if file_type_supported:
        new_path = req_path.replace(url_for('browse_fs'),url_for('openseadragon_entry'),1)
        return new_path
    
    else:
        return None
    
def omezarr_links(req_path):
    """Convert a supported browser or NG path to virtual OME-Zarr.

    Args:
        req_path: Browser or Neuroglancer endpoint path.

    Returns:
        str or None: Virtual ``.ome.zarr`` route, or ``None`` when unsupported.

    Raises:
        ValueError: If a supported file path has no recognized endpoint prefix.
    """
    file_types = openSeadragon.openseadragon_dtypes() + neuroGlancer.neuroglancer_dtypes()
    file_type_supported = utils.is_file_type(file_types, req_path)
    if file_type_supported:
        # print('omezarr supported',req_path)
        if req_path.startswith(url_for('neuro_glancer_entry')):
            return req_path.replace(url_for('neuro_glancer_entry'),url_for('omezarr_entry'),1) + '.ome.zarr'
        elif req_path.startswith(url_for('browse_fs')):
            return req_path.replace(url_for('browse_fs'),url_for('omezarr_entry'),1) + '.ome.zarr'
        else:
            raise ValueError('omezarr_links req_path must contain either neuro_glancer_entry or browse_fs endpoint')

    else:
        return None


def downloadable(req_path,size=None, max_sizeGB=None):
    """
    Determine if the requested file can be downlaoded
    max_size imposes a limit on the size of the file that can
    be downloaded
    
    req_path = str to url or file
    size = bytes as int/float
    max_sizeGB = int/float in GB
    """
    if max_sizeGB is not None:
        if size/1000/1000/1000 > max_sizeGB:
            return None
    return req_path



def dir_as_file(req_path):
    """
    Some directories should be treated like files.  For instance,
    it is not helpful to open a zarr directory in the browser,
    but options to view in neuroglancer should be available
    
    Temporalilly this looks at neuroglancer support only
    """
    
    file_types = neuroGlancer.neuroglancer_dtypes()
    file_type_supported = utils.is_file_type(file_types, req_path)
    
    if file_type_supported:
        return req_path
    else:
        return None
    
    
    
