# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 16:39:55 2022

@author: awatson
"""
# bil_api imports
from itertools import product
import io
import json
import re
from neuroglancer_scripts.chunk_encoding import RawChunkEncoder
import numpy as np
import os

## Project imports
from dataset_info import dataset_info
import utils

from flask import (
    render_template,
    request,
    send_file,
    redirect
    )

from flask_login import login_required
from flask_cors import cross_origin


def encode_ng_file(numpy_array,channels):
    encoder = RawChunkEncoder(numpy_array.dtype, channels)
    img_ram = io.BytesIO()
    img_ram.write(encoder.encode(numpy_array))
    img_ram.seek(0)
    return img_ram

def ng_shader(numpy_like_object):
    
    # channelMins = []
    # for ii in range
    
    metadata = utils.metaDataExtraction(numpy_like_object,strKey=False)
    res = numpy_like_object.ResolutionLevels
    
    shaderStr = ''
    shaderStr = shaderStr + '// Init for each channel:\n\n'
    shaderStr = shaderStr + '// Channel visability check boxes\n'
    
    for ii in range(metadata['Channels']):
        shaderStr = shaderStr + '#uicontrol bool channel{}_visable checkbox(default=true);\n'.format(ii)
    
    shaderStr = shaderStr + '\n// Lookup tables\n'
    for ii in range(metadata['Channels']):
        shaderStr = shaderStr + '#uicontrol invlerp lut_{} (range=[{},{}],window=[{},{}]'.format(
            ii,
            metadata[(res-1,0,ii,'HistogramMin')],
            metadata[(res-1,0,ii,'HistogramMax')],
            metadata[(res-1,0,ii,'HistogramMin')] - metadata[(res-1,0,ii,'HistogramMin')]//2,
            min( metadata[(res-1,0,ii,'HistogramMax')] + metadata[(res-1,0,ii,'HistogramMax')]//2, 65535 ), #<-- ToDo: code max based on dtype
            )
        if metadata['Channels'] > 1:
            shaderStr = shaderStr + ',channel=[{}]);\n'.format(ii)
        else:
            shaderStr = shaderStr + ');\n'
    
    shaderStr = shaderStr + '\n// Colors\n'
    
    defaultColors = ['green','red','purple','blue','yellow','orange'] * 10
    for ii in range(metadata['Channels']):
        shaderStr = shaderStr + '#uicontrol vec3 channel{}_color color(default="{}");\n'.format(ii, defaultColors[ii])
    
    shaderStr = shaderStr + '\n//RGB vector at 0 (ie channel off)\n'
    
    for ii in range(metadata['Channels']):
        shaderStr = shaderStr + 'vec3 channel{} = vec3(0);\n'.format(ii)
    
    shaderStr = shaderStr + '\n\nvoid main() {\n\n'
    shaderStr = shaderStr + '// For each color, if visable, get data, adjust with lut, then apply to color\n'
    
    for ii in range(metadata['Channels']):
        shaderStr = shaderStr + 'if (channel{}_visable == true)\n'.format(ii)
        shaderStr = shaderStr + 'channel{} = channel{}_color * ((toNormalized(getDataValue({})) + lut_{}()));\n\n'.format(ii,ii,ii,ii)
    
    shaderStr = shaderStr + '// Add RGB values of all channels\n'
    shaderStr = shaderStr + 'vec3 rgb = ('
    for ii in range(metadata['Channels']):
        shaderStr = shaderStr + 'channel{}'.format(ii)
        if ii < metadata['Channels']-1:
            shaderStr = shaderStr + ' + '
    shaderStr = shaderStr + ');\n\n'
    
    shaderStr = shaderStr + '//Retain RGB value with max of 1\n'
    shaderStr = shaderStr + 'vec3 render = min(rgb,vec3(1));\n\n'
    shaderStr = shaderStr + '// Render the resulting pixel map\n'
    shaderStr = shaderStr + 'emitRGB(render);\n'
    shaderStr = shaderStr + '}'
    
    return shaderStr
    
    
# // Init for each channel:

# // Channel visability check boxes
# #uicontrol bool channel0_visable checkbox(default=true)
# #uicontrol bool channel1_visable checkbox(default=true)

# // Lookup tables
# #uicontrol invlerp lut_0 (range=[0,10000],window=[0,20000],channel=[0]);
# #uicontrol invlerp lut_1 (range=[0,10000],window=[0,20000],channel=[1]);

# // Colors
# #uicontrol vec3 channel0_color color(default="green");
# #uicontrol vec3 channel1_color color(default="red");

# //RGB vector at 0 (ie channel off)
# vec3 channel0 = vec3(0);
# vec3 channel1 = vec3(0);

# void main() {
  
#   // For each color, if visable, get data, adjust with lut, then apply to color
#   if (channel0_visable == true) 
#     channel0 = channel0_color * ((toNormalized(getDataValue(0)) + lut_0()));
  
#   if (channel1_visable == true) 
#     channel1 = channel1_color * ((toNormalized(getDataValue(1)) + lut_1()));
  
#   // Add RGB values of all channels
#   vec3 rgb = (channel0 + channel1);
  
#   //Retain RGB value with max of 1
#   vec3 render = min(rgb,vec3(1));
  
#   // Render the resulting pixel map
#   emitRGB(
#     render
#   );
# }

## Build neuroglancer json
def ng_json(numpy_like_object,file=None, different_chunks=False):
    '''
    Save a json from a 5d numpy like volume
    file = None saves to a BytesIO buffer
    file == str saves to that file name
    file == 'dict' ourputs a dictionary
    '''

    # Alternative chunking depth along axial plane    
    offDimSize = 1
    
    metadata = utils.metaDataExtraction(numpy_like_object,strKey=False)
    
    neuro_info = {}
    neuro_info['data_type'] = metadata['dtype']
    neuro_info['num_channels'] = metadata['Channels']
    
    scales = []
    current_scale = {}
    for res in range(metadata['ResolutionLevels']):
        print('Creating JSON')
        chunks = list(reversed(list(metadata[(res,0,0,'chunks')][-3:]))) #<-- [x,y,z] orientation
        print(chunks)
        if different_chunks == False:
            current_scale["chunk_sizes"] = [
                        list(chunks)
                ]
        else:
            current_scale["chunk_sizes"] = [
                        [chunks[0],chunks[1],offDimSize],
                        [chunks[0],offDimSize,chunks[1]],
                        [offDimSize,chunks[0],chunks[1]]
                ]
        
        current_scale["encoding"] = 'raw'
        current_scale["key"] = str(res)
        current_scale["resolution"] = [x*1000 for x in list(
            reversed(
                list(metadata[(res,0,0,'resolution')])
                )
            )
            ]
        current_scale["size"] = list(
            reversed(
                list(metadata[(res,0,0,'shape')][-3:])
                )
            )
        current_scale["voxel_offset"] = [0, 0, 0]
        
        scales.append(current_scale)
        current_scale = {}
        
    
    neuro_info['scales'] = scales
    neuro_info['type'] = 'image'
    neuro_info['shader'] = ng_shader(numpy_like_object)
    
    
    print(neuro_info)
    if file is None:
        b = io.BytesIO()
        b.write(json.dumps(neuro_info).encode())
        b.seek(0)
        return b
    elif file == 'str':
        return json.dumps(neuro_info)
    elif file == 'dict':
        return neuro_info
    else:
        with open(file,'w') as f:
            f.write(json.dumps(neuro_info))
        return




def ng_files(numpy_like_object):
    '''
    Takes numpy_like_object representing a supported filetype
    and produces a dict where keys are int == resolution level and objects are 
    compreshensive lists of filenames representing each chunk of structure:
        xstart-xstop_ystart-ystop_zstart-zstop
    '''
    
    metadata = utils.metaDataExtraction(numpy_like_object,strKey=False)
    
    name_template = '{}-{}_{}-{}_{}-{}'
    fileLists = {}
    ## Make file list
    for res in range(metadata['ResolutionLevels']):
        chunks = metadata[(res,0,0,'chunks')]
        shape = metadata[(res,0,0,'shape')]
        
        fileLists[res] = []
        for x,y,z in product(
                range(0,shape[-1],chunks[-1]), #X-axis
                range(0,shape[-2],chunks[-2]), #Y-axis
                range(0,shape[-3],chunks[-3])  #Z-axis
                ):
            
            
            currentName = name_template.format(
                x,
                x + chunks[-1] if x + chunks[-1] <= shape[-1] else shape[-1],
                y,
                y + chunks[-2] if y + chunks[-2] <= shape[-2] else shape[-2],
                z,
                z + chunks[-3] if z + chunks[-3] <= shape[-3] else shape[-3],
                
                )
            fileLists[res].append(currentName)
            print(currentName)
    return fileLists






def make_ng_link(open_dataset_with_ng_json, compatible_file_link, ngURL='https://neuroglancer-demo.appspot.com/'):
    '''
    Attempts to build a fully working link to ng dataset
    '''
    stateDict = {}
    stateDict['dimensions'] = {'x': [ open_dataset_with_ng_json.ng_json['scales'][0]['resolution'][0]/1000,'um' ],
                               'y': [ open_dataset_with_ng_json.ng_json['scales'][0]['resolution'][1]/1000,'um' ],
                               'z': [ open_dataset_with_ng_json.ng_json['scales'][0]['resolution'][2]/1000,'um' ]
                               }
    stateDict['position'] = [ open_dataset_with_ng_json.ng_json['scales'][0]['size'][0]//2,
                             open_dataset_with_ng_json.ng_json['scales'][0]['size'][1]//2,
                             open_dataset_with_ng_json.ng_json['scales'][0]['size'][2]//2
                             ]
    
    stateDict['crossSectionScale'] = 50
    stateDict['projectionScale'] = 50 * stateDict['dimensions']['z'][0]
    
    stateDict['layers'] = []
    
    layer = {}
    layer['type'] = 'image'
    layer['source'] = 'precomputed://' + 'https://brain-api.cbi.pitt.edu' + compatible_file_link #<-- Needs to be imported intellegently
    # layer['tab'] = 'rendering'
    layer['shader'] = ng_shader(open_dataset_with_ng_json) # Includes controls and defaults
    # layer['shaderControls'] = {'normalized': {'range': [0, 9814], 'channel': [0]}} #<-- include an intellegent way to adjust shader
    # layer['channelDimensions'] = {'c^': [1, '']}
    layer['name'] = os.path.split(compatible_file_link)[-1]
    # layer['selectedLayer'] = {'visible': True, 'layer': layer['name']}
    layer['layout'] = '4panel'
    
    stateDict['layers'].append(layer)
    
    ## If source URL is not secure, use the non-secure version of neuroglancer
    if 'https://' in stateDict['layers'][0]['source'] == False:
        ngURL = ngURL.replace('https://','http://')
    
    outURL = ngURL + r'#!'
    outURL = outURL + str(stateDict)
    # outURL = outURL.replace(',','%2C')
    # outURL = outURL.replace('\\','')
    outURL = outURL.replace('True','true')
    outURL = outURL.replace('False','false')
    print(outURL)
    
    return outURL

    


def neuroglancer_dtypes():
    return [
        '.ims',
        '.zarr',
        '.weave'
        ]

def open_ng_dataset(config,datapath):
    
    datapath = config.loadDataset(datapath)
    
    if not hasattr(config.opendata[datapath],'ng_json'):
        # or not hasattr(config.opendata[datapath],'ng_files'):
            
            ## Forms a comrehensive file list for all chunks
            ## Not necessary for neuroglancer to function and take a long time
            # config.opendata[datapath].ng_files = \
            #     neuroGlancer.ng_files(config.opendata[datapath])
            
            ## Temp ignoring of ng_files
            ## Add attribute so this constantly repeated
            # config.opendata[datapath].ng_files = True
            
            config.opendata[datapath].ng_json = \
                ng_json(config.opendata[datapath],file='dict')
    
    return datapath
    

#######################################################################################
##  Neuroglancer entry point : decorated separately below to enable caching and flask entry
#######################################################################################

def setup_neuroglancer(app, config):
    
    def neuro_glancer_entry(req_path):
        # return str(request.url.split('/')[-2])
        # print(request.path)
        # print(request.base_url)
        # print(request.url)
        # if not utils.is_file_type(neuroglancer_dtypes(), request.path):
        #     print('Im here')
        #     return('Neuroglancer can not display this type of dataset')
        
        # settings = utils.get_config('settings.ini') #<-- need to add this to config class so each chunk access doesn't require a read of settings file
        settings = config.settings
        path_map = utils.get_path_map(settings,user_authenticated=True) #<-- Force user_auth=True to get all possible paths, in this way all ng links will be shareable to anyone
        datapath = utils.from_html_to_path(request.path, path_map)
        
        path_split = utils.split_html(request.path)
        
        # Test for different patterns
        file_name_template = '{}-{}_{}-{}_{}-{}'
        file_pattern = file_name_template.format('[0-9]+','[0-9]+','[0-9]+','[0-9]+','[0-9]+','[0-9]+')
        ## NEED to figure out how to extract the datapath from any version of ng request:
            # /hdshjk/file.ims : /hdshjk/file.ims/info : /hdshjk/file.ims/info/0/0-1_2-3_4-5
        
        # Find the file system path to the dataset
        # Assumptions are neuroglancer only requests 'info' file or chunkfiles
        # If only the file name is requested this will redirect to a 
        if isinstance(re.match(file_pattern,path_split[-1]),re.Match):
            datapath = '/' + os.path.join(*datapath.split('/')[:-2])
        elif path_split[-1] == 'info':
            datapath = '/' + os.path.join(*datapath.split('/')[:-1])
        elif utils.is_file_type(neuroglancer_dtypes(), datapath):
            datapath = open_ng_dataset(config,datapath) # Ensures that dataset is open AND info_json is formed
            link_to_ng = make_ng_link(config.opendata[datapath], request.path, ngURL='https://neuroglancer-demo.appspot.com/')
            return redirect(link_to_ng) # Redirect browser to fully formed neuroglancer link
        else:
            return 'No path to neuroglancer supported dataset'
        
        datapath = open_ng_dataset(config,datapath) # Ensures that dataset is open AND info_json is formed
        
        
        # Return 'info' json
        if path_split[-1] == 'info':
            # return 'in'
            b = io.BytesIO()
            b.write(json.dumps(config.opendata[datapath].ng_json, indent=2, sort_keys=False).encode())
            b.seek(0)
            
            return send_file(
                b,
                as_attachment=False,
                download_name='info',
                mimetype='application/json'
            )
        
        ## Serve neuroglancer raw-format files
        elif isinstance(re.match(file_pattern,path_split[-1]),re.Match):
            
            print(request.path + '\n')
            
            x,y,z = path_split[-1].split('_')
            x = x.split('-')
            y = y.split('-')
            z = z.split('-')
            x = [int(x) for x in x]
            y = [int(x) for x in y]
            z = [int(x) for x in z]
            
            img = config.opendata[datapath][
                int(path_split[-2]),
                slice(0),
                slice(None),
                slice(z[0],z[1]),
                slice(y[0],y[1]),
                slice(x[0],x[1])
                ]
            
            while img.ndim > 4:
                img = np.squeeze(img,axis=0)
                
            print(img.shape)
            
            img = encode_ng_file(img, config.opendata[datapath].ng_json['num_channels'])
            
            # Flask return of bytesIO as file
            return send_file(
                img,
                as_attachment=True,
                download_name=path_split[-1], # name needs to match chunk
                mimetype='application/octet-stream'
            )
        
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
        
        
            
            
        return 'Path not accessable'
    
    ##############################################################################
    
    ngPath = '/ng/' #<--- final slash is required for proper navigation through dir tree
    
    ## Decorating neuro_glancer_entry to allow caching ##
    if config.cache is not None:
        print('Caching setup')
        neuro_glancer_entry = config.cache.memoize()(neuro_glancer_entry)
        print(neuro_glancer_entry)
    # neuro_glancer_entry = login_required(neuro_glancer_entry)
    
    
   
    neuro_glancer_entry = cross_origin(allow_headers=['Content-Type'])(neuro_glancer_entry)
    # neuro_glancer_entry = login_required(neuro_glancer_entry)
    neuro_glancer_entry = app.route(ngPath + '<path:req_path>')(neuro_glancer_entry)
    neuro_glancer_entry = app.route(ngPath, defaults={'req_path': ''})(neuro_glancer_entry)
    
    
    
    
    return app

##############################################################################
## END NEUROGLANCER
##############################################################################


##############################################################################
##  Notes and examples below
##############################################################################

# metadata = {
#  'shape': (1, 2, 3, 27670, 19441),
#  'chunks': (1, 1, 4, 256, 256),
#  'dtype': 'uint16',
#  'ndim': 5,
#  'ResolutionLevels': 7,
#  'TimePoints': 1,
#  'Channels': 2,
#  (0, 0, 0, 'shape'): (1, 1, 3, 27670, 19441),
#  (0, 0, 0, 'resolution'): (10.0, 0.498, 0.498),
#  (0, 0, 0, 'HistogramMax'): 64623,
#  (0, 0, 0, 'HistogramMin'): 0,
#  (0, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (0, 0, 0, 'shapeH5Array'): (4, 27904, 19456),
#  (0, 0, 0, 'dtype'): 'uint16',
#  (0, 0, 1, 'shape'): (1, 1, 3, 27670, 19441),
#  (0, 0, 1, 'resolution'): (10.0, 0.498, 0.498),
#  (0, 0, 1, 'HistogramMax'): 65535,
#  (0, 0, 1, 'HistogramMin'): 0,
#  (0, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (0, 0, 1, 'shapeH5Array'): (4, 27904, 19456),
#  (0, 0, 1, 'dtype'): 'uint16',
#  (1, 0, 0, 'shape'): (1, 1, 3, 13835, 9720),
#  (1, 0, 0, 'resolution'): (10.0, 0.996, 0.996),
#  (1, 0, 0, 'HistogramMax'): 41136,
#  (1, 0, 0, 'HistogramMin'): 0,
#  (1, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (1, 0, 0, 'shapeH5Array'): (4, 14080, 9728),
#  (1, 0, 0, 'dtype'): 'uint16',
#  (1, 0, 1, 'shape'): (1, 1, 3, 13835, 9720),
#  (1, 0, 1, 'resolution'): (10.0, 0.996, 0.996),
#  (1, 0, 1, 'HistogramMax'): 65535,
#  (1, 0, 1, 'HistogramMin'): 0,
#  (1, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (1, 0, 1, 'shapeH5Array'): (4, 14080, 9728),
#  (1, 0, 1, 'dtype'): 'uint16',
#  (2, 0, 0, 'shape'): (1, 1, 3, 6917, 4860),
#  (2, 0, 0, 'resolution'): (10.0, 1.992, 1.992),
#  (2, 0, 0, 'HistogramMax'): 28705,
#  (2, 0, 0, 'HistogramMin'): 0,
#  (2, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (2, 0, 0, 'shapeH5Array'): (4, 7168, 4864),
#  (2, 0, 0, 'dtype'): 'uint16',
#  (2, 0, 1, 'shape'): (1, 1, 3, 6917, 4860),
#  (2, 0, 1, 'resolution'): (10.0, 1.992, 1.992),
#  (2, 0, 1, 'HistogramMax'): 65535,
#  (2, 0, 1, 'HistogramMin'): 0,
#  (2, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (2, 0, 1, 'shapeH5Array'): (4, 7168, 4864),
#  (2, 0, 1, 'dtype'): 'uint16',
#  (3, 0, 0, 'shape'): (1, 1, 3, 3458, 2430),
#  (3, 0, 0, 'resolution'): (10.0, 3.985, 3.984),
#  (3, 0, 0, 'HistogramMax'): 21381,
#  (3, 0, 0, 'HistogramMin'): 0,
#  (3, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (3, 0, 0, 'shapeH5Array'): (4, 3584, 2560),
#  (3, 0, 0, 'dtype'): 'uint16',
#  (3, 0, 1, 'shape'): (1, 1, 3, 3458, 2430),
#  (3, 0, 1, 'resolution'): (10.0, 3.985, 3.984),
#  (3, 0, 1, 'HistogramMax'): 65535,
#  (3, 0, 1, 'HistogramMin'): 0,
#  (3, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (3, 0, 1, 'shapeH5Array'): (4, 3584, 2560),
#  (3, 0, 1, 'dtype'): 'uint16',
#  (4, 0, 0, 'shape'): (1, 1, 3, 1729, 1215),
#  (4, 0, 0, 'resolution'): (10.0, 7.97, 7.968),
#  (4, 0, 0, 'HistogramMax'): 16536,
#  (4, 0, 0, 'HistogramMin'): 0,
#  (4, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (4, 0, 0, 'shapeH5Array'): (4, 1792, 1280),
#  (4, 0, 0, 'dtype'): 'uint16',
#  (4, 0, 1, 'shape'): (1, 1, 3, 1729, 1215),
#  (4, 0, 1, 'resolution'): (10.0, 7.97, 7.968),
#  (4, 0, 1, 'HistogramMax'): 65531,
#  (4, 0, 1, 'HistogramMin'): 0,
#  (4, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (4, 0, 1, 'shapeH5Array'): (4, 1792, 1280),
#  (4, 0, 1, 'dtype'): 'uint16',
#  (5, 0, 0, 'shape'): (1, 1, 3, 864, 607),
#  (5, 0, 0, 'resolution'): (10.0, 15.949, 15.95),
#  (5, 0, 0, 'HistogramMax'): 12182,
#  (5, 0, 0, 'HistogramMin'): 0,
#  (5, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (5, 0, 0, 'shapeH5Array'): (4, 1024, 768),
#  (5, 0, 0, 'dtype'): 'uint16',
#  (5, 0, 1, 'shape'): (1, 1, 3, 864, 607),
#  (5, 0, 1, 'resolution'): (10.0, 15.949, 15.95),
#  (5, 0, 1, 'HistogramMax'): 50946,
#  (5, 0, 1, 'HistogramMin'): 0,
#  (5, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (5, 0, 1, 'shapeH5Array'): (4, 1024, 768),
#  (5, 0, 1, 'dtype'): 'uint16',
#  (6, 0, 0, 'shape'): (1, 1, 3, 432, 303),
#  (6, 0, 0, 'resolution'): (10.0, 31.897, 31.953),
#  (6, 0, 0, 'HistogramMax'): 8203,
#  (6, 0, 0, 'HistogramMin'): 0,
#  (6, 0, 0, 'chunks'): (1, 1, 4, 256, 256),
#  (6, 0, 0, 'shapeH5Array'): (4, 512, 512),
#  (6, 0, 0, 'dtype'): 'uint16',
#  (6, 0, 1, 'shape'): (1, 1, 3, 432, 303),
#  (6, 0, 1, 'resolution'): (10.0, 31.897, 31.953),
#  (6, 0, 1, 'HistogramMax'): 41553,
#  (6, 0, 1, 'HistogramMin'): 0,
#  (6, 0, 1, 'chunks'): (1, 1, 4, 256, 256),
#  (6, 0, 1, 'shapeH5Array'): (4, 512, 512),
#  (6, 0, 1, 'dtype'): 'uint16'
#  }


# # Neuroglancer metadata file example

# example_json_neuro_info = {
#   "data_type": "uint8",
#   "num_channels": 1,
#   "scales": [{"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "8_8_8",
#     "resolution": [8, 8, 8],
#     "size": [6446, 6643, 8090],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "16_16_16",
#     "resolution": [16, 16, 16],
#     "size": [3223, 3321, 4045],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "32_32_32",
#     "resolution": [32, 32, 32],
#     "size": [1611, 1660, 2022],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "64_64_64",
#     "resolution": [64, 64, 64],
#     "size": [805, 830, 1011],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "128_128_128",
#     "resolution": [128, 128, 128],
#     "size": [402, 415, 505],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "256_256_256",
#     "resolution": [256, 256, 256],
#     "size": [201, 207, 252],
#     "voxel_offset": [0, 0, 0]},
#   {"chunk_sizes": [[64, 64, 64]],
#     "encoding": "jpeg",
#     "key": "512_512_512",
#     "resolution": [512, 512, 512],
#     "size": [100, 103, 126],
#     "voxel_offset": [0, 0, 0]}],
#   "type": "image"}


'''
File name convention by chunk = [x,y,z] <-- note: opposite from numpy (z,y,x)
chunks == [10,15,2]
size == [18,35,1]

Files:
    0-10_0-15_0-1
    0-10_15-30_0-1
    0-10_30-35_0-1
    10-18_0-15_0-1
    10-18_15-30_0-1
    10-18_30-35_0-1
    
'''

## n-tracer info

# {'data_type': 'uint16',
#  'num_channels': 1,
#  'type': 'image',
#  'scales': [{'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '1',
#    'resolution': [350, 350, 1000],
#    'size': [32768, 20480, 13312],
#    'voxel_offset': [0, 0, 0]},
#   {'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '2',
#    'resolution': [700, 700, 2000],
#    'size': [16384, 10240, 6656],
#    'voxel_offset': [0, 0, 0]},
#   {'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '4',
#    'resolution': [1400, 1400, 4000],
#    'size': [8192, 5120, 3328],
#    'voxel_offset': [0, 0, 0]},
#   {'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '8',
#    'resolution': [2800, 2800, 8000],
#    'size': [4096, 2560, 1664],
#    'voxel_offset': [0, 0, 0]},
#   {'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '16',
#    'resolution': [5600, 5600, 16000],
#    'size': [2048, 1280, 832],
#    'voxel_offset': [0, 0, 0]},
#   {'chunk_sizes': [[128, 128, 1], [128, 1, 128], [1, 128, 128]],
#    'encoding': 'raw',
#    'key': '32',
#    'resolution': [11200, 11200, 32000],
#    'size': [1024, 640, 416],
#    'voxel_offset': [0, 0, 0]}]}

####  ng_docs
'''
https://github.com/google/neuroglancer/blob/master/src/neuroglancer/sliceview/README.md

Neuroglancer also supports multiple (anisotropic) chunk sizes to be used 
simultaneously with a single volume, in which case each SliceView selects 
the chunk size (at each resolution) that is most efficient. For example, 
to support XY, XZ, and YZ cross-sectional views, chunk sizes of 
(512, 512, 1), (512, 1, 512) and (1, 512, 512) could be used. This does have 
the disadvantage, however, that chunk data is not shared at all by the 3 views
'''


# # ## Browser state example 'Hook's Brain:

# a = '{"dimensions":{"x":[4.98e-7%2C"m"]%2C"y":[4.98e-7%2C"m"]%2C"z":[0.00000533%2C"m"]}%2C"position":[9396.5%2C13847.5%2C562.5]%2C"crossSectionScale":54.598150033144236%2C"projectionScale":32000%2C"layers":[{"type":"image"%2C"source":"precomputed://https://brain-api.cbi.pitt.edu/api/ng/3"%2C"tab":"rendering"%2C"shaderControls":{"normalized":{"range":[0%2C9814]%2C"channel":[1]}}%2C"channelDimensions":{"c^":[1%2C""]}%2C"name":"3"}]%2C"selectedLayer":{"visible":true%2C"layer":"3"}%2C"layout":"4panel"}'

# # b = a.replace(r'https://neuroglancer-demo.appspot.com/#!','')
# # b = b.replace(r'http://neuroglancer-demo.appspot.com/#!','')
# b = a.replace('%2C',',')
# b = b.replace('true','True')
# b = b.replace('false','False')
# b = eval(b)












