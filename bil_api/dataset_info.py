# -*- coding: utf-8 -*-
"""
Created on Thu Nov  4 09:50:55 2021

@author: alpha
"""

import os

def dataset_info():
    
    '''
    Returns a dictionary where keys are integers and data is a 
    tuple with the (short_name_of_dataset, dSetPath) 
    '''
    
    if os.name == 'nt':
        choices = {
            1:('fmost_zarr','h:/globus/pitt/bil/t00_c00_0.zarr'),
            2:('testIMS','z:/testData/bitplaneConverter.ims'),
            3:('HooksBrain','h:/Acquire/Hooks/BrainA.ims'),
            4:('fmost_weave', 'h:/globus/pitt/bil/weave'),
            5:('home_testIMS', 'c:/code/testData/bitplaneConverter.ims') # For home testing
                   }
        
    else:
        
        if 'c00' in os.uname()[1]:
            
            choices = {
                1:('fmost','/bil/proj/bil/zarr/zarr/t00_c00_0.zarr'),
                2:('testIMS','/bil/proj/bil/zarr/ims_test/bitplaneConverter.ims'),
                3:('andor1a','/bil/data/a4/0e/a40eac7d32a2df23/TME10-1_Lvl1_Stitched/30x/Camk2a-MORF3-D1Tom_TME10-1_30x_Str_01A.ims'),
                4:('andor1b','/bil/data/a4/0e/a40eac7d32a2df23/TME10-1_Lvl1_Stitched/30x/Camk2a-MORF3-D1Tom_TME10-1_30x_Str_01B.ims')
                       }
        else:
            
            choices = {
                1:('fmost','/CBI_Hive/globus/pitt/bil/t00_c00_0.zarr'),
                2:('testIMS','/CBI_FastStore/testData/bitplaneConverter.ims'),
                3:('HooksBrain','/CBI_Hive/Acquire/Hooks/BrainA.ims'),
                4:('fmost_weave', '/CBI_Hive/globus/pitt/bil/weave')
                       }
        
    
    return choices