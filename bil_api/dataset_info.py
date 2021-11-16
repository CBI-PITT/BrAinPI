# -*- coding: utf-8 -*-
"""
Created on Thu Nov  4 09:50:55 2021

@author: alpha
"""


def dataset_info():
    
    '''
    Returns a dictionary where keys are integers and data is a 
    tuple with the (short_name_of_dataset, dSetPath) 
    '''
    
    choices = {
        1:('fmost','h:/globus/pitt/bil/t00_c00_0.zarr'),
        2:('testIMS','z:/testData/bitplaneConverter.ims'),
        3:('HooksBrain','h:/Acquire/Hooks/BrainA.ims')
               }
    
    # choices = {
    #     1:('fmost','/CBI_Hive/globus/pitt/bil/t00_c00_0.zarr'),
    #     2:('testIMS','/CBI_FastStore/testData/bitplaneConverter.ims'),
    #     3:('HooksBrain','/CBI_Hive/Acquire/Hooks/BrainA.ims')
    #            }
    
    return choices