# -*- coding: utf-8 -*-
"""
Created on Sun Jun  5 14:59:09 2022

@author: alpha
"""
# import numpy as np 

# A = np.array([[0.32, 0.35, 0.88, 0.63, 1.  ],
#               [0.23, 0.69, 0.98, 0.22, 0.96],
#               [0.7 , 0.51, 0.09, 0.58, 0.19],
#               [0.98, 0.42, 0.62, 0.94, 0.46],
#               [0.48, 0.59, 0.17, 0.23, 0.98]])

# B = np.array([[4, 0, 3, 2, 1],
#               [3, 2, 4, 1, 0],
#               [4, 3, 0, 2, 1],
#               [4, 2, 0, 3, 1],
#               [0, 3, 1, 2, 4]])

# desired_output = np.array([[1.  , 0.32, 0.63, 0.88, 0.35],
#        [0.22, 0.98, 0.96, 0.69, 0.23],
#        [0.19, 0.58, 0.7 , 0.09, 0.51],
#        [0.46, 0.62, 0.98, 0.94, 0.42],
#        [0.48, 0.23, 0.59, 0.17, 0.98]])



# m,n = A.shape
# B + n*np.arange(m)[:,None]

# a = np.zeros((4,4))
# b = np.zeros((4,4))
# c = np.zeros((4,4))

# # Start poistion = [0,0]
# # Final value should be in position [1,1]
# a[0,0] = 2
# b[0,0] = 3
# a.flatten()*c.shape[0] + b.flatten()


# z = np.zeros((4,4,4))
# y = np.zeros((4,4,4))
# x = np.zeros((4,4,4))
# desired = np.zeros((4,4,4))
# desired[2,3,2] = 1
# ## Need to produce 46

# # Start poistion = [0,0]
# # Final value should be in position [1,1]
# z[0,0,0] = 2
# y[0,0,0] = 3
# x[0,0,0] = 2

# out = z.flatten()*z.shape[0] + b.flatten()*a.shape[1] + c.flatten()
# out = x.flatten()*x.shape[-1] * y.flatten()*y.shape[-2] + z.flatten()*z.shape[-3]

# #Get to z
# #num * elements in yx
# 2*(4*4)
# # Get to y
# # num + elements in x
# 2*(4*4) + 3*4
# # Get to x
# # num + elements in y
# 2*(4*4) + 3*4 + 2

# out = z.flatten()*(z.shape[-2]*z.shape[-1]) +\
#     y.flatten()*y.shape[-1] +\
#         x.flatten()

from math import prod
import numpy as np
from skimage import io
from numba import jit

# shape = (1,1024,1024)

# array = np.arange(prod(shape),dtype=float).reshape(shape)
# z = np.zeros(shape)
# y = np.zeros(shape)
# x = np.zeros(shape)
# z[0,0,0] = 0
# y[0,0,0] = 500
# x[0,0,0] = 256

# z[0,1,0] = 0
# y[0,1,0] = 124
# x[0,1,0] = 111


# def deform(array, fields):
#     '''
#     array: 
#         a numpy array that will be deformed according to
#         the deformation fields provided in fields
    
#     fields:
#         A tuple of deformation fields where there is 1 field for 
#         each axis in array.  Each index in array will yield an new 
#         index for the associated axis.  
        
#         For example:
#             an array of shape (5,5,5), axes (z,y,x) and index location [1,2,3]
#             fields[0][1,2,3] = 'z index mapping the value in array[1,2,3] to its z-index in the output array'
#             fields[1][1,2,3] = 'y index mapping the value in array[1,2,3] to its y-index in the output array'
#             fields[2][1,2,3] = 'x index mapping the value in array[1,2,3] to its x-index in the output array'
    
#     Note:
#         Shapes of ALL array (both array and fields) must be the same
#     '''
    
#     # Round all numbers and convert array to int
#     # fields = [np.around(x).astype(int) for x in fields]
#     fields = [x*1000/y for x,y in zip(fields,[5.33,1.99,1.99])]
#     # fields = [x*10 for x in fields]
#     fields = [x.astype(int) for x in fields]
    
    
#     # Form flattened index
#     for idx, _ in enumerate(array.shape):
        
#         if idx == 0:
#             index = fields[idx].flatten() * prod( list(array.shape[1:]) + [1] )
#         elif idx == array.ndim-1:
#             index = index + fields[idx].flatten()
#         else:
#             index = index + fields[idx].flatten() * prod( list(array.shape[idx+1:]) + [1] )
    
#     # index = index.astype(int)
#     return np.take(array,index,mode='clip').reshape(array.shape)

def deform(array, fields,resolution):
    '''
    array: 
        a numpy array that will be deformed according to
        the deformation fields provided in fields
    
    fields:
        A tuple of deformation fields where there is 1 field for 
        each axis in array.  Each index in array will yield an new 
        index for the associated axis.  
        
        For example:
            an array of shape (5,5,5), axes (z,y,x) and index location [1,2,3]
            fields[0][1,2,3] = 'z index mapping the value in array[1,2,3] to its z-index in the output array'
            fields[1][1,2,3] = 'y index mapping the value in array[1,2,3] to its y-index in the output array'
            fields[2][1,2,3] = 'x index mapping the value in array[1,2,3] to its x-index in the output array'
    
    Note:
        Shapes of ALL array (both array and fields) must be the same
    '''
    if isinstance(resolution,int):
        resolution = (resolution,resolution,resolution)
    # Round all numbers and convert array to int
    # fields = [np.around(x).astype(int) for x in fields]
    # fields = [x*1000/y for x,y in zip(fields,resolution)]
    # fields = [x/y for x,y in zip(fields,[5.33,1.99,1.99])]
    # fields = [x*10 for x in fields]
    fields = [x.astype(int) for x in fields]
    fields = [np.where(x<0, 0,x) for x in fields]
    fields = [np.where(x>=y, y-1,x) for x,y in zip(fields,array.shape)]
    
    return array[fields[0],fields[1],fields[2]]

# def deform(array, fields,resolution):
#     '''
#     array: 
#         a numpy array that will be deformed according to
#         the deformation fields provided in fields
    
#     fields:
#         A tuple of deformation fields where there is 1 field for 
#         each axis in array.  Each index in array will yield an new 
#         index for the associated axis.  
        
#         For example:
#             an array of shape (5,5,5), axes (z,y,x) and index location [1,2,3]
#             fields[0][1,2,3] = 'z index mapping the value in array[1,2,3] to its z-index in the output array'
#             fields[1][1,2,3] = 'y index mapping the value in array[1,2,3] to its y-index in the output array'
#             fields[2][1,2,3] = 'x index mapping the value in array[1,2,3] to its x-index in the output array'
    
#     Note:
#         Shapes of ALL array (both array and fields) must be the same
#     '''
    
#     # Round all numbers and convert array to int
#     # fields = [np.around(x).astype(int) for x in fields]
#     print('Multiplying Resolution')
#     fields = [x*1000/y for x,y in zip(fields,[resolution,resolution,resolution])]
#     # fields = [x/y for x,y in zip(fields,[5.33,1.99,1.99])]
#     # fields = [x*10 for x in fields]
#     # print('Subtracting Min')
#     # fields = [x-x.min() for x in fields]
#     # print('Dividing Max')
#     # fields = [x/x.max() for x in fields]
#     # print('Scaling to Shape')
#     # fields = [x * (y-1) for x,y in zip(fields,array.shape)]
#     print('Converting to int')
#     fields = [x.astype(int) for x in fields]


#     print('Deforming')
#     return array[fields[0],fields[1],fields[2]]

origional = io.imread(r"Z:\testData\deformation_field\10um_raw_input.tiff")
origional = np.swapaxes(origional,0,1)
atlas_10um = io.imread(r"H:\CBI\Alan\Alan\Projects\Allen Atlas\atlasTemplates\zstack_10\volume.tif")
atlas_10um =  np.swapaxes(atlas_10um,0,1)
atlas_aligned = io.imread(r"Z:\testData\deformation_field\downsampled.tiff")


# field_scales = [int(1000 / resolution) for resolution in [5.33,1.992,1.992]]

####  Test Data
# z_deform = io.imread(r"H:\Acquire\Hooks\BrainA_alignment\registration_autofluorescence_fft_denoised\deformation_field_0.tiff")
# y_deform = io.imread(r"H:\Acquire\Hooks\BrainA_alignment\registration_autofluorescence_fft_denoised\deformation_field_1.tiff")
# x_deform = io.imread(r"H:\Acquire\Hooks\BrainA_alignment\registration_autofluorescence_fft_denoised\deformation_field_2.tiff")

####  Test Data
z_deform = io.imread(r"Z:\testData\deformation_field\deformation_field_0.tiff")
y_deform = io.imread(r"Z:\testData\deformation_field\deformation_field_1.tiff")
x_deform = io.imread(r"Z:\testData\deformation_field\deformation_field_2.tiff")

