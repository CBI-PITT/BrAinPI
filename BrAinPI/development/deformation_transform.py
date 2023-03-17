# -*- coding: utf-8 -*-
"""
Created on Sun Jun  5 14:59:09 2022

@author: alpha
"""


import os

MKL_NUM_THREADS = 12
OPENBLAS_NUM_THREADS = 12
BLIS_NUM_THREADS = 12

os.environ["MKL_NUM_THREADS"] = str(MKL_NUM_THREADS)
os.environ["OPENBLAS_NUM_THREADS"] = str(OPENBLAS_NUM_THREADS)
os.environ["BLIS_NUM_THREADS"] = str(BLIS_NUM_THREADS)

os.environ["OMP_NUM_THREADS"] = '12' # python -m threadpoolctl -i numpy scipy
os.environ["MPI_NUM_THREADS"] = '12'

import numpy as np
# try:
#     import torch
#     torch = True
#     device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
#     import torchvision.transforms as T
# except:
#     torch = False
#     device = None
import tifffile
from bg_atlasapi.bg_atlas import BrainGlobeAtlas
from skimage import io, img_as_uint, img_as_float32
from skimage.transform import resize



atlas = BrainGlobeAtlas('allen_mouse_25um')

path_to_brainreg_folder = "h:/Acquire/Hooks/analysis/BrainA.ims/resolution_level_x/registration_channel_0_25um_both_ways"

# field_scales = [1000 / resolution for resolution in atlas.resolution]
field_scales = [1000 / resolution for resolution in (10,10,10)]

print("Read input img")
# in_img = tifffile.imread(os.path.join(path_to_brainreg_folder, 'downsampled_standard.tiff'))
# in_img = tifffile.imread(os.path.join(path_to_brainreg_folder, 'boundaries.tiff'))
in_img = atlas.reference

print("Read deformation fields")
field0 = tifffile.imread(os.path.join(path_to_brainreg_folder, 'deformation_field_atlas_to_sample_0.tiff'))
field1 = tifffile.imread(os.path.join(path_to_brainreg_folder, 'deformation_field_atlas_to_sample_1.tiff'))
field2 = tifffile.imread(os.path.join(path_to_brainreg_folder, 'deformation_field_atlas_to_sample_2.tiff'))


out_img_shape = tifffile.imread(os.path.join(path_to_brainreg_folder, 'boundaries.tiff')).shape
out_img_shape = [int(x*2.5) for x in out_img_shape]
# out_img_shape = tifffile.imread(os.path.join(path_to_brainreg_folder, 'downsampled.tiff')).shape
out_img_shape_2x = tuple([int(x*2) for x in out_img_shape])
print('out_img_shape', out_img_shape)

print('Rescaling  images')
in_img = resize(in_img,out_img_shape_2x, order=0, anti_aliasing=False)
# in_img = resize(in_img,out_img_shape, anti_aliasing=False)
in_img = img_as_uint(in_img)
# in_img = np.repeat(in_img,2,axis=0)
# in_img = np.repeat(in_img,2,axis=1)
# in_img = np.repeat(in_img,2,axis=2)

field0 = resize(field0,out_img_shape_2x, order=3, anti_aliasing=False)
field1 = resize(field1,out_img_shape_2x, order=3, anti_aliasing=False)
field2 = resize(field2,out_img_shape_2x, order=3, anti_aliasing=False)


scaled_fields = [x*y for x,y in zip(field_scales,(field0,field1,field2))] # no rounding!
scaled_fields = [np.where(x < 0, 0, x) for x in scaled_fields]
# scaled_fields = [np.where(x >= y, y - 1, x) for x, y in zip(scaled_fields, out_img_shape)]
scaled_fields = [np.around(x) for x in scaled_fields]
scaled_fields = [np.where(x >= y, y - 1, x) for x, y in zip(scaled_fields, out_img_shape)]
scaled_fields = [x.astype(int) for x in scaled_fields]

out_img = np.zeros(out_img_shape, dtype=in_img.dtype)
out_img[scaled_fields[0],scaled_fields[1],scaled_fields[2]] = in_img

io.imsave(r"Z:\testData\deformation_field\out.tif",out_img)


######################  Iana's Code Using Scipy Interpolation #######################################
#https://github.com/CBI-PITT/notebooks/blob/master/scripts/deform_atlas_to_sample_interpolate.py
######################  Iana's Code Using Scipy Interpolation #######################################
