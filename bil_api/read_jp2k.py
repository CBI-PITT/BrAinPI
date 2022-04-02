# -*- coding: utf-8 -*-
"""
Created on Fri Apr  1 12:40:03 2022

@author: awatson
"""

import matplotlib as plt
import matplotlib.image as mpimg
from skimage import io

file = r'C:/Users/awatson/Downloads/StitchedImage_Z021_L001.jp2'
a = mpimg.imread(file)
io.imshow(a)