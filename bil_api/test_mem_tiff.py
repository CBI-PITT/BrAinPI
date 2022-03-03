# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 21:16:30 2022

@author: alpha
"""

import io
from skimage import img_as_uint
import numpy as np
import tifffile as tf
import tempfile


image = np.random.random((10,10))
image = img_as_uint(image)

img_ram = io.BytesIO()
tf.imwrite(img_ram,image)
img_ram = bytearray(img_ram.getvalue())

tf.imread(io.BytesIO(img_ram))

