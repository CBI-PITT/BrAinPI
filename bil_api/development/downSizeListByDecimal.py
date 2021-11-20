# -*- coding: utf-8 -*-
"""
Created on Sat Nov 20 11:23:26 2021

@author: alpha
"""

import numpy as np

aList = list(range(1000))
finalSize = 500

## Method for downsizing list to a apecific len by even spacing of elements
idx = np.round(np.linspace(0, len(aList) - 1, finalSize)).astype(int)
idx = list(idx)