import os
from tiff_utils import tiff
import numpy as np


# tmp_path is a pytest fixture
def test(tmp_path='brain.tif'):
    
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)),tmp_path)
    # Test whether a tiff file can be opened
    tiffClass = tiff(path)
    
    # Do we have some of the right attributes
    assert isinstance(tiffClass.shape, tuple)
    assert isinstance(tiffClass.tags, dict)
    assert isinstance(tiffClass.res_y_microns, float)
    assert isinstance(tiffClass.res_x_microns, float)
    assert tiffClass.filePathComplete == path
    assert tiffClass.fileExtension == '.tif'
    
    assert tiffClass.image.dtype == 'uint16'
    tiffClass.toFloat()
    assert tiffClass.image.dtype == float
    
    # Can we extract a numpy array
    assert isinstance(tiffClass.image,np.ndarray)