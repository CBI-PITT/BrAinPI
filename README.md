# BrAinPI

BrAinPI, pronounced "Brain Pie", is a Flask-based API for serving a variety of multiscale imaging data for access over the web or for visualization.  Originally designed for serving imaging data of whole brains from the Brain Image Library, it can be used for any type of imaging data.

BrAinPI provides a file browser interface that allows one to expose a file system with basic user and group management.  The browser supports basic file download.  When multiscale chunked imaging file types are available (ie. HDF5, Zarr, OME-Tiff), BrAinPI can view the dataset in neuroglancer or provide links where users can request image information from arbitrary regions within a dataset.  

BrAinPI is built as a webservice to enable the integration of large imaging data with application for visualization and processing workflows.



```python
conda create -y -n bil_api python=3.8
conda activate bil_api

git clone https://github.com/CBI-PITT/BrAinPI.git
pip install -e /path/to/cloned/repo/

## Before running BrAinPI

'''
Note: Before running the API edit the settings_TEMPLATE.ini and groups_TEMPLATE.ini files and rename them to settings.ini and groups.ini.
'''

For development:
	python -i /path/to/cloned/repo/bil_api/BrAinPI.py

In production:
    gunicorn -b 0.0.0.0:5000 --chdir path/to/cloned/repo/BrAinPI wsgi:app -w 4 --threads 6
    #Adjust these parameters for your specific situation
    #-w = workers
    #-threads = threads per worker

```

Note:   IMS files will work out of the box, zarr is currently likely to fail.
