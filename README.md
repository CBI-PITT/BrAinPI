# bil_api

The goal of this project is to create a flask-based API for public access to Brain Image Library (BIL) datasets in many forms



The broader goals are for the API to provide multi-resolution image data to enable visualization of multi-terabyte datasets over the internet.  This will first be implemented in Napari, but can be used used by any program that can interact with the API and make use of multi-resolution series data.



```python
conda create -y -n bil_api python=3.8
conda activate bil_api

pip install -e /path/to/cloned/repo/

## Run API
'''
Note: Before running the API edit the location of .ims and .zarr datasets in dataset_info.py.  IMS will work out of the box, zarr is currently likely to fail.
'''

python -i /path/to/cloned/repo/bil_api/flaskAPI.py

## Run Client
'''
Note: Before running the client edit testAPIClient.py and change 'baseURL' to address of API server.
'''
python -i /path/to/cloned/repo/bil_api/testAPIClient.py
```

Note:   IMS files will work out of the box, zarr is currently likely to fail.
