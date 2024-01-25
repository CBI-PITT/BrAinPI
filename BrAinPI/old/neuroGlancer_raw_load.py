# -*- coding: utf-8 -*-
"""
Created on Fri Mar 18 21:11:23 2022

@author: alpha
"""

from neuroglancer_scripts.chunk_encoding import RawChunkEncoder
import os
import io

raw_chunk = r"C:\code\neuroglancer_example\cfos_example_rawdata_an21_10000_10000_10000_0-128_0-128_64-128"
chunk_shape = (128,128,64)
dtype = 'uint16'
channels = 1

encoder = RawChunkEncoder(dtype, channels)

# Read NG raw chunk
with open(raw_chunk,'rb') as file:
    img = encoder.decode(file.read(), chunk_shape)

# Write numpy to NG raw chunk to disk
with open(os.path.join(os.path.split(raw_chunk)[0],'out'),'wb') as f:
    f.write(encoder.encode(img))

# Write numpy to NG raw chunk to IO buffer
img_ram = io.BytesIO()
img_ram.write(encoder.encode(img))
img_ram.seek(0)

# Flask return of bytesIO as file
return send_file(
    img_ram,
    as_attachment=True,
    ## TODO: dynamic naming of file (specific request or based on region of request)
    download_name='out.tiff', # name needs to match chunk
    mimetype='application/octet-stream'
)