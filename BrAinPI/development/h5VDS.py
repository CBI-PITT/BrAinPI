# -*- coding: utf-8 -*-
"""
Created on Thu Nov 18 20:34:08 2021

@author: alpha
"""
import h5py, os, random
import numpy as np
import hdf5plugin

dataset_shape = (100,10000,10000)
location = r'c:\code\h5test'

z = list(range(dataset_shape[0]))
y = list(range(dataset_shape[1]))
x = list(range(dataset_shape[2]))

layout = h5py.VirtualLayout(shape=dataset_shape, dtype=np.uint16)


# Create source files (0.h5 to 3.h5)
for n in range(dataset_shape[0]):
    filename = os.path.join(location,"{}.h5".format(n))
    print(filename)
    with h5py.File(filename, "w") as f:
        d = f.create_dataset(
            "data", 
            shape=(1,dataset_shape[1],dataset_shape[2]),
            chunks=(1,1000,1000),
            data=np.random.random_integers(0,65535,(1,dataset_shape[1],dataset_shape[2])),
            **hdf5plugin.Blosc(cname='blosclz', clevel=9, shuffle=hdf5plugin.Blosc.SHUFFLE)
            )

# Assemble virtual dataset
layout = h5py.VirtualLayout(shape=dataset_shape, dtype=np.uint16)
for n in range(dataset_shape[0]):
    filename = os.path.join(location,"{}.h5".format(n))
    print(filename)
    vsource = h5py.VirtualSource(filename, "data", shape=(1,dataset_shape[1],dataset_shape[2]))
    layout[n] = vsource

# Add virtual dataset to output file
with h5py.File(os.path.join(location,"VDS.h5"), "w", libver="latest") as f:
    f.create_virtual_dataset("vdata", layout, fillvalue=0)
    # f.create_dataset("data", data=data, dtype="i4")




# read data back
# virtual dataset is transparent for reader!
with h5py.File(os.path.join(location,"VDS.h5"), "r") as f:
    print("Virtual dataset:")
    print(f["vdata"][0,0,0:10])


    for ii in range(100000):
        zz = random.choice(z)
        yy = random.choice(y)
        xx = random.choice(x)
        print((ii,f['vdata'][zz,yy,xx]))



f = h5py.File(os.path.join(location,"VDS.h5"), "a")
g = f['vdata']
print(g[0:2,0:1001,0:1001])
g[0:2,0:1001,0:1001] = 42
print(g[0:2,0:1001,0:1001])




