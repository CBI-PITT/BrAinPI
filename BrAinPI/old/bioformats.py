# -*- coding: utf-8 -*-
"""
Created on Thu Mar  3 13:01:19 2022

@author: awatson
"""

'''
Integrate bioformats into API:
    Requires java SDK to be installed
    https://www.oracle.com/java/technologies/downloads/
    
    Path must be set to location of install
    EXAMPLE:
        nano ~/.bashrc
        PATH=/usr/lib/jvm/jdk-17/bin:$PATH
        export PATH
    

NOTE:
    Javabridge can ONLY be open once.  Must find a way to open and only 
    close after flask app shuts down
'''

# path = r'/CBI_FastStore/a.tif'
# path = r'/CBI_FastStore/testData/composite_z500_c488.tif'
path = r'/CBI_FastStore/testData/out.nd2'
path = r'/CBI_FastStore/testData/out.ims'

try:
    import javabridge
    import bioformats
    import_worked = True
except Exception:
    import_worked = False
    pass

if import_worked == False:
    pass
else:
    
    javabridge.start_vm(class_path=bioformats.JARS)
    
    def readImage(path):
        
        with bioformats.ImageReader(path) as reader:
            a = reader.read()
        
        return a

a = readImage(path)

## ONLY Call after ALL work is complete
# javabridge.kill_vm()
    

