# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 16:39:53 2021

@author: alpha
"""

## Test make a chunk locator

chunks = 16
zSlices = 1000000


# chunkDict = {}
# chunkStart = 0
# chunkStop = 0
# for ii in range(zSlices):
    
#     if ii%chunks == 0 and chunkStop < zSlices:
#         chunkStart = chunkStop
#         chunkStop = chunkStop + chunks if chunkStop + chunks <= zSlices else zSlices
    
#     chunkDict[ii] = chunkStart,chunkStop
    


chunkDict = {}
chunkStart = 0
chunkStop = 0
for ii in range(zSlices):
    
    mod = ii%chunks
    if mod == 0 and chunkStop < zSlices:
        chunkStart = chunkStop
        chunkStop = chunkStop + chunks if chunkStop + chunks <= zSlices else zSlices
    
    
    # key=pixel value
    # (fileNumber, startIndexInFile)
    chunkDict[ii] = chunkStart, mod


chunkDict[63]

start = 63
stop = 100

slices = []
for ii in range(start,stop):
    if 
    
