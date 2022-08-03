# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 21:11:13 2022

@author: awatson
"""

import zarr
import numpy as np
import tifffile
# import imagecodecs
from copy import deepcopy


file = r'C:\code\testData\191817_05308_CH1.tif'
file = r'H:\globus\pitt\bil\fMOST RAW\CH1\182725_03717_CH1.tif'

class tiff_manager:
    def __init__(self,file,desired_chunk_depth=64):
        self.file = file
        with tifffile.imread(self.file,aszarr=True) as store:
            img = zarr.open(store)
            self.shape = img.shape
            self.nbytes = img.nbytes
            self.ndim = img.ndim
            self.chunks = img.chunks
            self.dtype = img.dtype
            self._desired_chunk_depth = desired_chunk_depth
            del img
        
        self._adjust_chunk_depth()
        
    def __getitem__(self,key):
        # if key == (np.s_[0:0],)*self.ndim:
        if key == (slice(0,0,None),)*self.ndim:
            #Hack to speed up dask array conversions
            return np.asarray([],dtype=self.dtype)
        return self._read_tiff(key)
    
    def _read_tiff(self,key):
        with tifffile.imread(self.file,aszarr=True) as store:
            return zarr.open(store)[key]
        
    def _change_file(self,file):
        self.file = file
    
    def clone_manager_new_file(self,file):
        '''
        Changes only the file associated with the class
        Assumes that the new file shares all other properties
        No attempt is made to verify this
        
        This method is designed for speed.
        It is to be used when 1000s of tiff files must be referenced and 
        it avoids opening each file to inspect metadata
        
        Returns: a new instance of the class with a different filename
        '''
        new = deepcopy(self)
        new._change_file(file)
        return new
        
    def _adjust_chunk_depth(self):
        if self._desired_chunk_depth % self.chunks[0] == 0:
                self.chunks = (self._desired_chunk_depth,*self.chunks[1:])
    


class tiff_manager_3d:
    def __init__(self,fileList,desired_chunk_depth_y=64):
        assert isinstance(fileList,(list,tuple))
        self.fileList = fileList
        with tifffile.imread(self.fileList[0],aszarr=True) as store:
            img = zarr.open(store)
            self.shape = img.shape
            self.nbytes = img.nbytes
            self.ndim = img.ndim
            self.chunks = img.chunks
            self.dtype = img.dtype
            del img
        
        self._desired_chunk_depth_y = desired_chunk_depth_y
        self._conv_3d()
        self._adjust_chunk_depth()
        
    def _conv_3d(self):
        z_depth = len(self.fileList)
        self.shape = (z_depth,*self.shape)
        self.nbytes = int(self.nbytes*z_depth)
        self.ndim = 3
        self.chunks = (z_depth,*self.chunks)
    
    
    def __getitem__(self,key):
        #Hack to speed up dask array conversions
        if key == (slice(0,0,None),)*self.ndim:
            return np.asarray([],dtype=self.dtype)
        
        return self._get_3d(key)
    
    def _adjust_chunk_depth(self):
        if self._desired_chunk_depth_y % self.chunks[1] == 0:
                self.chunks = (self.shape[0],self._desired_chunk_depth_y,*self.chunks[2:])
    
    @staticmethod
    def _format_slice(key):
        # print('In Slice {}'.format(key))
        if isinstance(key,slice):
            return (key,)
        
        if isinstance(key,int):
            return (slice(key,key+1,None),)
        
        if isinstance(key,tuple):
            out_slice = []
            for ii in key:
                if isinstance(ii,slice):
                    out_slice.append(ii)
                elif isinstance(ii,int):
                    out_slice.append(slice(ii,ii+1,None))
                else:
                    out_slice.append(ii)
                    
        # print('Out Slice {}'.format(out_slice))
        return tuple(out_slice)
        
    def _slice_out_shape(self,key):
        key = self._format_slice(key)
        key = list(key)
        if isinstance(key,int):
            key = [slice(key,None,None)]
            # print(key)
        out_shape = []
        for idx,_ in enumerate(self.shape):
            if idx < len(key):
                if isinstance(key[idx],int):
                    key[idx] = slice(key[idx],None,None)
                # print(key)
                test_array = np.asarray((1,)*self.shape[idx],dtype=bool)
                # print(test_array)
                # print(key[idx])
                test_array = test_array[key[idx]].shape[0]
                # print(test_array)
                out_shape.append(
                        test_array
                    )
            else:
                out_shape.append(self.shape[idx])
        out_shape = tuple(out_shape)
        # print(out_shape)
        return out_shape
        
    
    def _read_tiff(self,key,idx):
        with tifffile.imread(self.fileList[idx],aszarr=True) as store:
            return zarr.open(store)[key]
        
    def _get_3d(self,key):
        key = self._format_slice(key)
        shape_of_output = self._slice_out_shape(key)
        canvas = np.zeros(shape_of_output,dtype=self.dtype)
        # print(canvas.shape)
        
        # if len(key) == 1:
        #     key = key[slice(None)]
        
        for idx in range(canvas.shape[0]):
            two_d = key[1:]
            # print(two_d)
            if len(two_d) == 1:
                two_d = two_d[0]
            # print(two_d)
            canvas[idx] = self._read_tiff(two_d,idx)
        return canvas
    
    def _change_file_list(self,fileList):
        old_zdepth = self.shape[0]
        
        self.fileList = fileList
        
        new_zdepth = len(self.fileList)
        self.shape = (new_zdepth,*self.shape[1:])
        self.nbytes = int(self.nbytes / old_zdepth * new_zdepth)
        self.chunks = (new_zdepth,*self.chunks[1:])
        
    
    def clone_manager_new_file_list(self,fileList):
        '''
        Changes only the file associated with the class
        Assumes that the new file shares all other properties
        No attempt is made to verify this
        
        This method is designed for speed.
        It is to be used when 1000s of tiff files must be referenced and 
        it avoids opening each file to inspect metadata
        
        Returns: a new instance of the class with a different filename
        '''
        new = deepcopy(self)
        new._change_file_list(fileList)
        return new











