# -*- coding: utf-8 -*-
"""
Created on Sat Nov 23 10:16:17 2013

@author: etytel01
"""

import sys, os
import h5py
import mimetypes, magic
from PIL import Image
import numpy as np

READLENGTH = 500
IMAGESIZE = 256

class Thumbnail_Image(object):
    '''
    Class to read thumbnails from image type files
    '''
    
    def __init__(self, path=None, h5parent=None):
        self.path = path
        self.h5parent = h5parent
        
    def from_file(self, path=None):
        if path:
            self.path = path

        self.im = Image.open(self.path)
        self.im.thumbnail((IMAGESIZE,IMAGESIZE))
        
    def from_hdf5(self, h5parent=None):
        if h5parent:
            self.h5parent = h5parent
        
        if 'Thumbnail' in h5parent:
            h5obj = h5parent['Thumbnail']
            data = h5obj[:]
            sz = data.shape
            data.reshape((data.size,))
            self.im = Image.fromstring("RGB",sz[0:2],data)
            
    def to_hdf5(self, h5parent=None):
        if h5parent:
            self.h5parent = h5parent

        sz = self.im.size + (3,)           
        data = np.fromstring(self.im.tostring(), dtype=np.uint8)
        data = np.reshape(data,sz)
        
        h5obj = h5parent.require_dataset('Thumbnail',shape=(IMAGESIZE,IMAGESIZE,3),
                                         dtype=np.uint8)
        h5obj[0:sz[0],0:sz[1],:] = data
         
        
class Thumbnail_Text(object):
    '''
    Class to read thumbnails from text type files
    '''
    
    def __init__(self, path=None, h5parent=None):
        self.path = path
        self.h5parent = h5parent
        
    def from_file(self, path=None):
        if path:
            self.path = path
        
        f = open(self.path, 'r')
        if os.path.getsize(self.path) > 2*READLENGTH:
            a = f.read(READLENGTH)
            f.seek(-READLENGTH,2)
            b = f.read(READLENGTH)
            self.text = [a,b]
        else:
            a = f.read(READLENGTH)
            b = f.read()
            if b:
                self.text = [a,b]
            else:
                self.text = [a]

    def from_hdf5(self, h5parent=None):
        if h5parent:
            self.h5parent = h5parent
        
        if 'Thumbnail' in h5parent:
            h5obj = h5parent['Thumbnail']
        
            (_,n) = h5obj.shape
            a = h5obj[:,0]
            if n == 2:
                b = h5obj[:,1]
                tlen = h5obj.attrs['ThumbnailLen']
                if tlen < 2*READLENGTH:
                    b = b[0:(tlen-READLENGTH)]
            else:
                b = []
                
            self.text = [a,b]

    def to_hdf5(self, h5parent=None):
        if h5parent:
            self.h5parent = h5parent
        
        if 'Thumbnail' in h5parent:
            h5obj = h5parent['Thumbnail']
            
            (n,) = h5obj.shape
            if n != len(self.text):
                h5obj.set_extent((n,))
            for (i,txt) in enumerate(self.text):
                h5obj[i] = txt
        else:
            h5obj = h5parent.create_dataset('Thumbnail',
                                            data=self.text,
                                            maxshape=(None,))
            
                
            
thumbtypes = {'image': Thumbnail_Image,
                 'text': Thumbnail_Text}

def get_thumbnail(path=None, h5obj=None):
    thumb = None
    if path:
        (mimetp,enc) = mimetypes.guess_type(path)
        if not mimetp:
            mimetp = magic.from_file(path, mime=True)
        (mimebase, mimedetail) = mimetp.split('/')
        
        if mimebase in thumbtypes:
            try:
                thumb = thumbtypes[mimebase](path=path)
            except IOError:
                thumb = None
            
    elif h5obj:
        if 'Thumbnail' in h5obj:
            tobj = h5obj['Thumbnail']
            tp = tobj.attrs['Type']
            if tp in thumbtypes:
                thumb = thumbtypes[tp](h5obj=h5obj)
            else:
                thumb = None
    return thumb