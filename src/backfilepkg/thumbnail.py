# -*- coding: utf-8 -*-
"""
Created on Sat Nov 23 10:16:17 2013

@author: etytel01
"""

import sys, os
import h5py
import mimetypes, magic
import cv2
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

        imfull = cv2.imread(self.path)
        r = IMAGESIZE / max(imfull)
        dim = tuple([int(a*r) for a in imfull.shape])
        
        self.im = cv2.resize(imfull,dim, interpolation = cv2.INTER_NEAREST)
        
    def from_hdf5(self, h5parent=None):
        if h5parent:
            self.h5parent = h5parent
        
        if 'Thumbnail' in h5parent:
            h5obj = h5parent['Thumbnail']
            data = h5obj[:]
            self.im = cv2.imdecode(data,1)
            
    def to_hdf5(self, h5parent=None):
        if h5parent:
            self.h5parent = h5parent

        data = cv2.imencode('.jpg',self.im, [cv2.cv.IMWRITE_JPEG_QUALITY, 95])
        
        if 'Thumbnail' in h5parent:
            h5obj = h5parent['Thumbnail']
            h5obj.resize(self.im.shape)
        else:
            h5obj = h5parent.create_dataset('Thumbnail',shape=self.im.shape, 
                                            maxshape=(None,),
                                            dtype=np.uint8)
        h5obj[:] = data
         

#==============================================================================
#class Thumbnail_Video(object):
# vidcap.set(cv2.cv.CV_CAP_PROP_POS_AVI_RATIO,0.9)
# Out[14]: True
# 
# In [15]: vid^Cp.set(cv2.cv.CV_CAP_PROP_POS_AVI_RATIO,0.9)
# KeyboardInterrupt
# 
# In [15]: success, image = vidcap.read()
#==============================================================================
        
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