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
THUMBSIZE = 256

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
        self.size = imfull.shape[:2]
        if (imfull.shape[0] > THUMBSIZE) or (imfull.shape[1] > THUMBSIZE):
            rat = float(THUMBSIZE) / max(imfull.shape[:2])
            dim = (int(imfull.shape[0]*rat), int(imfull.shape[1]*rat))
            
            self.im = cv2.resize(imfull,dim, interpolation = cv2.INTER_NEAREST)
        else:
            self.im = imfull
        
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

        success,data = cv2.imencode('.jpg',self.im, [cv2.IMWRITE_JPEG_QUALITY, 95])
        
        if success:
            if 'Thumbnail' in h5parent:
                h5obj = h5parent['Thumbnail']
                h5obj.resize(data.shape)
                h5obj[:] = data
            else:
                h5obj = h5parent.create_dataset('Thumbnail',data=data, 
                                                maxshape=(None,1))
            h5obj.attrs['Size'] = self.size
            if len(self.im.shape) == 3:
                h5obj.attrs['Depth'] = self.im.shape[2]
            else:
                h5obj.attrs['Depth'] = 1
            h5obj.attrs['Type'] = str(self.im.dtype)
        

class Thumbnail_Video(object):
    '''
    Class to read thumbnails from video type files
    '''
    
    def __init__(self, path=None, h5parent=None):
        self.path = path
        self.h5parent = h5parent
        
    def from_file(self, path=None):
        if path:
            self.path = path

        cap = cv2.VideoCapture(self.path)
        
        success, im1 = cap.read()
        if not success:
            self.im = None
            return
            
        cap.set(cv2.cv.CV_CAP_PROP_POS_AVI_RATIO,0.5)
        success, im2 = cap.read()
        if not success:
            self.im = None
            return
        
        cap.set(cv2.cv.CV_CAP_PROP_POS_AVI_RATIO,0.99)
        success, im3 = cap.read()
        if not success:
            self.im = None
            return

        self.size = (int(cap.get(cv2.cv.CV_CAP_PROP_FRAME_WIDTH)),
                     int(cap.get(cv2.cv.CV_CAP_PROP_FRAME_HEIGHT))
                     
        self.nframes = int(cap.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        self.fps = cap.get(cv2.cv.CV_CAP_PROP_FPS)
        
        cap.release()
        
        if (im1.shape[0] > THUMBSIZE) or (im1.shape[1] > THUMBSIZE):
            rat = float(THUMBSIZE) / max(im1.shape[:2])
            dim = (int(im1.shape[0]*rat), int(im1.shape[1]*rat))
            
            self.im = [cv2.resize(im1,dim, interpolation = cv2.INTER_NEAREST),
                       cv2.resize(im2,dim, interpolation = cv2.INTER_NEAREST),
                       cv2.resize(im3,dim, interpolation = cv2.INTER_NEAREST)]
        else:
            self.im = [im1, im2, im3]
        
    def from_hdf5(self, h5parent=None):
        if h5parent:
            self.h5parent = h5parent
        
        if 'Thumbnail' in h5parent:
            h5obj = h5parent['Thumbnail']
            data = h5obj[:]

            self.im = [cv2.imdecode(data[:,i],1) for i in xrange(data.shape[1])]
            self.size = h5obj.attrs['Size']
            self.nframes = h5obj.attrs['NFrames']
            self.fps = h5obj.attrs['FramesPerSec']
            
            
    def to_hdf5(self, h5parent=None):
        if h5parent:
            self.h5parent = h5parent

        ok, data = zip(*[cv2.imencode('.jpg',im1, [cv2.IMWRITE_JPEG_QUALITY, 95]) for im1 in self.im])
        if all(ok):
            vlendatatype = h5py.special_dtype(vlen=data[0].dtype)
            try:
                h5obj = h5parent.require_dataset('Thumbnail',dtype=vlendatatype,
                                                 shape=(3,))
            except TypeError:
                del h5parent['Thumbnail']
                h5obj = h5parent.require_dataset('Thumbnail',dtype=vlendatatype,
                                                 shape=(3,))
                
            for (i,data1) in enumerate(data):
                h5obj[i] = data1[:,0]

            h5obj.attrs['Size'] = self.size
            if len(self.im[0].shape) == 3:
                h5obj.attrs['Depth'] = self.im[0].shape[2]
            else:
                h5obj.attrs['Depth'] = 1
            h5obj.attrs['Type'] = str(self.im[0].dtype)
            h5obj.attrs['NFrames'] = self.nframes
            h5obj.attrs['FramesPerSec'] = self.fps
            
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
                 'text': Thumbnail_Text,
                 'video': Thumbnail_Video}

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