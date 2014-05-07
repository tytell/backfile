# -*- coding: utf-8 -*-
"""
Created on Sat Apr 12 14:46:36 2014

@author: etytel01
"""

import sys, os
import h5py
import mimetypes, magic
from PIL import Image
import numpy as np
from thumbnail import Thumbnail_Image

READLENGTH = 500
IMAGESIZE = 256

def main():
    f = h5py.File('testthumb.h5', 'w')
    
    thumb = Thumbnail_Image('/Users/etytel01/Documents/Scanner/backfile/test/testthumbs/SharkPrey.JPG', f)
    thumb.from_file()
    thumb.to_hdf5(f)
    
    ftree.to_hdf5(f)
    f.close()    
