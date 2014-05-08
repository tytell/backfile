# -*- coding: utf-8 -*-
"""
Created on Thu May  8 09:57:49 2014

@author: etytel01
"""

import os, time, shutil
import sys
import h5py
import multiprocessing
import numpy as np
import hashlib

from filetree import RootTree
from progress import ProgressCLI
from test import build_test_directory, modify_dir

TIME_FORMAT = '%Y-%m-%d %H:%M:%S %Z'
HASH_FUNCTION = hashlib.sha256
BLOCK_SIZE = 128 * HASH_FUNCTION().block_size

class FileData(object):
    '''
    Data on a file
    '''
    
    name = None
    fullpath = None
    root = None
    
    def __init__(self, fullpath, isdir=None, size=None, modified=None, hashval=None, thumbnail=None):
        self.fullpath = fullpath
        
        (_,name) = os.path.split(fullpath)
        self.name = name
        self.size = size
        self.modified = modified
        self.hashval = hashval
        self.thumbnail = thumbnail
        self.isdir = isdir
        
    def __str__(self):
        if self.isdir:
            return '{0} (dir)'.format(self.fullpath)
        else:
            return '{0}: size = {1}; hash = {2}'.format(self.fullpath, self.size, self.hexdigest())

    def hexdigest(self):
        if self.hashval is not None:
            return ''.join("%02X" % n for n in self.hashval)
        else:
            return 'None'

    def __eq__(self, other):
        if isinstance(other, FileData):
            if self.hashval is not None and other.hashval is not None:
                return all(self.hashval == other.hashval)
            else:
                return (self.size == other.size) and (self.modified == other.modified)
        else:
            return False
    
    def __ne__(self, other):
        return not self.__eq__(other)
            
    def from_path(self):
        if os.path.isdir(self.fullpath):
            self.isdir = True
        else:
            self.isdir = False
            self.size = os.path.getsize(self.fullpath)
            self.modified = time.localtime(os.path.getmtime(self.fullpath))

    def from_hdf5(self, h5gp):
        if h5gp.attrs['IsDir']:
            self.isdir = True
        else:
            self.isdir = False
            if 'Size' in h5gp.attrs:
                self.size = h5gp.attrs['Size']
            if 'Modified' in h5gp.attrs:
                self.modified = time.struct_time(h5gp.attrs['Modified'])
            
            #self.islink = h5gp.attrs["IsLink"]
            ## TODO: process links better here
            
            if "Hash" in h5gp:
                self.hashval = np.zeros(HASH_FUNCTION().digest_size, dtype='uint8')
                self.hashval = h5gp["Hash"][:,-1]
            else:
                self.hashval = None
        
class FileDataWriter(object):
    '''
    Writes file data to a log file
    '''

    def __init__(self, outfile, rootpath=None):
        if os.path.isfile(outfile):
            self.filename = outfile
            self.open_file(outfile)
        else:
            self.init_file(outfile, rootpath)
            
    def open_file(self, filename):
        self.h5file = h5py.File(filename, 'a')
        
        self.rootgp = self.h5file.require_group('ROOT')
        self.rootpath = self.rootgp.attrs['RootPath']
        assert(('IsDir' in self.rootgp.attrs) and self.rootgp.attrs['IsDir'] == 1)
        assert(os.path.exists(self.rootpath))
        self.deletedgp = self.h5file.require_group('DELETED')
        
    def init_file(self, filename, rootpath):
        assert(rootpath is not None)
        
        self.h5file = h5py.File(filename, 'w')
        self.rootpath = os.path.abspath(rootpath)
        
        self.rootgp = self.h5file.create_group('ROOT')
        self.rootgp.attrs['RootPath'] = self.rootpath
        self.rootgp.attrs['IsDir'] = 1
        
        self.deletedgp = self.h5file.create_group('DELETED')
        
    def close(self):
        self.h5file.close()
        
    def write_data(self, fd, relpath=None, parentgp=None):
        if relpath is None:
            relpath = os.path.relpath(fd.fullpath, self.rootpath)
            
        if parentgp is None:
            if relpath == '.':
                gp = self.rootgp
            else:
                gp = self.rootgp.require_group(relpath)
        else:
            gp = parentgp.require_group(fd.name)
            
        if fd.isdir:
            if ('IsDir' in gp.attrs) and (gp.attrs['IsDir'] == 0):
                #it's actually a file
                self.make_deleted(relpath)
                gp = self.rootgp.create_group(relpath)
            gp.attrs['IsDir'] = 1
        else:               
            gp.attrs['IsDir'] = 0
            if fd.size is not None:
                gp.attrs['Size'] = fd.size
            if fd.modified is not None:
                gp.attrs["Modified"] = np.array(fd.modified, dtype='int32')
        
            if fd.hashval is not None:
                self.write_hash(gp, fd)
        
        return gp
        
    def write_hash(self, gp, fd):
        '''
        Write a new hash value to the HDF5 group, extending the hash dataset if needed
        '''
        if 'Hash' in gp:
            #hash dataset is already there
            hset = gp['Hash']
            dateset = gp['HashDate']
            
            (ds,n) = hset.shape
            assert(n > 0)
            
            lasthash = hset[:,n-1]
            
            #extend it if the last value doesn't match the current hash
            if np.any(lasthash != fd.hashval):
                hset.resize((ds,n+1))
                dateset.resize((9,n+1))
                ind = n
                isnewhash = True
            else:
                ind = n-1
                isnewhash = False
        else:
            #otherwise just create the dataset
            hset = gp.create_dataset('Hash',(HASH_FUNCTION().digest_size,1),
                                   maxshape=(HASH_FUNCTION().digest_size,None),
                                   dtype='uint8')
            dateset = gp.create_dataset('HashDate',(9,1),
                                   maxshape=(9,None),
                                   dtype='int16')
            ind = 0
            isnewhash = True
            
        if isnewhash:
            hset[:,ind] = fd.hashval
        dateset[:,ind] = np.array(time.localtime(), dtype='int16')
        
    def make_deleted(self, name, parentgp, parentpath):
        gp = parentgp[name]
        
        if 'Hash' in gp:
            hashval1 = np.zeros(HASH_FUNCTION().digest_size, dtype='uint8')
            hashval1 = gp["Hash"][:,-1]
            
            hashtxt1 = ''.join("%02X" % n for n in hashval1)
            
            hashgp1 = self.deletedgp.require_group(hashtxt1)
            if hashgp1.keys():
                nm = str(int(hashgp1.keys()[-1])+1)
            else:
                nm = '1'
                
            hashgp1.file.copy(gp, hashgp1, name=nm)
            hashgp1[nm].attrs['OriginalPath'] = os.path.join(parentpath, name)
            
            del parentgp[name]
        else:
            del parentgp[name]
            
    def scan(self):
        '''
        Scan the whole path and compare/update the HDF5 tree
        '''
        assert(self.rootpath is not None)
        assert(os.path.exists(self.rootpath))
        
        needshash = []
        hashsize = 0
        for maindir,subdirs,files in os.walk(self.rootpath):
            fd = FileData(maindir, isdir=True)
            
            gp = self.write_data(fd)
            deleted = set(gp.keys())

            for subdir in subdirs:
                subfd = FileData(os.path.join(maindir,subdir), isdir=True)
                self.write_data(subfd, parentgp=gp)
                deleted.discard(subdir)
                
            for filename in files:
                ondisk = FileData(os.path.join(maindir, filename))
                ondisk.from_path()
            
                if filename in gp:
                    infile = FileData(os.path.join(maindir, filename))
                    infile.from_hdf5(gp[filename])
                    
                    if ondisk != infile:
                        self.write_data(ondisk,parentgp=gp)
                        needshash.append(ondisk.fullpath)
                        hashsize += ondisk.size
                else:
                    self.write_data(ondisk, parentgp=gp)
                    needshash.append(ondisk.fullpath)
                    hashsize += ondisk.size
                deleted.discard(filename)

            for name in deleted:
                self.make_deleted(name, gp, maindir)
        
        return (needshash, hashsize)
        
    def update_hashes(self, filenames, totalsize):
        with ProgressCLI(unit='sec', total=totalsize) as prog:
            for name in filenames:
                fd = FileData(name, isdir=False)
                h = get_file_hash(name, prog)
                fd.hashval = np.frombuffer(h.digest(), dtype=np.uint8, count=h.digest_size)
                self.write_data(fd)

def get_file_hash(filename, progress=None):
    h = HASH_FUNCTION()
    with open(filename, 'rb') as fid:
        def readblock():
            return fid.read(BLOCK_SIZE)
        
        for b in iter(readblock,''):
            h.update(b)
            if progress:
                progress.update(len(b), info=filename)
                
    return h   
    
        
def main():
    testdir = '/Users/etytel01/Documents/Scanner/backfile/test/testdir1'
    outfile = '/Users/etytel01/Documents/Scanner/backfile/test/newtest.h5'
    if os.path.exists(testdir):
        shutil.rmtree(testdir)
    if os.path.exists(outfile):
        os.unlink(outfile)
        
    ftree = build_test_directory(testdir, depth=3, filesperdir=3, minfiles=2, dirsperdir=1,
                                  filesizes=10*1024, randomize=True) 

    
    scanner = FileDataWriter(outfile, testdir)                                      
    needshash, hashsize = scanner.scan()
    scanner.update_hashes(needshash, hashsize)
    
    modify_dir(ftree, 3,3,3)
    needshash, hashsize = scanner.scan()
    scanner.update_hashes(needshash, hashsize)
    
    scanner.close()
                                 

    
if __name__ == '__main__':
    sys.exit(main())            
            
                
                    

                