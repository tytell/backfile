# -*- coding: utf-8 -*-
"""
Created on Thu May  8 09:57:49 2014

@author: etytel01
"""

import os, time, shutil
import sys
import h5py
import multiprocessing as mp
import numpy as np
import hashlib
import logging
import mimetypes
#import magic       # problems on windows

from thumbnail import get_thumbnail
from filetree import RootTree
from progress import ProgressCLI
from test import build_test_directory, modify_dir

import filedataglobal

TIME_FORMAT = '%Y-%m-%d %H:%M:%S %Z'
HASH_FUNCTION = hashlib.sha256
BLOCK_SIZE = 128 * HASH_FUNCTION().block_size
POOL_SIZE = 4
PARALLEL = True

def init_pool(status):
    filedataglobal.status = status

class FileType:
    File, Dir, Link = range(3)
    
    @staticmethod
    def get_name(num):
        name = [nm1 for (nm1,num1) in FileType.__dict__.iteritems() if num1 == num]
        if len(name) == 1:
            return name[0]
        else:
            raise TypeError
            
    @staticmethod
    def get_num(name):
        return FileType.__dict__[name]
        
class FileData(object):
    '''
    Data on a file
    '''
    
    name = None
    fullpath = None
    root = None
    
    def __init__(self, fullpath, ftype=None, size=None, modified=None, hashval=None, thumbnail=None, mimetype=None):
        self.fullpath = fullpath
        
        (_,name) = os.path.split(fullpath)
        self.name = name
        self.size = size
        self.modified = modified
        self.hashval = hashval
        self.thumbnail = thumbnail
        self.type = ftype
        self.mimetp = mimetype
        self.isthumbnail = False
        
    def __str__(self):
        if self.type == FileType.Dir:
            return '{0} (dir)'.format(self.name)
        elif self.type == FileType.Link:
            return '{0} (link)'.format(self.name)
        else:
            return '{0}: size = {1}; hash = {2}'.format(self.name, self.size, self.hexdigest())

    def hexdigest(self):
        if self.hashval is not None:
            return ''.join("%02X" % n for n in self.hashval)
        else:
            return 'None'

    def __eq__(self, other):
        if self.type != other.type:
            return False
        elif self.type == FileType.Dir or self.type == FileType.Link:
            return other.name == self.name
        else:
            if self.hashval is not None and other.hashval is not None:
                return all(self.hashval == other.hashval)
            else:
                return (other.name == self.name) and (self.size == other.size) and (self.modified == other.modified)            
    
    def __ne__(self, other):
        return not self.__eq__(other)
            
    def from_path(self):
        if os.path.isdir(self.fullpath):
            self.type = FileType.Dir
        elif os.path.islink(self.fullpath):
            self.type = FileType.Link
        else:
            self.type = FileType.File
            self.size = os.path.getsize(self.fullpath)
            self.modified = time.localtime(os.path.getmtime(self.fullpath))
            (self.mimetp,enc) = mimetypes.guess_type(self.fullpath)
            

    def from_hdf5(self, h5gp):
        self.type = FileType.get_num(h5gp.attrs['Type'])        
        if self.type == FileType.File:
            if 'Size' in h5gp.attrs:
                self.size = h5gp.attrs['Size']
            if 'Modified' in h5gp.attrs:
                self.modified = time.struct_time(h5gp.attrs['Modified'])
            if 'MimeType' in h5gp.attrs:
                self.mimetp = h5gp.attrs['MimeType']
                
            #self.islink = h5gp.attrs["IsLink"]
            ## TODO: process links better here
            
            if "Hash" in h5gp:
                self.hashval = np.zeros(HASH_FUNCTION().digest_size, dtype='uint8')
                self.hashval = h5gp["Hash"][:,-1]
            else:
                self.hashval = None

            if 'Thumbnail' in h5gp:
                self.isthumbnail = True
                
class FileDataStatus(object):
    '''
    Shared object for status of writing data to file
    '''        
    def __init__(self):
        self.lock = mp.Lock()
        self.statebuf    = mp.Array('c',256, lock=self.lock)
        self.totalbytes  = mp.Value('i', lock=self.lock)
        self.curbytes    = mp.Value('i', lock=self.lock)
            
    def setstatus(self, state=None, total=None, cur=None, curfile=None):
        if state:
            assert(len(state) < 256)
            self.statebuf.value = state
        if total:
            self.totalbytes.value = total
        if cur:
            self.curbytes.value = cur
        
    def getstatus(self):
        return (self.statebuf.value, self.curbytes.value, self.totalbytes.value)
    
    def incbytes(self, inc):
        self.curbytes.value = self.curbytes.value + inc

def get_file_hash(filename):
    logging.debug('Hashing %s',filename)
    
    h = HASH_FUNCTION()
    with open(filename, 'rb') as fid:
        def readblock():
            return fid.read(BLOCK_SIZE)
        
        for b in iter(readblock,''):
            h.update(b)
            if filedataglobal.status:
                filedataglobal.status.incbytes(BLOCK_SIZE)    
                
    return filename, np.frombuffer(h.digest(), dtype=np.uint8, count=h.digest_size)

def get_file_thumbnail(filename):
    logging.debug('Thumbnail for %s',filename)
    
    t = get_thumbnail(path=filename)
    if t is not None:
        try:
            t.from_file()
        except:
            t = None
    if filedataglobal.status:
        filedataglobal.status.incbytes(1)
    
    return filename, t
    
class FileDataWriter(mp.Process):
    '''
    Writes file data to a log file
    '''

    def __init__(self, outfile, rootpath=None, status=None):
        super(FileDataWriter, self).__init__()
        self.outfile = outfile
        self.rootpath = rootpath
        self.status = status

    def run(self):
        logging.debug('In run')
        
        try:
            if os.path.isfile(self.outfile):
                self.filename = self.outfile
                self.open_file(self.outfile)
            else:
                self.init_file(self.outfile, self.rootpath)
            
            self.scan()
        finally:
            self.close()
        
    def open_file(self, filename):
        self.h5file = h5py.File(filename, 'a')
        
        self.rootgp = self.h5file.require_group('ROOT')
        assert((self.rootpath is None) or (self.rootpath == self.rootgp.attrs['RootPath']))
        
        self.rootpath = self.rootgp.attrs['RootPath']
        assert(self.rootgp.attrs['Type'] == FileType.get_name(FileType.Dir))
        assert(os.path.exists(self.rootpath))
        self.deletedgp = self.h5file.require_group('DELETED')
        
    def init_file(self, filename, rootpath):
        assert(rootpath is not None)
        
        self.h5file = h5py.File(filename, 'w')
        self.rootpath = os.path.abspath(rootpath)
        
        self.rootgp = self.h5file.create_group('ROOT')
        self.rootgp.attrs['RootPath'] = self.rootpath
        self.rootgp.attrs['Type'] = FileType.get_name(FileType.Dir)
        
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
            
        if fd.type == FileType.Dir:
            if 'Type' in gp.attrs and gp.attrs['Type'] != FileType.get_name(FileType.Dir):
                #it's actually a file
                self.make_deleted(fd.name, parentgp, fd.fullpath)
                gp = self.rootgp.create_group(relpath)
            if fd.type is not None:
                gp.attrs['Type'] = FileType.get_name(fd.type)
        else:
            if fd.type is not None:             
                gp.attrs['Type'] = FileType.get_name(fd.type)
            if fd.type == FileType.Link:
                pass
            else:
                if fd.size is not None:
                    gp.attrs['Size'] = fd.size
                if fd.modified is not None:
                    gp.attrs["Modified"] = np.array(fd.modified, dtype='int32')
                if fd.mimetp is not None:
                    gp.attrs['MimeType'] = fd.mimetp
                if fd.hashval is not None:
                    self.write_hash(gp, fd)
                if fd.thumbnail is not None:
                    fd.thumbnail.to_hdf5(gp)
        
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
            logging.debug('Writing new hash at %d',ind)
            hset[:,ind] = fd.hashval
        dateset[:,ind] = np.array(time.localtime(), dtype='int16')
        
    def make_deleted(self, name, parentgp, fullpath):
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
            hashgp1[nm].attrs['OriginalPath'] = fullpath
            
            del parentgp[name]
        else:
            del parentgp[name]
            
    def scan(self):
        '''
        Scan the whole path and compare/update the HDF5 tree
        '''
        assert(self.rootpath is not None)
        assert(os.path.exists(self.rootpath))
        
        logging.debug("in scan")
        
        if self.status:
            self.status.setstatus(state='Initial scan')
            
        #walk through the directory tree
        needshash = []
        hashsize = 0
        needsthumb = []
        thumbsize = 0
        for maindir,subdirs,files in os.walk(self.rootpath):
            fd = FileData(maindir, ftype=FileType.Dir)
            
            gp = self.write_data(fd)
            deleted = set(gp.keys())

            for subdir in subdirs:
                subfd = FileData(os.path.join(maindir,subdir), ftype=FileType.Dir)
                self.write_data(subfd, parentgp=gp)
                deleted.discard(subdir)
                
            for filename in files:
                ondisk = FileData(os.path.join(maindir, filename))
                ondisk.from_path()
            
                if filename in gp:
                    infile = FileData(os.path.join(maindir, filename))
                    infile.from_hdf5(gp[filename])
                    
                    if ondisk != infile:
                        logging.debug('%s != %s',str(ondisk),str(infile))
                        self.write_data(ondisk,parentgp=gp)
                        if ondisk.type == FileType.File:
                            needshash.append(ondisk.fullpath)
                            hashsize += ondisk.size
                            needsthumb.append(ondisk.fullpath)
                            thumbsize += ondisk.size
                    elif ondisk.type == FileType.File:
                        #it's the same in the file as on disk, but we didn't
                        #get a chance to calculate a hash or a thumbnail
                        if infile.hashval is None:
                            needshash.append(ondisk.fullpath)
                            hashsize += ondisk.size
                        if not infile.isthumbnail:
                            needsthumb.append(ondisk.fullpath)
                            thumbsize += ondisk.size
                else:
                    self.write_data(ondisk, parentgp=gp)
                    if ondisk.type == FileType.File:
                        needshash.append(ondisk.fullpath)
                        hashsize += ondisk.size
                        needsthumb.append(ondisk.fullpath)
                        thumbsize += ondisk.size
                deleted.discard(filename)

            for name in deleted:
                self.make_deleted(name, gp, os.path.join(maindir, name))
        
        if self.status:
            self.status.setstatus(state='Computing hashes',total=hashsize,cur=0)

        logging.debug('Done with scan: %d files / %d bytes to hash',
                      len(needshash),hashsize)
                      
        if PARALLEL:
            hash_pool = mp.Pool(POOL_SIZE, initializer=init_pool,initargs=(self.status,))
            hash_map = dict(hash_pool.imap_unordered(get_file_hash, needshash, 10))            
            hash_pool.close()
        else:
            hash_map = dict([get_file_hash(fn) for fn in needshash])
        
        if self.status:
            self.status.setstatus(state='Writing hashes')
            
        for (filename, h) in hash_map.iteritems():
            fd = FileData(filename, hashval=h)
            self.write_data(fd)

        if self.status:
            self.status.setstatus(state='Generating thumbnails',total=len(needsthumb),cur=0)
    
        if PARALLEL:
            thumb_pool = mp.Pool(POOL_SIZE, initializer=init_pool,initargs=(self.status,))
            thumb_map = dict(thumb_pool.imap_unordered(get_file_thumbnail, needsthumb, 10))            
            thumb_pool.close()
        else:
            thumb_map = dict([get_file_thumbnail(fn) for fn in needsthumb])
            
        if self.status:
            self.status.setstatus(state='Writing thumbnails')
            
        for (filename, t) in thumb_map.iteritems():
            fd = FileData(filename, thumbnail=t)
            self.write_data(fd)
        
def main():
    logging.basicConfig(level=logging.DEBUG)
    
    #testdir = '/Users/etytel01/Documents/Scanner/backfile/test/testdir1'
    if os.name == 'nt':
        testdir = "E:\\"
        outfile =  r"C:\Users\etytel01\Documents\backfile\test\Tytell22.h5"
    else:
        testdir = '/Users/etytel01/Documents/Scanner/backfile/test/testthumbs'
        outfile = '/Users/etytel01/Documents/Scanner/backfile/test/newtest.h5'
    #if os.path.exists(testdir):
    #    shutil.rmtree(testdir)
    #if os.path.exists(outfile):
    #    os.unlink(outfile)
    
    
    #ftree = build_test_directory(testdir, depth=3, filesperdir=3, minfiles=2, dirsperdir=1,
    #                              filesizes=10*1024, randomize=True) 

    if PARALLEL:
        status = FileDataStatus()   
    else:
        status = None
    filedataglobal.status = status
    
    scanner = FileDataWriter(outfile, testdir, status=status)
    
    if PARALLEL:
        scanner.start()

        try:
            while scanner.is_alive():
                (state, curbytes, totalbytes) = status.getstatus()        
                print '{0}: {1}/{2}'.format(state, curbytes, totalbytes)
                time.sleep(0.5)
        except KeyboardInterrupt:
            scanner.terminate()
        scanner.join()
    else:
        scanner.run()
        
    print 'Done'

    
if __name__ == '__main__':
    sys.exit(main())            
            
                
                    

                