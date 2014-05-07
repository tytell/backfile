'''
Created on Jun 5, 2012

@author: eric
'''

import os, sys, time, shutil
import random
import hashlib
import subprocess
import h5py

from filetree import DirTree, RootTree, FileNode, get_file_hash, update_hashes

BUF_SIZE = 8192

def build_test_directory(pathname, depth, filesperdir, minfiles=0, dirsperdir=1, filesizes=10*1024, randomize=True):
    if not os.path.exists(pathname):
        os.makedirs(pathname)
    
    ftree = RootTree(name=pathname)
    build_subdirectories(pathname, depth, filesperdir, minfiles, dirsperdir, filesizes, randomize, ftree)
    
    return ftree
    
def fill_file(fid, size, hashfcn=None):
    sz = 0
    while sz < size:
        buf = bytearray([random.randint(0,255) for j in xrange(BUF_SIZE)])
        if size-sz < BUF_SIZE:
            fid.write(buf[:size-sz])
            if hashfcn:
                hashfcn.update(buf[:size-sz])
        else:
            fid.write(buf)
            if hashfcn:
                hashfcn.update(buf)
        sz += BUF_SIZE
    
    return hashfcn

def build_subdirectories(pathname, depth, filesperdir, minfiles, dirsperdir, filesizes, randomize, ftree):
    if randomize:
        nfiles = random.randint(minfiles,filesperdir)
        ndirs = random.randint(0,dirsperdir)
        sizes = [random.randint(0,filesizes) for i in xrange(nfiles)]
    else:
        nfiles = filesperdir
        ndirs = dirsperdir
        sizes = [filesizes] * nfiles
    
    if depth == 0:
        ndirs = 0
    elif depth > 0 and ndirs == 0:
        ndirs = 1
    
    dirname = os.path.basename(pathname)
    letters = 'abcdefghijklmnopqrstuvwxyz'
    for (i,sz) in enumerate(sizes):
        name = '{0}{1:02d}'.format(dirname, i+1)
        with open(os.path.join(pathname, name), 'wb') as fid:
            h = fill_file(fid, sz, hashfcn=hashlib.sha256())

        fnode = FileNode(name=name, parent=ftree, size=sz, modified=time.localtime(), hashval=h.hexdigest())
        ftree.children[name] = fnode
        
    for i in xrange(ndirs):
        name = dirname + letters[i]
        fullname = os.path.join(pathname, name)
        if os.path.exists(fullname):
            os.unlink(fullname)
        os.mkdir(fullname)
        
        subdir = DirTree(name=name, parent=ftree)
        build_subdirectories(fullname, depth-1, filesperdir, minfiles, dirsperdir, filesizes, randomize, subdir)
        ftree.children[name] = subdir

def modify_files(nodes, appendlen, dohash=False, quiet=False):
    for node in nodes:
        if dohash:
            h = get_file_hash(node.abspath())
        else:
            h = None
        if ~quiet:
            print "    {0}".format(node.name)
        with open(node.abspath(), 'ab') as fid:
            h = fill_file(fid, appendlen, hashfcn=h)

def modify_dir(ftree, nmod, nadd, ndel, appendlen=50*1024):
    filenodes = [node for (name,node) in ftree.iternodes() if node.isfile]
    dirnodes = [node for (name,node) in ftree.iternodes() if node.isdir]
    
    print "Modifying..."
    modnodes = random.sample(filenodes, nmod+ndel)
    delnodes = modnodes[nmod:]
    modnodes = modnodes[:nmod]
    modify_files(modnodes, appendlen)
    
    print "Deleting..."
    for delnode in delnodes:
        os.unlink(delnode.abspath())
        print "    {0}".format(delnode.name)
    
    #random selection of dirs, potentially repeating
    print "Adding..."
    adddirs = [random.choice(dirnodes) for i in xrange(nadd)]
    addnames = []
    for (ind, dirnode) in enumerate(adddirs):
        nm = "add{0}".format(ind)
        addname = os.path.join(dirnode.abspath(), nm)
        print "    {0}".format(addname)
        with open(addname, 'ab') as fid:
            fill_file(fid, appendlen)
        addnames.append(addname)
    
    modnames = [node.abspath() for node in modnodes]
    delnames = [node.abspath() for node in delnodes]
    
    print "Done."
    
    return (modnames, delnames, addnames)
    
    
            
    
    
    
def get_hash_openssl(filename):
    out = subprocess.check_output(['openssl','dgst','-sha256',filename])

    if out.startswith('SHA256'):
        (intro, sep, hashval) = out.partition('=')
        hashval = hashval.strip()
        return hashval
    return None
    
    
def check_tree(ftree):
    basepath = ftree.abspath()
    
    isclean = True
    ondisk = {}
    for (root, dirs, files) in os.walk(basepath):
        if root != basepath:
            relpath = os.path.relpath(root, basepath)
            assert(relpath in ftree)
        else:
            relpath = ''
    
        if '.annex' in dirs:
            dirs.remove('.annex')
                
        for d in dirs:
            subpath = os.path.join(relpath, d)
            if subpath not in ftree:
                print "Not in tree: {0} (and subnodes)".format(subpath)
                dirs.remove(d)
                isclean = False
            ondisk[subpath] = None
        for f in files:
            subpath = os.path.join(relpath, f)
            if subpath not in ftree:
                print "Not in tree:", subpath
                isclean = False
            else:
                node = ftree[subpath]
                nodehash = node.hexdigest()
                diskhash = get_hash_openssl(os.path.join(basepath, subpath)).upper()
                ondisk[subpath] = diskhash
                if not diskhash:
                    print "OpenSSL problem: ", subpath
                else:
                    if diskhash != nodehash:
                        print "{0}: Hashes not equal ({1} != {2})".format(subpath, diskhash, nodehash)
                        isclean = False
    
    for (_,node) in ftree.iternodes():
        if node.isfile and node.relpath() not in ondisk:
            print "Not on disk: ", node
            isclean = False
    
    if isclean:
        print "All clean"
        
def main():
    testdir = '/Users/etytel01/Documents/Scanner/backfile/test/testdir1'
    if os.path.exists(testdir):
        shutil.rmtree(testdir)
    ftree0 = build_test_directory(testdir, depth=3, filesperdir=3, minfiles=2, dirsperdir=1,
                                  filesizes=10*1024, randomize=True) 
    
    ftree1 = RootTree()
    ftree1.from_path(testdir, dohash=True)
    
    f = h5py.File('test1.h5', 'w')
    ftree1.to_hdf5(f)
    shutil.copy('test1.h5','test1-init.h5')
    
    modify_dir(ftree1, 3,3,3)
    
    check_tree(ftree1)
    
    needshash = ftree1.update_from_path(testdir)
    update_hashes(needshash)
    
    check_tree(ftree1)
    ftree1.to_hdf5(f)
    
    ftree2 = RootTree(ident=ftree1.id)
    ftree2.from_hdf5(f)
    check_tree(ftree2)
    
    f.close()
    
if __name__ == '__main__':
    sys.exit(main())            
            
        
    
        
            
        