'''
Created on Nov 27, 2012

@author: eric
'''

import sys
import os, time
import hashlib
import h5py
import numpy as np

from progress import ProgressCLI

TIME_FORMAT = '%Y-%m-%d %H:%M:%S %Z'
HASH_FUNCTION = hashlib.sha256
BLOCK_SIZE = 128 * HASH_FUNCTION().block_size

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

def update_hashes(nodes):
    totalsize = sum([node.size for node in nodes])
    
    with ProgressCLI(unit='sec', total=totalsize) as prog:
        for node in nodes:
            h = get_file_hash(node.abspath(), prog)
            node.hashval = np.frombuffer(h.digest(), dtype=np.uint8, count=h.digest_size)

class Node(object):
    '''
    Hierarchy of files and directories, with some simple data
    '''

    name = None
    parent = None
    children = dict()
    
    @property    
    def isdir(self):
        return self.children is not None
    
    @property
    def isfile(self):
        return self.children is None

    @property
    def isroot(self):
        return self.parent is None
    
    def abspath(self):
        if self.name:
            abspath = [self.name]
        else:
            abspath = []
            
        par = self.parent
        while par:
            abspath.append(par.name)
            par = par.parent
        abspath.reverse()
        
        return '/'.join(abspath)
    
    def relpath(self):
        par = self
        relpath = []
        while par.parent:
            relpath.append(par.name)
            par = par.parent
        relpath.reverse()
        
        return '/'.join(relpath)
    
    def __init__(self, name=None, parent = None):
        self.name = name
        self.parent = parent
        self.children = dict()
        self.isdir = True
        self.isfile = False
                    
    def __str__(self):
        return self.abspath()

    def __getitem__(self, relpath):
        '''
        Search the tree for a node given by the path relpath.
        Returns None if it's not found
        '''
        if (len(relpath) == 0) and not self.parent:
            #empty relpath means they want the top node, which is this one
            return self
        
        if (len(relpath) > 0) and relpath[0] == '/':
            #actually an abspath
            (_, common, relpath) = relpath.partition(self.abspath() + '/')
            if not common:
                raise KeyError
        
        (child, sep, relpath) = relpath.partition('/')
        if not sep and child == self.name:
            return self
        elif child in self.children:
            if not sep:
                return self.children[child]
            else:
                return self.children[child].__getitem__(relpath)
        else:
            raise KeyError
            
    def __contains__(self, relpath):
        if (len(relpath) > 0) and relpath[0] == '/':
            #actually an abspath
            (_, common, relpath) = relpath.partition(self.abspath() + '/')
            if not common:
                return False
            
        (child, sep, relpath) = relpath.partition('/')
        if not sep and child == self.name:
            return True
        elif child in self.children:
            if not sep:
                return True
            else:
                return self.children[child].__contains__(relpath)
        return False
        
    def getroot(self):
        par = self
        while par.parent is not None:
            par = par.parent
        return par
    
    def isorder(self, isord):
        '''
        Check to see if our order = isord. 
        Leaves are order 0, directories with just files are order 1, and so forth
        '''
        if self.children is None:
            return isord == 0
        elif len(self.children) == 0:
            return isord == 1
        else:
            for node in self.children.values():
                if not node.isorder(isord-1):
                    return False
            else:
                return True
            
    def order(self):
        '''
        Compute the order
        which may take a while because it requires descending all
        of the subitems
        '''
        if self.children is None:
            return 0
        
        suborders = [node.order() for node in self.children.values()]
        
        if len(suborders) == 0:
            #directory with no files or subdirs still has order 1
            return 1
        else:
            return max(suborders)+1
    
    def depth(self):
        '''
        Sort of the opposite of order.  Number of levels above
        this node to the root.
        '''
        par = self.parent
        d = 0
        while par:
            d += 1
            par = par.parent
        return d

    def iternodes(self):
        '''
        Walk through the hierarchy and return names and nodes.
        '''
        nodes = [(self.name, self)]
        while nodes:
            curname, curnode = nodes.pop()
            yield((curname, curnode))
            if curnode.children:
                nodes += list(curnode.children.items())

    def walk(self):
        '''Walk through the hierarchy and return nodes.
        Similar to os.walk'''
        nodes = [self]
        root = self
        curnode = self
        while nodes:
            files = []
            dirs = []
            for node in curnode.children.values():
                if node.isdir:
                    dirs.append(node)
                else:
                    files.append(node)
                
            yield((root, dirs, files))
            
            nodes += dirs
            curnode = nodes.pop()
            root = curnode
    
    def to_hdf5(self, h5gp):
        '''Save the tree structure to an HDF5 group'''
        gp1 = h5gp.create_group(self.name)
        if self.children:
            for node in self.children.values():
                node.to_hdf5(gp1)

class FileNode(Node):
    size = 0
    modified = None
    hashval = None
    islink = False

    def __init__(self, name, parent, size=0, modified=None, hashval=None, islink=False):
        self.name = name
        self.parent = parent
        self.size = size
        self.modified = modified
        self.islink = islink
        self.linktarget = None
        self.hashval = hashval
        
        self.children = None
    
    def __eq__(self, other):
        if isinstance(other, FileNode):
            if self.hashval is not None and other.hashval is not None:
                return all(self.hashval == other.hashval)
            else:
                return (self.size == other.size) and (self.modified == other.modified)
        else:
            return False
    
    def __ne__(self, other):
        return not self.__eq__(other)
                
    def __str__(self):
        hd = ''.join("%02X" % n for n in self.hashval)
        return '{0}: hash = {1}'.format(self.abspath(), hd)

    def hexdigest(self):
        if self.hashval is not None:
            return ''.join("%02X" % n for n in self.hashval)
        else:
            return 'None'
            
    def from_file(self, fullpath=None, dohash=False):
        if not fullpath:
            fullpath = self.abspath()
        self.size = os.path.getsize(fullpath)
        self.modified = time.localtime(os.path.getmtime(fullpath))
        if dohash:
            h = get_file_hash(fullpath)
            self.hashval = np.frombuffer(h.digest(), dtype=np.uint8, count=h.digest_size)
        else:
            self.hashval = None

    def update(self, other):
        assert(other.name == self.name)
        self.size = other.size
        self.modified = other.modified
        self.islink = other.islink
        self.linktarget = other.linktarget
        self.hashval = other.hashval
                    
    def to_hdf5(self, h5gp): 
        '''Save the file info to an HDF5 group'''
        gp1 = h5gp.create_group(self.name)
        gp1.attrs["Size"] = self.size
        if self.modified is not None:
            gp1.attrs["Modified"] = np.array(self.modified, dtype='int32')
        
        if self.islink:
            gp1.attrs["IsLink"] = True
            if self.linktarget in self.getroot():
                gp1["InternalLink"] = h5py.SoftLink(self.linktarget)
            else:
                gp1["ExternalLink"] = self.linktarget
        else:
            gp1.attrs["IsLink"] = False
        
        if self.hashval is not None:
            gp1["Hash"] = self.hashval
        
    def from_hdf5(self, h5gp):
        self.size = h5gp.attrs['Size']
        self.modified = time.struct_time(h5gp.attrs['Modified'])
        
        self.islink = h5gp.attrs["IsLink"]
        ## TODO: process links better here
        
        if "Hash" in h5gp:
            self.hashval = np.zeros(HASH_FUNCTION().digest_size, dtype='uint8')
            h5gp["Hash"].read_direct(self.hashval)
        else:
            self.hashval = None
        
class DirTree(Node):
    def __init__(self, name=None, parent=None):
        self.name = name
        self.parent = parent
        self.id = None
        self.children = dict()
                    
    def __eq__(self, other):
        #check that all of our children are present and
        #equal to those in other
        for (name, node) in self.children.iteritems():
            if name not in other.children:
                return False
            elif node == other.children[name]:
                continue
            else:
                return False
        #and the reverse - check that there are no items
        #in other that aren't in our tree
        for (name, node) in other.children.iteritems():
            if name not in self.children:
                return False
        return True

    def __ne__(self, other):
        for (name, node) in self.children:
            if name not in other.children:
                return True
            else:
                if node == other.children[name]:
                    continue
                else:
                    return True
        else:
            return False

    def update(self, other):
        assert(self.name == other.name)

    def get_needs_hash(self):
        needshash = [node for (name, node) in self.iternodes() if node.isfile and node.hashval is None]
        return needshash

    def from_path(self, path, dohash=False, exclude=['.annex']):
        '''
        Builds a file tree based on an existing path
        '''
        for item in os.listdir(path):
            if item in exclude:
                continue
            
            pathname = os.path.join(path,item)
            if os.path.isdir(pathname):
                subdir = DirTree(name=item, parent=self)
                subdir.from_path(pathname, dohash)
                self.children[item] = subdir
            elif os.path.islink(pathname):
                sub = FileNode(name=item, parent=self, islink=True)
                sub.linktarget = os.path.realpath(pathname)
                self.children[item] = sub
            else:
                sub = FileNode(name=item, parent=self)
                sub.from_file(pathname, dohash)
                self.children[item] = sub

    def update_from_path(self, path, exclude=['.annex']):
        '''
        Updates the file tree from a path on disk.
        Returns a list of files that need to be rehashed.
        '''
        
        needshash = []
        for item in os.listdir(path):
            if item in exclude:
                continue
            
            pathname = os.path.join(path, item)
            if os.path.isdir(pathname):
                if (item in self.children) and self.children[item].isdir:
                    needshash += self.children[item].update_from_path(pathname, exclude)
                else:
                    subdir = DirTree(name=item, parent=self)
                    subdir.from_path(pathname, False)                    
                    self.children[item] = subdir
                    needshash += subdir.get_needs_hash()
            elif os.path.islink(pathname):
                sub = FileNode(name=item, parent=self, islink=True)
                sub.linktarget = os.path.realpath(pathname)
                self.children[item] = sub
            else:
                ondisk = FileNode(name=item, parent=self)
                ondisk.from_file(pathname, dohash=False)
                
                if (item not in self.children) or (ondisk != self.children[item]):
                    self.children[item] = ondisk
                    needshash.append(ondisk)
        
        return needshash
                
        
    def from_hdf5(self, h5gp):
        for item in h5gp:
            subgp = h5gp[item]
            if 'InternalLink' in subgp:
                #it's a link
                sub = FileNode(name=item, parent=self, islink=True)
            elif 'Size' in subgp.attrs:
                #it's a file
                sub = FileNode(name=item, parent=self)
                sub.from_hdf5(subgp)
            else:
                sub = DirTree(name=item, parent=self)
                sub.from_hdf5(subgp)
            self.children[item] = sub


                
class RootTree(DirTree):
    def __init__(self, name=None, ident=None):
        self.name = name
        self.parent = None
        self.id = ident
        self.children = dict()

    def make_id(self):
        '''
        Generate a unique identifier.  
        Right now it's a hash of the root path, plus the local time
        '''
        h = HASH_FUNCTION(self.name)
        h.update(time.strftime('%Y%m%d%H%M%S'))
        
        self.id = h.hexdigest()
        
    def to_hdf5(self, h5gp):
        '''Save the tree structure to an HDF5 group'''
        
        assert(self.id is not None)
        
        gp1 = h5gp.create_group(self.id)
        gp1.attrs['RootPath'] = self.name
        
        if self.children:
            for node in self.children.values():
                node.to_hdf5(gp1)

    def from_hdf5(self, h5gp):
        '''
        Builds the file tree from an hdf5 group
        '''
        
        #make sure we're asking for a root group
        assert('RootPath' in h5gp.attrs)
        self.name = h5gp.attrs['RootPath']
        self.id = h5gp.name
        
        super(RootTree, self).from_hdf5(h5gp)

    def from_path(self, path, dohash=False, exclude=['.annex']):
        '''
        Builds a file tree based on an existing path
        '''
        if not self.name:
            self.name = os.path.abspath(path)
        if not self.id:
            self.make_id()
        super(RootTree, self).from_path(path,dohash,exclude)

def main():
    ftree = RootTree()
    ftree.from_path('/Users/eric/Eclipse/annex2/tests/testA')
    
    needshash = [node for (name, node) in ftree.iternodes() if node.isfile and node.hashval is None]
    update_hashes(needshash)
    
    f = h5py.File('test.h5', 'w')
    ftree.to_hdf5(f)
    f.close()

    for (name, node) in ftree.iternodes():
        indent = '   ' * node.depth()
        if node.isfile:
            print "{0}{1} ({2})".format(indent, name, node.hexdigest())
        else:
            print "{0}{1}".format(indent, name)
    
    f2 = h5py.File('test.h5', 'r')
    k = f2.keys()
    
    ftree2 = RootTree()
    ftree2.from_hdf5(f2[k[0]])

    for (name, node) in ftree2.iternodes():
        indent = '   ' * node.depth()
        if node.isfile:
            print "{0}{1} ({2})".format(indent, name, node.hexdigest())
        else:
            print "{0}{1}".format(indent, name)
    

    
if __name__ == '__main__':
    sys.exit(main())            