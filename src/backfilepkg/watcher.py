'''
Created on Dec 4, 2012

@author: eric
'''

import subprocess
import os, sys
import time
import shutil
import h5py

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from filetree import RootTree,  DirTree, FileNode, update_hashes
from test import build_test_directory, check_tree, modify_dir

WATCHDIR = '/Users/etytel01/Documents/Scanner/backfile/test'

class FileTreeWatcher(FileSystemEventHandler):
    """Monitor the changes in a directory and record them in a FileTree"""
    def __init__(self, tree):
        self.tree = tree
        
    def on_created(self, event):
        print event.src_path, event.event_type  # print now only for degug
        
        self.tree.add(event.src_path, isdir=event.is_directory, dohash=False)
                    
                
def main():
    testdir = '/Users/etytel01/Documents/Scanner/backfile/test/testdir1'
    if os.path.exists(testdir):
        shutil.rmtree(testdir)
    ftree0 = build_test_directory(testdir, depth=3, filesperdir=3, minfiles=2, dirsperdir=1,
                                  filesizes=10*1024, randomize=True) 
    
    ftree1 = RootTree()
    ftree1.from_path(testdir, dohash=True)
    
    #watcher = FileTreeWatcher(ftree1)
    #observer = Observer()
    #observer.schedule(watcher, path=ftree1.abspath(), recursive=True)
    #observer.start()
    
    f = h5py.File('testwatch.h5', 'w')
    ftree1.to_hdf5(f)
    shutil.copy('testwatch.h5','testwatch-init.h5')
    
    (modnames, delnames, addnames) = modify_dir(ftree1, 3,3,3)
    
    needshash = []
    for name in modnames + addnames:
        needshash += ftree1.update(name, isdir=False, dohash=False)
    update_hashes(needshash)
    
    check_tree(ftree1)
    
#    try:
#        while True:
#            time.sleep(1)
#    except KeyboardInterrupt:
#        watcher.stop()
#        
#    watcher.join()    
    
if __name__ == '__main__':
    sys.exit(main())            
