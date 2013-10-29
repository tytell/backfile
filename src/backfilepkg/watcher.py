'''
Created on Dec 4, 2012

@author: eric
'''

import subprocess
import os, sys
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from filetree import FileTree, FileNode, Node

WATCHDIR = '/Users/eric/Eclipse/annex2/tests'

class WatchTree(FileSystemEventHandler):
    """
    Watch a directory tree and keep a record of it in memory
    """
    
    class ChangeHandler(FileSystemEventHandler):
        """Monitor the changes in a directory"""
        def on_any_event(self, event):
            "If anything happens"
            
            print "Event! {0} at {1}".format(event.event_type, event.src_path)

    def __init__(self, watchdir, tree=None, hdffile):
        self.watchdir = watchdir
        

    def on_created(self, event):
                
def main():
    event_handler = ChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, '/Users/eric/Eclipse/annex2', recursive=True)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        
    observer.join()    
    
if __name__ == '__main__':
    sys.exit(main())            
