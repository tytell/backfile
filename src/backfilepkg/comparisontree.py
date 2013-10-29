'''
Created on May 21, 2012

@author: eric
'''

import sys, time
from filetree import Node, FileNode, DirTree, TIME_FORMAT
from collections import defaultdict

class ComparisonNode(Node):
    '''
    Hierarchical structure to compare two trees.
    '''
    
    state = None
    nodes = None
    
    def __init__(self, name=None, parent=None, state=None, nodes=None):
        self.name = name
        self.parent = parent
        self.state = state
        self.nodes = nodes
        self.children = dict()
        
    def compare(self, base,cur):
        '''
        Compares trees base and cur to each other.
        Does not look for moved files/directories
        '''
        if not cur:
            self.name = base.name
            self.nodes = (base, None)
            self.state = 'removed'
            if base.children is None:
                self.children = None
            else:
                for (name, node) in base.children.iteritems():
                    comp1 = ComparisonNode(name, parent=self, nodes=(node, None), state='removed')
                    comp1.compare(node, None)
                    self.children[name] = comp1
        elif not base:
            self.name = cur.name
            self.nodes = (None, cur)
            self.state = 'added'
            if cur.children is None:
                self.children = None
            else:
                for (name, node) in cur.children.iteritems():
                    comp1 = ComparisonNode(name, parent=self, nodes=(None, node), state='added')
                    comp1.compare(None, node)
                    self.children[name] = comp1
        else:
            self.name = base.name
            self.nodes = (base, cur)
            
            if base.isfile and cur.isfile:
                self.children = None
                if base == cur:
                    self.state = 'clean'
                else:
                    self.state = 'modified'
            elif base.isdir != cur.isdir:
                self.state = 'typechange'
                
                if base.children:
                    for (name, node) in base.children.iteritems():
                        comp1 = ComparisonNode(name, parent=self, nodes=(node, None), state='removed')
                        self.children[name] = comp1
                
                if cur.children:
                    for (name, node) in cur.children.iteritems():
                        comp1 = ComparisonNode(name, parent=self, nodes=(None, node), state='added')
                        self.children[name] = comp1
            else:
                for (name, node) in base.children.iteritems():
                    comp1 = ComparisonNode(name=name, parent=self)
                    if name in cur.children:
                        comp1.compare(node, cur.children[name])
                    else:
                        comp1.compare(node, None)
                    self.children[name] = comp1
                
                for (name, node) in cur.children.iteritems():
                    if name not in base.children:
                        comp1 = ComparisonNode(name=name, parent=self)
                        comp1.compare(None, node)
                        self.children[name] = comp1

    def get_update_hashes(self, checkhashes, checkmoved):
        '''Get file nodes that need to have hash values calculated'''
        basehash = set()
        righthash = set()
        if checkmoved or checkhashes in ['all','clean']:
            for (name, node) in self.iternodes():
                ishash = [n and n.isfile and bool(n.hashval) for n in node.nodes]
            
                if not all(ishash):
                    if checkmoved and (node.state == 'removed'):
                        basehash.add(node.nodes[0])
                    elif checkmoved and (node.state == 'added'):
                        righthash.add(node.nodes[1])
                    elif (checkhashes == 'all') or ((checkhashes == 'clean') and (node.state == 'clean')):
                        if ishash[0]:
                            basehash.add(node.nodes[0])
                        if ishash[1]:
                            righthash.add(node.nodes[1])                    
        
        return (basehash, righthash)
    
    def compare_again(self):
        '''
        Check the comparison again for each file.  Useful if the hashes have been updated.
        '''
        for (name, node) in self.iternodes():
            if node.nodes[0] and node.nodes[1] and node.nodes[0].isfile and node.nodes[1].isfile:
                if node.nodes[0] == node.nodes[1]:
                    self.state = 'clean'
                else:
                    self.state = 'modified'
                    
    def check_moved(self):
        #directories are bit trickier, because we don't have a simple measure of
        #equality
        
        #track down the order 1 directories
        basedirs = []
        curdirs = []
        for (_, dirs, _) in self.walk():
            basedirs += [node for node in dirs if node.isorder(1) and node.state == 'removed']
            curdirs += [node for node in dirs if node.isorder(1) and node.state == 'added']

        #first we'll compare the order 1 directories, then their parents, and so forth
        for based in basedirs:
            curmatch = None
            for curd in curdirs:
                (basepar, curpar) = (based, curd)
                basematch = None
                ae = None
                while basepar is not curpar and basepar.nodes[0].checkequal(curpar.nodes[1], ae):
                    basematch = basepar
                    ae = basematch.nodes[0]
                    curmatch = curpar
                    basepar = basepar.parent
                    curpar = curpar.parent
                if curmatch:
                    break

            if curmatch:
                curdirs.remove(curd)
                
                matchdir = MovedNode(nodes=(basematch.nodes[0], curmatch.nodes[1]), oldparent=basematch.parent, newparent=curmatch.parent)
                matchdir.children = basematch.children
                for ((basename, basecomp), (curname, curcomp)) in zip(basematch.iternodes(), curmatch.iternodes()):
                    if basecomp is not basematch:
                        basecomp.state = 'clean'
                        basecomp.nodes = (basecomp.nodes[0], curcomp.nodes[1])
                        
                basepar.children[basematch.name] = matchdir
                del(curpar.children[curmatch.name])
                    
        #now look for moved/renamed files/dirs
        basehashes = defaultdict(list)
        curhashes = defaultdict(list)
        for (name, comp) in self.iternodes():
            if comp.state == 'removed' and comp.isfile: 
                basehashes[comp.nodes[0].hashval].append(comp)
            elif comp.state == 'added' and comp.isfile:
                curhashes[comp.nodes[1].hashval].append(comp)
        
        for (hashval, comp) in basehashes.iteritems():
            if (hashval in curhashes) and (len(comp) == 1) and (len(curhashes[hashval]) == 1):
                basenode = comp[0]
                curnode = curhashes[hashval][0]
                matchfile = MovedNode((basenode.nodes[0], curnode.nodes[1]), basenode.parent, curnode.parent)
                
                basenode.parent.children[basenode.name] = matchfile
                del(curnode.parent.children[curnode.name])
    
    def update_base(self, remove=False, add=False):
        basetree = self.nodes[0]
        if self.state == 'added':
            basetree = self.nodes[1].duplicate()
        else:
            for (root, dirs, files) in self.walk():
                par = basetree[root.relpath()]
                nodes = dirs + files
                
                for node in nodes:
                    name = node.name
                    
                    if node.state == 'moved':
                        basenode = node.nodes[0]
                        #could be a rename
                        basenode.name = node.newname
                        
                        #or a move (or both)
                        oldparent = par
                        newparent = basetree[node.newparent.relpath()] 
                        if oldparent is not newparent:
                            del(oldparent.children[name])
                            newparent.children[node.newname] = basenode
                            basenode.parent = newparent
                    elif node.state == 'typechange':
                        n = node.nodes[1].duplicate()
                        n.parent = par
                        par.children[name] = n
                    elif add and node.state == 'added':
                        n = node.nodes[1].duplicate()
                        n.parent = par
                        par.children[name] = n
                        if node.isdir:
                            #all of the elements below an added directory are added also
                            #and duplicate copies them all, so we don't need to walk into the directory
                            dirs.remove(node)
                    elif remove and node.state == 'removed':
                        del(par[name])
                    elif node.state == 'modified':
                        basenode = node.nodes[0]
                        basenode.update(node.nodes[1])
        return basetree
    
    def get_modified_detail(self, node):
        if node.nodes[0].hashval and node.nodes[1].hashval:
            detail1 = 'hashes not equal ({0} != {1})'.format(node.nodes[0].hashval, node.nodes[1].hashval)
        elif node.nodes[0].size != node.nodes[1].size:
            detail1 = 'sizes not equal ({0} != {1})'.format(node.nodes[0].size, node.nodes[1].size)
        else:
            detail1 = 'modified at different times ({0} != {1})'.format(time.strftime(TIME_FORMAT, node.nodes[0].modified),
                                                                       time.strftime(TIME_FORMAT, node.nodes[1].modified))
        return detail1
    
    def get_status(self):
        status = defaultdict(list)
        for (_, node) in self.iternodes():
            if node.state == 'moved':
                detail1 = 'from {0} to {1}'.format(node.nodes[0].relpath(), node.nodes[1].relpath())
            elif node.state == 'typechange':
                if node.nodes[0].isdir:
                    detail1 = 'from directory to file'
                else:
                    detail1 = 'from file to directory'
            elif node.state == 'modified':
                detail1 = node.get_modified_detail(node)
            else:
                detail1 = ''
                
            if node.state == 'added':
                relpath = node.nodes[1].relpath()
            else:
                relpath = node.nodes[0].relpath()
                
            if node.state:
                status[node.state].append((relpath, detail1))
        
        return status                                                 

class ComparisonTree(ComparisonNode):
    '''
    Wrapper class to set up comparisons
    '''
    def __init__(self, base,cur):
        self.name = None
        self.parent = None
        self.state = None
        self.nodes = [base,cur]
        self.children = dict()

        self.compare(base, cur)
             
class MovedNode(Node):
    state = 'moved'
    newname = None
    newparent = None
    nodes = None

    def __init__(self, nodes, oldparent, newparent):
        self.name = nodes[0].name
        self.newname = nodes[1].name
        self.parent = oldparent
        self.newparent = newparent
        self.state = 'moved'
        self.nodes = nodes
        
        if nodes[0].isfile:
            assert(nodes[1].isfile)
            self.children = None
        else:
            self.children = {}
    
def main():
    ondisk = RootTree()
    ondisk.from_path('/Users/eric/Eclipse/annex2/tests')

    Atree = FileTree()
    Atree.from_xml(filename='/Users/eric/Eclipse/annex2/tests/testA.xml')
        
    Btree = FileTree()
    Btree.from_xml(filename='/Users/eric/Eclipse/annex2/tests/testB.xml')
        
    Otree = FileTree()
    Otree.from_xml(filename='/Users/eric/Eclipse/annex2/tests/testO.xml')
    
    left = ComparisonTree(Otree, Atree)
    left.check_moved()
    
    for (name, node) in left.iternodes():
        indent = '   ' * node.depth()
        print indent, name, ': ', node.state
        if node.state == 'moved':
            print indent, '  (from {0} to {1})'.format(node.nodes[0].relpath(), node.nodes[1].relpath())
            
    right = ComparisonTree(Otree, Btree)
    right.check_moved()
    
    for (name, node) in right.iternodes():
        indent = '   ' * node.depth()
        print indent, name, ': ', node.state
        if node.state == 'moved':
            print indent, '  (from {0} to {1})'.format(node.nodes[0].relpath(), node.nodes[1].relpath())
            
    (update, conflicts) = merge(left, right)
    
    for (i, side) in enumerate(['LEFT','RIGHT']):
        print side
        for (cmd, nodes) in update[i].items():
            print '  ', cmd
            for (base, cur, remote) in nodes:
                print '    ', 
                if base:
                    print '{0}: '.format(base.relpath()),
                     
                if remote:
                    print 'from {0}'.format(remote.relpath())
                else:
                    print ''
    
    print 'CONFLICTS'
    for (relpath, (states, nodes)) in conflicts.iteritems():
        print '   {0}: left state = {1}, right state = {2}'.format(relpath, states[0],states[1])

    
if __name__ == '__main__':
    sys.exit(main())            
            