# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 11:47:08 2013

@author: etytel01
"""

import sys
from PySide import QtGui

class BackfileWindow(QtGui.QMainWindow):
    '''
    Base window for the Backfile program
    '''
    
    def __init__(self):
        super(BackfileWindow, self).__init__()
            
        textEdit = QtGui.QTextEdit()
        self.setCentralWidget(textEdit)

        self.statusBar().showMessage('Ready')
        
        self.createActions()
        self.createMenus()
        
        toolbar = self.addToolBar('Main')
        toolbar.addAction(self.addScanAct)
               
        self.setGeometry(300, 300, 300, 150)
        self.setWindowTitle('Backfile')    

    def newCatalog(self):
        fname = QtGui.QFileDialog.getSaveFileName(self, 'Choose datafile',
                                                  filter=self.tr("Catalog files (*.h5)"))        
        self.catalogName = fname;
        
    def openCatalog(self):
        fname = QtGui.QFileDialog.getOpenFileName(self, 'Choose datafile',
                                                  '/home')        
        self.catalogName = fname;

    def addScan(self):
        self.statusBar().showMessage('Add scan')

    def aboutBackfile(self):
        self.statusBar().showMessage('About')
        
    def createActions(self):
        self.newAct = QtGui.QAction("&New catalog...", self,
                shortcut=QtGui.QKeySequence.New,
                statusTip="Create a new catalog", triggered=self.newCatalog)
        
        self.openAct = QtGui.QAction("&Open catalog...", self,
                shortcut=QtGui.QKeySequence.Open,
                statusTip="Open an existng catalog", triggered=self.openCatalog)
        
        self.addScanAct = QtGui.QAction("&Add scan directory...", self,
                statusTip="Create a new catalog", triggered=self.addScan)
        
        self.exitAct = QtGui.QAction("&Quit", self, shortcut=QtGui.QKeySequence.Quit,
                statusTip="Quit the application", triggered=self.close)

        self.aboutAct = QtGui.QAction("About &Backfile", self,
                statusTip="Show the Backfile About box",
                triggered=self.aboutBackfile)

    def createMenus(self):
        self.fileMenu = self.menuBar().addMenu("&File")
        self.fileMenu.addAction(self.newAct)
        self.fileMenu.addAction(self.openAct)
        self.fileMenu.addSeparator()
        self.fileMenu.addAction(self.addScanAct)
        self.fileMenu.addSeparator()
        self.fileMenu.addAction(self.exitAct)
        
        self.helpMenu = self.menuBar().addMenu("&Help")
        self.helpMenu.addAction(self.aboutAct)
        
def main():
    app = QtGui.QApplication(sys.argv)
    wnd = BackfileWindow()
    wnd.show()
    wnd.activateWindow()
    sys.exit(app.exec_())
    

    
if __name__ == '__main__':
    main()

    