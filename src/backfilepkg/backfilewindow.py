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
        
        self.initUI()
        
    def initUI(self):
        textEdit = QtGui.QTextEdit()
        self.setCentralWidget(textEdit)

        self.statusBar().showMessage('Ready')
        
        exitAction = QtGui.QAction(QtGui.QIcon.fromTheme("application-exit"), '&Exit', self)
        exitAction.setShortcut('Ctrl+Q')
        exitAction.setStatusTip('Exit application')
        exitAction.triggered.connect(self.close)

        self.statusBar()

        menubar = QtGui.QMenuBar()
        fileMenu = menubar.addMenu('&File')
        fileMenu.addAction(exitAction)
                        
        toolbar = self.addToolBar('Exit')
        toolbar.addAction(exitAction)
        
        self.setGeometry(300, 300, 300, 150)
        self.setWindowTitle('Backfile')    
        
    def choosefile_clicked(self):
        fname = QtGui.QFileDialog.getOpenFileName(self, 'Choose data file', 
                '/home')
        self.datafileEdit.setText(fname)

def main():
    app = QtGui.QApplication(sys.argv)
    wnd = BackfileWindow()
    wnd.show()
    wnd.activateWindow()
    sys.exit(app.exec_())
    

    
if __name__ == '__main__':
    main()

    