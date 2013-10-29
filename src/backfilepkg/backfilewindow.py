# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 11:47:08 2013

@author: etytel01
"""

import sys
from PyQt4 import QtGui, QtCore

class BackfileWindow(QtGui.QDialog):
    '''
    Base window for the Backfile program
    '''
    
    def __init__(self):
        super(BackfileWindow, self).__init__()
        
        self.initUI()
        
    def initUI(self):
        self.datafileEdit = QtGui.QLineEdit()
        choosefileButton = QtGui.QPushButton("Browse")
        
        doneButton = QtGui.QPushButton("Done")

        grid = QtGui.QGridLayout()
        grid.addWidget(self.datafileEdit,0,0)
        grid.addWidget(choosefileButton,0,1)
        grid.addWidget(doneButton,1,1, alignment=QtCore.Qt.AlignRight)

        doneButton.clicked.connect(self.done)
        choosefileButton.clicked.connect(self.choosefile_clicked)
        
        self.setLayout(grid)
        
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
    app.exec_()
    

    
if __name__ == '__main__':
    main()

    