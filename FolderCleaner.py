'''

@author: xinranhe
'''
from ConfigLoader import *
from DBUtil import *
from MyLogger import *

import os
import shutil
import threading
import time

class FolderCleaner(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.waitTime = 60
    
    def run(self):
        while True:
            self.cleanUpData()
            self.cleanUpExp()
            self.cleanUpModule()
            cleanPendingItem()
            time.sleep(self.waitTime)
    def cleanUpExp(self):
        path = expConfig.expDir
        allFolders = [name for name in os.listdir(path)
            if os.path.isdir(os.path.join(path, name))]
        expIdList = getAllExpId()
        for expFolder in allFolders:
            if expFolder not in expIdList:
                logger.info('[FolderCleaner]: Clean up deleted Exp: %s' % (expFolder))
                shutil.rmtree(os.path.join(path, expFolder))
    
    def cleanUpData(self):
        path = storageConfig.dataDir
        allFolders = [name for name in os.listdir(path)
            if os.path.isdir(os.path.join(path, name))]
        dataIdList = getAllDataId()
        for dataFolder in allFolders:
            if dataFolder not in dataIdList:
                logger.info('[FolderCleaner]: Clean up deleted Data: %s' % (dataFolder))
                shutil.rmtree(os.path.join(path, dataFolder))
    
    def cleanUpModule(self):
        path = storageConfig.moduleDir
        allFolders = [name for name in os.listdir(path)
            if os.path.isdir(os.path.join(path, name))]
        moduleIdList = getAllModuleId()
        for moduleFolder in allFolders:
            if moduleFolder not in moduleIdList:
                logger.info('[FolderCleaner]: Clean up deleted Module: %s' % (moduleFolder))
                shutil.rmtree(os.path.join(path, moduleFolder))

def test():
    cleaner = FolderCleaner()
    cleaner.run()
    
if __name__ == "__main__":
    test()
