from DataUploader import *
from DBUtil import *
from ExpUploader import *
from ModuleUploader import *
from MyLogger import *
from UploadHandler import *

import threading
import time

def getUploadHandle(dataTuple):
    if(dataTuple[1] == 'Data'):
        logger.info('[UploadProcessor]: Get Data Upload Handler')
        return DataUploader(dataTuple)
    elif(dataTuple[1] == 'Module'):
        logger.info('[UploadProcessor]: Get Module Upload Handler')
        return ModuleUploader(dataTuple)
    elif(dataTuple[1] == 'Exp'):
        logger.info('[UploadProcessor]: Get Exp Upload Handler')
        return ExpUploader(dataTuple)
    else:
        logger.error('[UploadProcessor]: Unknown upload item type:'+dataTuple[1])
        return None        

class UploadProcessor(threading.Thread):
    def __init__(self, waitTime = 5):
        threading.Thread.__init__(self)
        self.waitTime = waitTime
    def run(self):
        while(True):
            self.processAllUploads()
            time.sleep(self.waitTime)
    def processAllUploads(self):
        # get all pending items
        pendingItems = getAllPendingItems()
        for dataTuple in pendingItems:
            logger.info('[UploadProcessor]: New upload item found:' + str(dataTuple))
            newHandler = getUploadHandle(dataTuple)
            newHandler.processUpload()