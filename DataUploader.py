
from MyLogger import *
from ConfigLoader import *
from DataInfo import *
from UploadHandler import *

import os
import shutil

class DataUploader(UploadHandler):
    def __init__(self, dataTuple):
        UploadHandler.__init__(self, dataTuple)
        self.uploadFolder = submitConfig.dataDir
        self.targetFolder = storageConfig.dataDir
    
    def moveFile(self):
        # generate Data folder
        dataId = self.item.dataId
        newDataFolder = self.targetFolder + os.sep + dataId
        os.mkdir(newDataFolder)
        logger.info('[DataUploader]:create folder:' + newDataFolder)
        # move data file into the folder, assume single data file
        mainFilePath = self.uploadFolder + os.sep + self.fileLocation + os.sep + self.mainFile
        newFilePath = newDataFolder + os.sep + dataId + '.txt';
        logger.info('[DataUploader]:Move data file from %s to %s' % (mainFilePath, newFilePath))
        shutil.move(mainFilePath, newFilePath)
        
    def parseJson(self):
        self.item = DataInfo()
        # init from json string
        self.item.initFromJsonString(self.itemJson)
        # get new Id
        self.item.getDataId()
        logger.info('[DataUploader]:Parsed new DataIndo:' + self.item.toStr())
    def insertItemIntoDB(self):
        # fill in fileLocation
        dataId = self.item.dataId
        newDataFolder = self.targetFolder + os.sep + dataId
        newFilePath = newDataFolder + os.sep + dataId + '.txt';
        self.item.fileLocation = newFilePath
        # insert new data to DB
        insertNewDataIntoDB(self.item)
        logger.info('[DataUploader]:Insert new Data to DB:' + self.item.toStr())

def test():
    dataTuple = getAllPendingItems()[0]
    print dataTuple
    temp = DataUploader(dataTuple)
    temp.processUpload()

if __name__ == "__main__":
    test()
