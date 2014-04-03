from MyLogger import *
from ConfigLoader import *
from ModuleInfo import *
from UploadHandler import *

import os
import shutil

class ModuleUploader(UploadHandler):
    def __init__(self, dataTuple):
        UploadHandler.__init__(self, dataTuple)
        self.uploadFolder = submitConfig.moduleDir
        self.targetFolder = storageConfig.moduleDir
    
    def moveFile(self):
        # generate Data folder
        moduleId = self.item.moduleId
        newModuleFolder = self.targetFolder + os.sep + moduleId

        os.mkdir(newModuleFolder)
        logger.info('[ModuleUploader]:create folder:' + newModuleFolder)
        # move module file and all other files into the folder, assume multiple files
        # rename module main file
        mainFileFolder = self.uploadFolder + os.sep + self.fileLocation
        mainFileOldName = self.mainFile
        mainFileNewName = self.item.moduleId + '.bin'
        shutil.move(mainFileFolder + os.sep + mainFileOldName, mainFileFolder + os.sep + mainFileNewName)
        logger.info('[ModuleUploader]:rename module main file from %s to %s:' %\
                    (mainFileOldName, mainFileNewName) )
        # move all files to the target folder
        self.moveFilesFromSrcToDst(mainFileFolder, newModuleFolder)
        logger.info('[ModuleUploader]:Move module files from %s to %s' % (mainFileFolder, newModuleFolder))
    
    
    def moveFilesFromSrcToDst(self, src, dst):
        sourceFolder = os.listdir(src)
        dst += os.sep
        for file in sourceFolder:
            path = os.path.join(src, file)
            shutil.move(path, dst)
    
    def parseJson(self):
        self.item = ModuleInfo()
        # init from json string
        self.item.initFromJsonString(self.itemJson)
        # get new Id
        self.item.getModuleId()
        logger.info('[ModuleUploader]:Parsed new ModuleIndo:' + self.item.toStr())
    def insertItemIntoDB(self):
        # fill in fileLocation
        moduleId = self.item.moduleId
        newModuleFolder = self.targetFolder + os.sep + moduleId
        newFilePath = newModuleFolder + os.sep + moduleId + '.bin';
        self.item.fileLocation = newFilePath
        # insert new data to DB
        insertNewDataIntoDB(self.item)
        logger.info('[ModuleUploader]:Insert new Module to DB:' + self.item.toStr())

def test():
    dataTuple = getAllPendingItems()[0]
    print dataTuple
    temp = ModuleUploader(dataTuple)
    temp.processUpload()

if __name__ == "__main__":
    test()
