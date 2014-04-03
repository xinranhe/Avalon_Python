from ConfigLoader import *
from MyLogger import *
from DBUtil import *

import os
import shutil


class UploadHandler:
    def __init__(self, dataTuple):
        # this depend on different handlers, put empty here
        # should be implemented by different child item uploaders
        self.uploadFolder = ''
        self.targetFolder = ''
        
        self.itemId = dataTuple[0]
        self.fileLocation = dataTuple[2]
        self.mainFile = dataTuple[3]
        self.itemJson = dataTuple[4]
        
        self.item = None
        self.isSuccess = True
        self.message = ''
    
    # main function to handle item upload
    def processUpload(self):
        try:
            # check the existence of main file
            if(not self.isFileExist()):
                raise Exception('Main file not exist')
            self.parseJson()
            self.moveFile()
            self.insertItemIntoDB()
            self.updateStatus('Finished')
            self.isSuccess = True
        except Exception as e:
            self.updateStatus('Failed')
            self.message = str(e)
            self.isSuccess = False
            logger.exception(e)
        finally:
            self.deleteUploadFile()
            pass
    #abstract methods, should be implemented by different upload handlers        
    def moveFile(self):
        raise NotImplementedError("Please Implement this method")    
    def insertItemIntoDB(self):
        raise NotImplementedError("Please Implement this method")
    def parseJson(self):
        raise NotImplementedError("Please Implement this method") 
    # do not throw exception
    def deleteUploadFile(self):
        try:
            uploadPath = self.uploadFolder + os.sep + self.fileLocation
            shutil.rmtree(uploadPath)
            logger.info('[Uploader]:Delete upload folder:'+uploadPath)
        except Exception as e:
            logger.exception(e)
    
    # do not throw exception        
    def updateStatus(self, status):
        try:
            updateStatusAndMessageById(self.itemId, status, self.message)
            logger.info('[Uploader]:update status of %s. Status: %s Msg: %s' % \
                        (self.itemId, status, self.message))
        except Exception as e:
            logger.exception(e)   
    
    def isFileExist(self):
        mainFilePath = self.uploadFolder + os.sep + self.fileLocation + os.sep + self.mainFile
        return os.path.isfile(mainFilePath)