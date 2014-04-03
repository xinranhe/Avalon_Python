from ConfigLoader import *
from DBUtil import *
from ExpInfo import *
from UploadHandler import *

class ExpUploader(UploadHandler):
    def __init__(self, dataTuple):
        UploadHandler.__init__(self, dataTuple)
        self.uploadFolder = submitConfig.expDir
        
    def parseJson(self):
        self.item = ExpInfo()
        self.item.expId = getUniqueExpId()
        self.item.initFromJsonString(self.itemJson)
        logger.info('[ExpUploader]: Parsed new experiment:\n' + self.item.toStr())
    def insertItemIntoDB(self):
        insertNewExpIntoDB(self.item);
        logger.info('[ExpUploader]: Insert new experiment to DB:\n' + self.item.toStr())

    def moveFile(self):
        # for experiment, currently just create experiment folder
        newExpFolder = expConfig.expDir + os.sep + self.item.expId
        os.mkdir(newExpFolder)
        logger.info('[ExpUploader]:create folder:' + newExpFolder)
        
        mainFilePath = self.uploadFolder + os.sep + self.fileLocation + os.sep + self.mainFile
        newFilePath = newExpFolder + os.sep + self.item.expId + '_vis.json';
        logger.info('[ExpUploader]:Move experiment vis file from %s to %s' % (mainFilePath, newFilePath))
        shutil.move(mainFilePath, newFilePath)

def test():
    dataTuple = getAllPendingItems()[0]
    print dataTuple
    temp = ExpUploader(dataTuple)
    temp.processUpload()

if __name__ == "__main__":
    test()