from ConfigLoader import *
from DataInfo import *
from MyLogger import *
from NodeExecHandler import *

import os
import shutil
import subprocess

class DataNodeRunner(NodeExecHandler):
    def __init__(self, dataTuple):
        NodeExecHandler.__init__(self, dataTuple)
        
    def getNodeInfoFromDB(self):
        self.item = getDataInfoFromDBByDataId(self.itemId)
    
    def execNode(self):
        # copy file to local exp folder
        expDataFileName = self.expId + '_' + str(self.nodeId) + '_1.out'
        outFileFullPath = expConfig.expDir + os.sep \
                        + self.expId + os.sep + expDataFileName
        inFileFullPath = self.item.fileLocation
        
        fullCommand = 'cp ' + inFileFullPath + ' ' + outFileFullPath
        
        if(os.path.isfile(inFileFullPath)):
            logger.info('[DataNodeRunnder]:Copy Data file from %s to %s' % (inFileFullPath, outFileFullPath))
            self.subProcessThread = subprocess.Popen("exec " + fullCommand, shell=True)
            self.subProcessThread.wait()
        else:
            exceptMsg = '[DataNodeRunnder]:Data file %s not exists' % (inFileFullPath)
            logger.error(exceptMsg)
            raise Exception(exceptMsg)