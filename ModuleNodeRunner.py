from ConfigLoader import *
from DBConnector import *
from DBUtil import *
from ModuleInfo import *
from MyLogger import *
from NodeExecHandler import *

import os
import shutil
import subprocess

class ModuleNodeRunner(NodeExecHandler):
    def __init__(self, dataTuple):
        NodeExecHandler.__init__(self, dataTuple);
        
    def getNodeInfoFromDB(self):
        self.item = getModuleInfoFromDBByModuleId(self.itemId)

    def execNode(self):
        argStr = self.getFullArgument()
        (stdOut, stdErr) = self.getStdOutErrFileName()
        
        stdOutFile = open(stdOut, 'w')
        stdErrFile = open(stdErr, 'w')
        
        logger.info('[ModuleRunnder]:Exec node %s in exp %s with argument %s' 
                    % (self.nodeId, self.expId, argStr))
        
        self.subProcessThread = subprocess.Popen("exec " + argStr, shell=True, stdout=stdOutFile, stderr=stdErrFile)
        self.subProcessThread.wait()
        
        returnValue = self.subProcessThread.returncode
        
        stdOutFile.flush()
        stdErrFile.flush()
        stdOutFile.close()
        stdErrFile.close()
        
        if(returnValue == 0):
            return True
        else:
            errorMsg = '[ModuleRunnder]:Error in exec node, exit with %d' % (returnValue)
            logger.error(errorMsg)
            raise Exception(errorMsg)
    
    def getStdOutErrFileName(self):
        tempPath = expConfig.expDir + os.sep + self.expId 
        stdOut = tempPath + os.sep + self.expId + '_' + str(self.nodeId) + '_StdOut.txt'
        stdErr = tempPath + os.sep + self.expId + '_' + str(self.nodeId) + '_StdErr.txt'
        return (stdOut, stdErr)
    def getFullArgument(self):
        argStr = self.getCommandArgument() + self.getAndCheckInputArguments()\
            + self.getOutputArguments() + self.getParaArguments()
        return  argStr
    def getAndCheckInputArguments(self):
        results = extractDependNodePortFromDB(self.expId, self.nodeId)
        nowP = 1
        inStr = ''
        for result in results:
            inFileName = self.expId + '_' + str(result[0]) + '_' + str(result[1]) + '.out'
            fullInFilePath = expConfig.expDir + os.sep + self.expId + os.sep + inFileName
            if(not os.path.isfile(fullInFilePath)):
                expMessage = '[ModuleRunnder]:Output file %s not found' % (inFileName)
                logger.error(expMessage)
                raise Exception(expMessage)
            inStr = inStr + '-i' + str(nowP) + ' ' + fullInFilePath + ' '
            nowP += 1
        return inStr     
    def getOutputArguments(self):
        outNum = self.item.getOutArgNumber()
        outStr = ''
        for i in xrange(1, outNum+1):
            outFileName = self.expId + '_' + str(self.nodeId) + '_' + str(i) + '.out'
            fullOutFilePath = expConfig.expDir + os.sep + self.expId + os.sep + outFileName
            outStr = outStr + '-o' + str(i) + ' ' + fullOutFilePath + ' '
        return outStr
    def getParaArguments(self):
        paraNum = self.item.getParaNumber()
        fields = self.parameterStr.split(';')
        if(len(fields) != paraNum+2):
            errMessage = '[ModuleRunnder]:Parameter number mismatch, require:%d, provided:%d'\
                % (paraNum, len(fields)-2)
            raise Exception(errMessage)
        paraStr = ''
        for i in xrange(1, paraNum + 1):
            paraStr = paraStr + '-p' + str(i) + ' ' + "'" + fields[i] + "'" + ' '
        return paraStr
    def getCommandArgument(self):
        fullMainFilePath = self.item.fileLocation
        # Linux binary
        if self.item.moduleTypeId == '0':
            return fullMainFilePath + ' '
        # Python script
        elif self.item.moduleTypeId == '1':
            return 'python ' + fullMainFilePath + ' '
        else:
            errorMessage = '[ModuleRunnder]:Unknown moduleTypeId: %s' % (self.item.moduleTypeId)
            logger.error(errorMessage)
            raise Exception(errorMessage)
def extractDependNodePortFromDB(expId, nodeId):
    sql = '''
            select NodeIdDependBy, PortIdDependBy
            from %s
            where ExpId = '%s' and NodeIdDependOn = '%s'
            order by PortIdDependOn
        ''' % (nodeDepTable, expId, nodeId)
    results = dbConnector.execQuery(sql)
    return results
    