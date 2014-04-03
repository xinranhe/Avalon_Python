from DBUtil import *
from MyLogger import *

import threading
import os
import signal

class NodeExecHandler(threading.Thread):
    def __init__(self, dataTuple):
        threading.Thread.__init__(self)
        
        self.expId = dataTuple[0]
        self.nodeId = int(dataTuple[1])
        self.itemId = dataTuple[3]
        self.status = dataTuple[4]
        self.parameterStr = dataTuple[5]
        self.node = None
        self.message = ''
        
        self.runnerStatus = 'New' 
        self.subProcessThread = None
    
    def initNodeRunnder(self):
        isSuccess = False
        try:
            # get node info from DB
            self.getNodeInfoFromDB()
            # update node status to Running
            self.status = 'Running'
            self.message = 'Init node info from DB'
            isSuccess = True
        except Exception as e:
            self.message = str(e)
            self.status = 'Failed'
            logger.exception('[NodeRunnder]:' + str(e))
        finally:
            # update node status
            self.updateNodeStatus()
            return isSuccess
        
    def pauseNodeSubprocess(self):
        os.kill(self.subProcessThread.pid, signal.SIGSTOP)
        self.runnerStatus = 'Pausing'
    def stopNodeSubprocess(self):
        os.kill(self.subProcessThread.pid, signal.SIGKILL)
        self.runnerStatus = 'Aborted'
    
    def resumeNodeSubprocess(self):
        os.kill(self.subProcessThread.pid, signal.SIGCONT)
        self.runnerStatus = 'Running'
    
    def run(self):
        self.runnerStatus = 'Running'
        self.message = 'Running node...'
        self.updateNodeStatus()
        self.processExec();
        self.runnerStatus = 'Finished'
        
    def processExec(self):
        try:
            # exec the extract node
            self.subProcessThread = self.execNode()
            self.status = 'Finished'
            self.message = ''
        except Exception as e:
            self.message = str(e)
            self.status = 'Failed'
            logger.exception('[NodeRunnder]:' + str(e))
        finally:
            # update node status
            self.updateNodeStatus()
            return True
        
    def getNodeInfoFromDB(self):
        raise NotImplementedError("Please Implement this method") 
    
    def execNode(self):
        raise NotImplementedError("Please Implement this method") 
    
    def updateNodeStatus(self):
        updateNodeStatusAndMsgById(self.expId, self.nodeId, self.status, self.message)
        logger.info('[NodeRunnder]:update Node %s_%s: Status:%s Msg:%s' \
                    % (self.expId, self.nodeId, self.status, self.message))