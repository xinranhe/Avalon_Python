from DataNodeRunner import *
from DBUtil import *
from ModuleNodeRunner import *
from MyLogger import *

import threading
import time

def getNodeRunnerByType(dataTuple):
    nodeType = dataTuple[2]
    if nodeType == 'Data':
        logger.info('[ExecProcessor]:Get Node Runner for:' + str(dataTuple))
        return DataNodeRunner(dataTuple)
    elif nodeType == 'Module':
        logger.info('[ExecProcessor]:Get Module Runner for:' + str(dataTuple))
        return ModuleNodeRunner(dataTuple)
    else:
        logger.error('[ExecProcessor]:Unknown node type:' + nodeType)
        return NodeExecHandler(dataTuple)

class ExecProcessor(threading.Thread):
    def __init__(self, waitTime = 5):
        threading.Thread.__init__(self)
        self.waitTime = 5
        
        self.nodeRunnderList = []
        self.runningRunners = 0
        self.pausingRunners = 0
        
        self.maxRunningThreads = 5;
        
    def run(self):
        while(True):
            self.checkAllRunnerStatus()
            self.processAllExec()
            time.sleep(self.waitTime)
            
    def checkAllRunnerStatus(self):
        logger.info('[ExecProcessor]: In total %d runners: Running: %d Pausing: %d' \
                     % (len(self.nodeRunnderList), self.runningRunners, self.pausingRunners))
        tempRunnerList = []
        for nodeRunner in self.nodeRunnderList:
            expId = nodeRunner.expId
            nodeId = nodeRunner.nodeId
            
            self.updateExpStatus(expId)
            
            runnerStatus = nodeRunner.runnerStatus
            dbStatus = getNodeStatusByNodeIdAndExpId(expId, nodeId)
            
            logger.info('[ExecProcessor]: Runnner for %s:%s: IsAlive=%s, runnerStatus=%s, dbStatus=%s'\
                        % (expId, nodeId, str(nodeRunner.isAlive()), runnerStatus, dbStatus))
            # if nodeRunner already finished, just delete the runner from list
            if not nodeRunner.isAlive():
                logger.info('[ExecProcessor]: Runnner for %s:%s: Finished and Delete' % (expId, nodeId))
                self.runningRunners -= 1
                continue
            else:
                if(runnerStatus=='Running' and dbStatus=='Pausing'):
                    logger.info('[ExecProcessor]: Runnner for %s:%s: Try Pausing Exec' % (expId, nodeId))
                    nodeRunner.pauseNodeSubprocess()
                    self.runningRunners -= 1
                    self.pausingRunners += 1
                elif(runnerStatus!='Aborted' and dbStatus=='Aborted'):
                    logger.info('[ExecProcessor]: Runnner for %s:%s: Try Aborting Exec' % (expId, nodeId))
                    nodeRunner.stopNodeSubprocess()
                elif(runnerStatus=='Pausing' and dbStatus=='Running'):
                    logger.info('[ExecProcessor]: Runnner for %s:%s: Try Resuming Exec' % (expId, nodeId))
                    nodeRunner.resumeNodeSubprocess()
                    nodeRunner.runnerStatus = 'Running'
                    self.runningRunners += 1
                    self.pausingRunners -= 1
            tempRunnerList.append(nodeRunner)
        self.nodeRunnderList = tempRunnerList
    
    def processAllExec(self):
        remainRunnerNum = self.maxRunningThreads - self.runningRunners;
        for ri in xrange(remainRunnerNum):
            # extract node to run from DB
            dataTuple = extractNewNodeToRunFromDB()
            if not dataTuple:
                logger.info('[ExecProcessor]:No nodes to run at the moment...')
                return False
            # get node runner for the particular node
            nodeRunner = getNodeRunnerByType(dataTuple)
            # update node status to running in order not to be picked up again
            if(nodeRunner and nodeRunner.initNodeRunnder()):
                # execute the node
                self.updateExpStatus(nodeRunner.expId)
                nodeRunner.start()
                self.runningRunners += 1;
                self.nodeRunnderList.append(nodeRunner)

    def updateExpStatus(self, expId):
        if(isExpSuccess(expId)):
            updateExpStatusById(expId, 'Finished')
            logger.info('[ExecProcessor]:update status of Experiment %s to Finished' % (expId))
        elif(isExpFailed(expId)):
            updateAllNewNodesStatus(expId, 'Aborted')
            updateExpStatusById(expId, 'Failed')
            logger.warn('[ExecProcessor]:update status of Experiment %s to Failed' % (expId))
        elif(isExpPausing(expId)):
			pass
	else:
            updateExpStatusById(expId, 'Running')
            
def test():
    tempExecP = ExecProcessor()
    tempExecP.processAllExec()
    
if __name__ == '__main__':
    test()
