from ConfigLoader import *
from DBUtil import *
from EdgeInfo import *
from MyLogger import *
from NodeInfo import *

class ExpInfo(JsonObjBase):
    def __init__(self):
        JsonObjBase.__init__(self)
        self.attNamesInOrder = ['expId', 'name', 'userId', 'expStatus', 'description', 'message', 'createTime']
        self.initAttributes()
        
        self.nodesArray = []
        self.nodes = []
        self.nodes = []
        self.nodeNum = 0
        
        self.edgesArray = []
        self.edges = []
        self.edgeNum = 0
    def toStr(self):
        result = "Experiment(%s)\n" % (','.join(self.toArray()))
        result += 'Nodes:\n'
        for nodeInfo in self.nodes:
            result += nodeInfo.toStr() + '\n'
        result += 'Edges:\n'
        for edgeInfo in self.edges:
            result += edgeInfo.toStr() + '\n'
        return result
    def initFromJsonString(self, jsonStr):
        # call base to set attribute values
        JsonObjBase.initFromJsonString(self, jsonStr)
        self.parseAllNodes()
        self.parseAllEdges()
        self.expStatus = 'New'
    def parseAllNodes(self):
        for nodeDict in self.nodesArray:
            newNodeInfo = NodeInfo()
            newNodeInfo.expId = self.expId
            newNodeInfo.nodeId = self.nodeNum
            newNodeInfo.nodeStatus = 'New'
            newNodeInfo.setValueFromDict(nodeDict)
            self.nodes.append(newNodeInfo)
            self.nodeNum += 1
    def parseAllEdges(self):
        for edgeStr in self.edgesArray:
            newEdgeInfo = EdgeInfo()
            newEdgeInfo.expId = self.expId

            newEdgeInfo.initFromStr(edgeStr)
            self.edges.append(newEdgeInfo)
            self.edgeNum += 1

def insertNewExpIntoDB(expInfo):
    # Step 1: insert into Experiment table
    insertValues = ["'"+value+"'" for value in expInfo.toArray()]
    sql = '''
        insert into %s values(%s) ''' % (expTable, ','.join(insertValues))
    dbConnector.execNonQuery(sql)
    
    # Step 2: insert into Node table    
    for nodeInfo in expInfo.nodes:
        insertNewNodeIntoDB(nodeInfo)
    
    # Step 3: insert into NodeDependence table
    for edgeInfo in expInfo.edges:
        insertNewEdgeIntoDB(edgeInfo)