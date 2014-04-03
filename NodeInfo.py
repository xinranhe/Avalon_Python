from DBConnector import *
from DBUtil import *
from JsonObjBase import *


class NodeInfo(JsonObjBase):
    def __init__(self):
        JsonObjBase.__init__(self)
        self.attNamesInOrder = ['expId','nodeId','nodeType','itemId',\
                                'nodeStatus','message','parameters']
        self.initAttributes()
    def toStr(self):
        return "Node(%s)" % (','.join(self.toArray()))
    
def getNodeInfoByExpIdAndNodeId(expId, nodeId):
    sql = '''
        select * from %s where ExpId = '%s' and 'NodeId' = %s
        ''' % (nodeTable, expId, nodeId)
    resultRows = dbConnector.execQuery(sql)
    resultRow = resultRows[0];
    nodeInfo = NodeInfo()
    nodeInfo.initFromArray(resultRow);
    return nodeInfo

def insertNewNodeIntoDB(nodeInfo):
    insertValues = ["'"+value+"'" for value in nodeInfo.toArray()]
    #print insertValues
    sql = '''
        insert into %s values(%s) ''' % (nodeTable, ','.join(insertValues))
    dbConnector.execNonQuery(sql)