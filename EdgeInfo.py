from DBConnector import *
from DBUtil import *
from JsonObjBase import *

class EdgeInfo:
    def __init__(self):
        self.expId = ''
        self.nodeIdDependBy = -1
        self.portIdDependBy = -1
        self.nodeIdDependOn = -1
        self.portIdDependOn = -1
    def toStr(self):
        return 'Edge(%s,%d,%d,%d,%d)' % (self.expId, self.nodeIdDependBy, self.portIdDependBy,\
                                         self.nodeIdDependOn, self.portIdDependOn)
    def initFromStr(self, string):
        parts = string.rstrip().split(':');
        fromParts = parts[0].split('_')
        self.nodeIdDependBy = int(fromParts[0])
        self.portIdDependBy = int(fromParts[1])
        
        toParts = parts[1].split('_')
        self.nodeIdDependOn = int(toParts[0])
        self.portIdDependOn = int(toParts[1])
        
def insertNewEdgeIntoDB(edgeInfo):
    sql = '''
        insert into %s values('%s',%d,%d,%d,%d) 
        ''' % (nodeDepTable, edgeInfo.expId, edgeInfo.nodeIdDependOn, \
               edgeInfo.nodeIdDependBy, edgeInfo.portIdDependOn, edgeInfo.portIdDependBy)
    dbConnector.execNonQuery(sql)