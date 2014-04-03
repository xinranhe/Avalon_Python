'''
Created on Dec 15, 2013

@author: hxr
'''

from ConfigLoader import *
from MyLogger import *
from DBConnector import *

import uuid

dataTable = 'Data'
dataTypeTable = 'DataType'
expTable = 'Experiment'
moduleTable = 'Module'
moduleTypeTable = 'ModuleType'
nodeTable = 'Node'
nodeDepTable = 'NodeDependence'
pendingItemTable = 'PendingItem'

# DB utilities for data,module,exp uplaoding
def updateStatusAndMessageById(itemId, newStatus, newMsg):
    sql = '''
        update %s set Status='%s', Message='%s' where ItemId = '%s'
          ''' % (pendingItemTable, newStatus, newMsg, itemId)
    dbConnector.execNonQuery(sql)

def deleteFromPendingTable(itemId):
    sql = '''
        delete from %s where ItemId='%s'
        ''' % (pendingItemTable, itemId)
    dbConnector.execNonQuery(sql)

def getAllPendingItems():
    sql = '''
        select * from %s where Status = 'New'
        ''' % (pendingItemTable)
    resultRows = dbConnector.execQuery(sql)
    result = []
    for row in resultRows:
        tempList = [str(tstr).strip() for tstr in row]
        result.append(tuple(tempList))
    return result

# Functions for get unique ids
def getUniqueDataId():
    while(True):
        newID = str(uuid.uuid4())
        newID = newID.replace('-','')
        if(isUniqueID(newID, dataTable, 'DataId')):
            return newID   

def getUniqueModuleId():
    while(True):
        newID = str(uuid.uuid4())
        newID = newID.replace('-','')
        if(isUniqueID(newID, moduleTable, 'ModuleId')):
            return newID    

def getUniqueExpId():
    while(True):
        newID = str(uuid.uuid4())
        newID = newID.replace('-','')
        if(isUniqueID(newID, expTable, 'ExpId')):
            return newID  

def isUniqueID(testID, tableName, columnName):
    sql = "select * from %s where %s = '%s'" % (tableName, columnName, testID)
    results = dbConnector.execQuery(sql)
    if len(results)>0:
        return False;
    else:
        return True;  
    
# functions for node executing
def extractNewNodeToRunFromDB():
    sql = '''
            select ExpId, NodeId, NodeType, ItemId, NodeStatus, Parameters 
            from %s as node
            where node.NodeStatus = 'New' and not exists (
                select * from %s as tnode, %s as tedge
                where tnode.ExpId = node.ExpId and tedge.ExpId = node.ExpId
                      and node.NodeId = tedge.NodeIdDependOn 
                      and tnode.NodeId = tedge.NodeIdDependBy
                      and tnode.NodeStatus <> 'Finished'
            ) and exists (
                select * from %s as tExp
                where tExp.ExpId = node.ExpId and (tExp.ExpStatus = 'New' or tExp.ExpStatus = 'Running')
            )
            limit 1
        ''' % (nodeTable, nodeTable, nodeDepTable, expTable)
    results = dbConnector.execQuery(sql)
    if(len(results) >= 1):
        return results[0]
    else:
        return None

def updateNodeStatusAndMsgById(expId, nodeId, status, msg):
    sql = '''
             update %s set NodeStatus = '%s', Message = '%s'
             where ExpId = '%s' and NodeId = '%s'
          ''' % (nodeTable, status, msg, expId, nodeId)
    dbConnector.execNonQuery(sql)   

def getNodeStatusByNodeIdAndExpId(expId, nodeId):
    sql = '''
        select NodeStatus from Node where ExpId = '%s' and NodeId = '%s' 
    ''' % (expId, nodeId)
    results = dbConnector.execQuery(sql)
    return results[0][0];
          
# functions about experiment overall experiments
def isExpSuccess(expId):
    sql = '''
            select * from %s
            where ExpId = '%s' and NodeStatus <> 'Finished'
        ''' % (nodeTable, expId)
    results = dbConnector.execQuery(sql)
    return len(results)==0

def isExpPausing(expId):
    sql = '''
            select * from %s
            where ExpId = '%s' and NodeStatus = 'Pausing'
        ''' % (nodeTable, expId)
    results = dbConnector.execQuery(sql)
    isNodePausing = len(results)>0
    sql = '''
            select * from %s
            where ExpId = '%s' and ExpStatus = 'Pausing'
        ''' % (expTable, expId)
    results = dbConnector.execQuery(sql)
    isExpPausing = len(results)>0
    return isNodePausing or isExpPausing

def isExpFailed(expId):
    sql = '''
            select * from %s
            where ExpId = '%s' and NodeStatus = 'Failed'
        ''' % (nodeTable, expId)
    results = dbConnector.execQuery(sql)
    return len(results)>0

def updateAllNewNodesStatus(expId, status):
    sql = '''
             update %s set NodeStatus = '%s'
             where ExpId = '%s' and NodeStatus = 'New'
          ''' % (nodeTable, status, expId)
    dbConnector.execNonQuery(sql)   

def updateExpStatusById(expId, status):
    sql = '''
             update %s set ExpStatus = '%s'
             where ExpId = '%s'
          ''' % (expTable, status, expId)
    dbConnector.execNonQuery(sql)  

def cleanPendingItem():
	sql = "delete from PendingItem where Status='Finished'"
	dbConnector.execNonQuery(sql)  
    
def getAllExpId():
    sql = '''
        select ExpId from %s
        ''' % (expTable)
    resultRows = dbConnector.execQuery(sql)
    result = []
    for row in resultRows:
        result.append(str(row[0]).strip())
    return result

def getAllDataId():
    sql = '''
        select DataId from %s
        ''' % (dataTable)
    resultRows = dbConnector.execQuery(sql)
    result = []
    for row in resultRows:
        result.append(str(row[0]).strip())
    return result

def getAllModuleId():
    sql = '''
        select ModuleId from %s
        ''' % (moduleTable)
    resultRows = dbConnector.execQuery(sql)
    result = []
    for row in resultRows:
        result.append(str(row[0]).strip())
    return result