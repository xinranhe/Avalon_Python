'''
Created on Dec 15, 2013

@author: hxr
'''
from ConfigLoader import *
from DBUtil import *
from MyLogger import *

import json

class DataInfo:
    def __init__(self):
        self.dataId = ''
        self.version = ''
        self.name = ''
        self.dataTypeId = -1
        self.userId = ''
        self.fileLocation = ''
        self.createTime = ''
        self.description = ''
        self.attNamesInOrder = ['dataId', 'name', 'version', 'userId', 'fileLocation', 'createTime', 'dataTypeId', 'description']
    
    def getDataId(self):
        self.dataId = getUniqueDataId()

    def toArray(self):  
        result = []
        for eachName in self.attNamesInOrder:
            result.append(str(getattr(self, eachName)))
        return result
    
    def initFromArray(self, attList):
        for i in xrange(len(attList)):
            attStr = str(attList[i]).strip()
            setattr(self, self.attNamesInOrder[i], attStr)
    
    def toStr(self):
        return "Data(%s)" % (','.join(self.toArray()))
    
    def setValueFromDict(self, mydict):
        for k,v in mydict.items():
            setattr(self, k, v)
    
    def initFromJsonString(self, jsonStr):
        dataJson = json.loads(jsonStr);
        self.setValueFromDict(dataJson)

    def initFromJsonFile(self, fileName):
        with open(fileName) as jsonF:
            dataJson = json.load(jsonF)
            self.setValueFromDict(dataJson)

# DB functions
def getDataInfoFromDBByDataId(dataId):
    sql = '''
        select * from %s where DataId = '%s'
    ''' % (
           dataTable,
           dataId
           )
    resultRows = dbConnector.execQuery(sql)
    resultRow = resultRows[0];
    dataInfo = DataInfo()
    dataInfo.initFromArray(resultRow);
    return dataInfo

def insertNewDataIntoDB(dataInfo):
    insertValues = ["'"+value+"'" for value in dataInfo.toArray()]
    #print insertValues
    sql = '''
        insert into %s values(%s) ''' % (dataTable, ','.join(insertValues))
    dbConnector.execNonQuery(sql)


def test():
    tempDataInfo = DataInfo()
    tempDataInfo.getDataId();
    print tempDataInfo.dataId

if __name__ == '__main__':
    test()
