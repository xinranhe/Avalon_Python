from ConfigLoader import *
from DBConnector import *
from DBUtil import *
from UploadProcessor import *


import shutil

def clearAllTables():
    tableNames = [dataTable, expTable, moduleTable, nodeTable, nodeDepTable, pendingItemTable]
    for tableName in tableNames:
        sql = 'delete from %s' % (tableName)
        dbConnector.execNonQuery(sql)

def clearAllFolders():
    paths = [submitConfig.moduleDir,\
             submitConfig.dataDir,\
             submitConfig.expDir,\
             storageConfig.dataDir,\
             storageConfig.moduleDir,\
             expConfig.expDir]
    for path in paths:
        commandStr = 'rm -rf ' + path + os.sep + '*'
        print commandStr
        os.system(commandStr)

def uploadData():
    src = '/Users/xinranhe/Dropbox/Projects/Avalon/code/python/ServerNew/testData/testData'
    dst = submitConfig.dataDir + os.sep + 'testData'
    itemId = 'testUploadData'
    itemType = 'Data'
    location = 'testData'
    mainFile = 'testData.txt'
    itemJson = '''{
    "dataId" : "111",
    "name" : "testData",
    "version" : "1.0",
    "userId" : "0000",
    "createTime" : "2014-01-01 10:00:00",
    "description" : "test data for update"
    }'''
    shutil.copytree(src,dst)
    sql = '''insert into %s values('%s','%s','%s','%s','%s','%s','')
    ''' % (pendingItemTable, itemId, itemType, location, mainFile, itemJson,'new') 
    dbConnector.execNonQuery(sql)

def uploadModule():
    src = '/Users/xinranhe/Dropbox/Projects/Avalon/code/python/ServerNew/testData/testModule'
    dst = submitConfig.moduleDir + os.sep + 'testModule'
    itemId = 'testUploadModule'
    itemType = 'Module'
    location = 'testModule'
    mainFile = 'testModule'
    itemJson = '''{
    "moduleId" : "222",
    "name" : "txtCombiner",
    "version" : "1.0",
    "userId" : "0000",
    "moduleTypeId" : "0",
    "createTime" : "2014-01-01 10:00:00",
    "description" : "Combine two input txt file",
    "inputArguments": "2;In1:1;In2:1;",
    "outputArguments": "1;Out1:1;"
    }'''
    shutil.copytree(src,dst)
    sql = '''insert into %s values('%s','%s','%s','%s','%s','%s','')
    ''' % (pendingItemTable, itemId, itemType, location, mainFile, itemJson,'new') 
    dbConnector.execNonQuery(sql)
    
def uploadExp():
    itemId = 'textUploadExp'
    itemType = 'Exp'
    itemJson = '''
        {
    "name": "TestExp",
    "userId": "0000",
    "description": "test experiment",
    "nodesArray": [
            {
                "itemId": "111",
                "nodeType": "Data"
            },
            {
                "itemId": "111",
                "nodeType": "Data"
            },
            {
                "itemId": "222",
                "nodeType": "Module"
            }
        ],
    "edgesArray": [
        "0_1:2_1",
        "1_1:2_2"
        ]
    }
    '''
    sql = '''insert into %s values('%s','%s','','','%s','%s','')
    ''' % (pendingItemTable, itemId, itemType, itemJson, 'new') 
    dbConnector.execNonQuery(sql)

def cleanAll():
    clearAllFolders();
    clearAllTables();

# run this with careful, this method clears all tables
def createExperiment():
    cleanAll()
    uploadData()
    uploadModule()
    uploadExp()
    
    tempUploadProcessor = UploadProcessor()
    tempUploadProcessor.processAllUploads()

if __name__ == '__main__':
    cleanAll()
    