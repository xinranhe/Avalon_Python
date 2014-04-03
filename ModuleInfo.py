from ConfigLoader import *
from DBUtil import *
from MyLogger import *

class ModuleInfo:
    def __init__(self):
        self.moduleId = ''
        self.version = ''
        self.name = ''
        self.moduleTypeId = -1
        self.userId = ''
        self.fileLocation = ''
        self.createTime = ''
        self.description = ''
        self.inputArguments = ''
        self.outputArguments = ''
        self.modelParameters = ''
        self.attNamesInOrder = ['moduleId', 'name', 'version', 'userId', 'fileLocation', 'createTime', \
                                'moduleTypeId', 'description', 'inputArguments', 'outputArguments',\
                                'modelParameters']
    def getNumberFromStr(self, inStr):
        if inStr == '':
            return 0;
        fields = inStr.split(';')
        return int(fields[0])
    def getInArgNumber(self):
        return self.getNumberFromStr(self.inputArguments)
    def getOutArgNumber(self):
        return self.getNumberFromStr(self.outputArguments)
    def getParaNumber(self):
        return self.getNumberFromStr(self.modelParameters)
    def getModuleId(self):
        self.moduleId = getUniqueModuleId()
        
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
        return "Module(%s)" % (','.join(self.toArray()))
    
    def setValueFromDict(self, dict):
        for k,v in dict.items():
            setattr(self, k, v)
    
    def initFromJsonString(self, jsonStr):
        dataJson = json.loads(jsonStr);
        self.setValueFromDict(dataJson)

    def initFromJsonFile(self, fileName):
        with open(fileName) as jsonF:
            dataJson = json.load(jsonF)
            self.setValueFromDict(dataJson)
    
# DB functions
def getModuleInfoFromDBByModuleId(dataId):
    sql = '''
        select * from %s where ModuleId = '%s'
    ''' % (
           moduleTable,
           dataId
           )
    resultRows = dbConnector.execQuery(sql)
    resultRow = resultRows[0];
    moduleInfo = ModuleInfo()
    moduleInfo.initFromArray(resultRow);
    return moduleInfo

def insertNewDataIntoDB(dataInfo):
    insertValues = ["'"+value+"'" for value in dataInfo.toArray()]
    #print insertValues
    sql = '''
        insert into %s values(%s) ''' % (moduleTable, ','.join(insertValues))
    dbConnector.execNonQuery(sql)
