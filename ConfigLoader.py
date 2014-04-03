'''
Created on Oct 13, 2013

@author: hxr
'''
import json

configFolder = '../config/'
jsonConfigFile = 'config.json'


class LoggerConfig:
    def __init__(self, dict):
        self.logFolder = dict['logFolder'];
        self.logFileName = dict['logFile'];
        
class DBConfig:
    def __init__(self, dict):
        self.dbType = dict['dbType'];
        self.dbName = dict['dbName'];
        self.user = dict['user'];
        self.password = dict['password'];
        self.host = dict['host'];

class StorageConfig:
    def __init__(self, dict):
        self.dataDir = dict['dataDir'];
        self.moduleDir = dict['moduleDir'];

class SubmitConfig:
    def __init__(self, dict):
        self.dataDir = dict['dataDir'];
        self.moduleDir = dict['moduleDir'];
        self.expDir = dict['expDir'];

class ExpConfig:
    def __init__(self, dict):
        self.expDir = dict['expDir'];

def initExpConfig():
    with open(configFolder+jsonConfigFile) as jsonF:
        jsonConfig = json.load(jsonF)
        tempDict = jsonConfig['Experiment'];
        return ExpConfig(tempDict);    

def initLoggerConfig():
    with open(configFolder+jsonConfigFile) as jsonF:
        jsonConfig = json.load(jsonF)
        tempDict = jsonConfig['logging'];
        return LoggerConfig(tempDict);

def initDBConfig():
    with open(configFolder+jsonConfigFile) as jsonF:
        jsonConfig = json.load(jsonF)
        tempDict = jsonConfig['DataBase'];
        return DBConfig(tempDict);

def initStorageConfig():
    with open(configFolder+jsonConfigFile) as jsonF:
        jsonConfig = json.load(jsonF)
        tempDict = jsonConfig['Storage'];
        return StorageConfig(tempDict); 

def initSubmitConfig():
    with open(configFolder+jsonConfigFile) as jsonF:
        jsonConfig = json.load(jsonF)
        tempDict = jsonConfig['Submit'];
        return SubmitConfig(tempDict);    

def testJson():
    fileName = configFolder+jsonConfigFile
    print json.load(open(fileName));
    
logConfig = initLoggerConfig()
dbConfig = initDBConfig()
storageConfig = initStorageConfig()
submitConfig = initSubmitConfig()
expConfig = initExpConfig()
    
