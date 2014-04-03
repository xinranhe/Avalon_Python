import json

class JsonObjBase:
    def __init__(self):
        self.attNamesInOrder = []
    
    def initAttributes(self):
        for item in self.attNamesInOrder:
            setattr(self, item, '')
    
    def toArray(self):
        result = []
        for eachName in self.attNamesInOrder:
            result.append(str(getattr(self, eachName)))
        return result
    
    def initFromArray(self, attList):
        for i in xrange(len(attList)):
            attStr = str(attList[i]).strip()
            setattr(self, self.attNamesInOrder[i], attStr)         

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