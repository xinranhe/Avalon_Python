'''
Created on Jul 8, 2013

@author: hxr
'''

from ConfigLoader import *
from MyLogger import *

import MySQLdb

class DBConnector:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
    
    def execQuery(self, sql):
        raise NotImplementedError("Please Implement this method")
    
    def execNonQuery(self, sql):
        raise NotImplementedError("Please Implement this method")
    
    def getConnection(self):
        raise NotImplementedError("Please Implement this method")

class MySqlConnector(DBConnector):
    def __init__(self,host,user,pwd,db):
        DBConnector.__init__(self,host,user,pwd,db)
    
    def getConnection(self):
        con = MySQLdb.connect(self.host, self.user, self.pwd, self.db);
        return con
    
    def execQuery(self, sql):
        try:
            con = self.getConnection()
            cur = con.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            return rows
        except MySQLdb.Error as e:
            logger.exception(e)
        finally:
            if con:
                con.close()
    
    def execNonQuery(self, sql):
        try:
            con = self.getConnection()
            cur = con.cursor()
            cur.execute(sql)
            con.commit()
        except MySQLdb.Error as e:
            logger.exception(e)
        finally:
            if con:
                con.close()

def getDBConnector():
    if(dbConfig.dbType == 'MySql'):
        dbConnector = MySqlConnector(dbConfig.host, dbConfig.user, dbConfig.password, dbConfig.dbName);
    else:
        dbConnector = DBConnector(dbConfig.host, dbConfig.user, dbConfig.password, dbConfig.dbName);
    return dbConnector

dbConnector = getDBConnector()
