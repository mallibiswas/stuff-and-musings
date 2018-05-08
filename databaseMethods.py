#! /bin/python

import pyodbc
import datetime
import csv
import os
import sys
import json
import numpy as np
import pandas as pd
import requests
import pymssql
import initialize

#
# Module contains the basic database methods: insert, delete, truncate and execute stored procedure
#

def openDBConnection(connType):
    
    # Connection parameters are encrypter global variables
    
    print(".... Setting up",connType,"connection")

    if connType == 'source':
        conn = pymssql.connect(server=initialize.sourceServer, user=initialize.sourceUser, password=initialize.sourcePwd, database=initialize.sourceDB)
    elif connType == 'target':
        conn = pymssql.connect(server=initialize.targetServer, user=initialize.targetUser, password=initialize.targetPwd, database=initialize.targetDB)
    else:
        print ('Not a valid connection type: source or target')
    
    return conn



def insertRecs(conn, table_name, DBSchema, df):

    print(".... Inserting ",len(df.index)," records into target DB")
    
    # Handle null datasets
    if df.empty: return print ('no recs to insert')
    
    # opened connection 
    cur = conn.cursor()              

    # Handle nulls
    df = df.astype(object).where(pd.notnull(df), None)
    
    # create column list from dataframe and load data in tuples
    wildcards = ','.join(['%s'] * len(df.columns))    
    data = [tuple(x) for x in df.values]
    colnames = ','.join(cols for cols in df.columns)
    
    try:
        
        # Set identity off if the table has it on
        cur.execute("IF OBJECTPROPERTY(OBJECT_ID('%s.%s'), 'TableHasIdentity') = 1 SET IDENTITY_INSERT [%s].[%s] OFF" % (DBSchema, table_name, DBSchema, table_name))

        cur.executemany("INSERT INTO %s.%s (%s) VALUES(%s)" % (DBSchema, table_name, colnames, wildcards), data)
        conn.commit()
    except pymssql.DatabaseError as e:
        print ("Raised Error: {0}".format(e))
        conn.rollback()
    finally:
        cur.close()
        
    return


def deleteRecs(conn, tableName, DBSchema, primaryKey, df):

    print(".... Deleting ",len(df.index)," records from target DB")

    _df=df.loc[:,[primaryKey,'UpdatedOn']]
    # Need at least 2 cols to build tuple, make sure campiagnkey is in position 1
    
    data = [tuple(x) for x in _df.values]

    cur=conn.cursor()

    try:
        cur.executemany("Delete from %s.%s where %s = %s"  % (DBSchema, tableName, primaryKey, '%d'), data)
        conn.commit()
    except pymssql.DatabaseError as e:
        print ("Raised Error: {0}".format(e))
        conn.rollback()
    finally:
        cur.close()

    return


def deleteDates(conn, tableName, DBSchema, dateCol, dateList):

    listOfDates = str(dateList).strip('[]')

    print(".... Deleting partitions from [",DBSchema,"][",tableName,"][", dateCol,"]:",listOfDates)
    
    cur=conn.cursor()

    query = "Delete from {0}.{1} where convert(date,{2}) = %s".format(DBSchema, tableName, dateCol)
    
    try:
        # Set identity on/off depending on what the table has
        cur.executemany(query, dateList)
        conn.commit()
    except pymssql.DatabaseError as e:
        print ("Raised Error: {0}".format(e))
        conn.rollback()
    finally:
        cur.close()

    return


def executeProc(conn, procParams):

    DBSchema = procParams["Schema"] 
    procName = procParams["Name"] 
    colName = procParams["Column"]
    colValue = procParams["Value"]
    
    print(".... Executing procedure:",procName," with parameters:",colName,"=",colValue)
    
    cur=conn.cursor()

    try:
        
        if colName: # execute with parameters 
            cur.execute("EXEC %s.%s @%s = %d"  % (DBSchema, procName, colName, colValue))
        else:
            cur.execute("EXEC %s.%s"  % (DBSchema, procName)) # execute without parameters 
            
        print ("successfully executed ", procName)
        
    except pymssql.DatabaseError as e:
        print ("Raised Error: {0}".format(e))
        conn.rollback()
    finally:
        cur.close()
        conn.close()

    return df



def truncateRecs(conn, DBSchema, table_name):
    
    # truncate table
    
    print(".... truncating ",table_name)
    
    # reopen connection 
    cur = conn.cursor()              

    try:
        cur.execute("TRUNCATE TABLE %s.%s" % (DBSchema, table_name))
        conn.commit()
    except pymssql.DatabaseError as e:
        print ("Raised Error: {0}".format(e))
        conn.rollback()
    finally:
        cur.close()
        
    return

