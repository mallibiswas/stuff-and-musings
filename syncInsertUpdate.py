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
import databaseMethods 
import time

# read datsource to datastore file mapping
def readDictionary(dictFilename):
    
    with open(dictFilename) as json_file:
        json_data = json.load(json_file)
    
    return json_data 


def dataDiffSourceTarget(sourceConn, targetConn, table, srcSchema, tgtSchema, dateCol):
        
    print(".... Running sql for daily distribution on source and target DB")
    
    # queries
    Query = "select convert(date,"+dateCol+") as runDate, count(*) as recs from ["+srcSchema+"].["+table+"] group by convert(date,"+dateCol+")"

    try:
        source_df = pd.read_sql(Query, sourceConn)
    except: # any error here is severe
        e = sys.exc_info()[0]
        print(e)
        print("error executing source sql:", Query)
        print(source_df.head())
        
    try:
        target_df = pd.read_sql(Query, targetConn)
    except:
        e = sys.exc_info()[0]
        print(e)
        print("error executing target sql:", Query)
          

    print ("Comparing ",table," on ",dateCol)
        
    _df = source_df.merge(target_df, how='outer', on='runDate', suffixes=['_1', '_2'], indicator=True)
    _df['check'] = _df.recs_1 != _df.recs_2
    df = _df.loc[_df['check'] == True]
    
    return df
 

def checkDFsize(df):
    return len(df.index)


def columnExists(colname, df):

    if colname in df.columns:
        return True


def createInsertDF (sourceConn, targetConn, Table, srcSchema, pullType, dateCol, dateList):

    listOfDates = str(dateList).strip('[]')

    # Construct query to extract insert df    
    sourceQueryBase="select * from ["+srcSchema+"].["+Table+"]" 
    whereClause=" where convert(date,"+dateCol+") in ("+listOfDates+")"


    # queries
    if pullType == "IR": # = Incremental refresh
        sourceQuery = sourceQueryBase+whereClause+";"
    elif pullType == "FR": # = Full refresh
        sourceQuery = sourceQueryBase+";"

    print (sourceQuery)
        
    try:
        source_df = pd.read_sql(sourceQuery, sourceConn)
    except: # any error here is severe
        e = sys.exc_info()[0]
        print("error in source sql:", sourceQuery)
        print(source_df.head())
        print(e)    
        
    return source_df

    
def loadIncremental (targetConn, Table, srcSchema, tgtSchema, insert_df, dateCol, dateList):

    # delete the date partitions first
    databaseMethods.deleteDates(targetConn, Table, tgtSchema, dateCol, dateList)
        
    if not insert_df.empty: 
        databaseMethods.insertRecs(targetConn, Table, tgtSchema, insert_df) # insert records from insert_df
        print("Inserting Records")
    else:
        print (".... insert df is empty")
        
    return


def loadFull (targetConn, Table, srcSchema, tgtSchema, insert_df):

    if insert_df.empty: 
        print(".... Empty insert dataset - nothing to insert")
        return  
    else:
        databaseMethods.truncateRecs(targetConn, tgtSchema, Table) # truncate table
        databaseMethods.insertRecs(targetConn, Table, tgtSchema, insert_df) # insert records from insert_df

    return


def syncTargetTable (sourceConn, targetConn, Table, srcSchema, tgtSchema, dateCol, pullType, primaryKey, numdays):
    
    print('>>>> refreshing '+tgtSchema+"."+Table+" <<<<")

    print ("Checking for differences in counts:")
    
    check_df = dataDiffSourceTarget(sourceConn, targetConn, Table, srcSchema, tgtSchema, dateCol) 

    print (check_df)
    
    if not check_df.empty: 
        dateList = check_df["runDate"].tolist()
                
        # Sync tables will always use incremental refresh on InsertedOn or UpdatedOn fields
        
        if pullType == "IR":
            print("loadIncremental (Table, tgtSchema, dateList)")
            insert_df = createInsertDF (sourceConn, targetConn, Table, srcSchema, "IR", dateCol, dateList)
            loadIncremental (targetConn, Table, srcSchema, tgtSchema, insert_df, dateCol, dateList)
        elif pullType == "FR":
            print("loadFull (Table, tgtSchema)") # insert the source file for full refresh
            insert_df = createInsertDF (sourceConn, targetConn, Table, srcSchema, "FR", dateCol, ['00-00-0000'])
            loadFull (targetConn, Table, srcSchema, tgtSchema, insert_df) # insert the source file for full refresh
            print ("Run full refresh, don't care about counts")
        else:
            print ("not valid pulltype")
        
    print("*"*50,"\n")
        
    return 'ok'


def initializeSync (paramFile):
    
    sourceConn = databaseMethods.openDBConnection('source') # Open connection
    targetConn = databaseMethods.openDBConnection('target') # Open connection
        
    for i in range (0,len(paramFile["syncTable"])):
    
        tableName      = paramFile["syncTable"][i]["tableName"]
        sourceSchema   = paramFile["syncTable"][i]["sourceSchema"]
        targetSchema   = paramFile["syncTable"][i]["targetSchema"]
        dateField      = paramFile["syncTable"][i]["dateField"]
        pullType       = paramFile["syncTable"][i]["pullType"]
        primaryKey     = paramFile["syncTable"][i]["primaryKey"]
        lookbackDays   = paramFile["syncTable"][i]["lookbackDays"]        
         
        # tablename, source schema, target schema, Date Field, pull Type, Primary Key, identity Insert, Lookback days
        syncTargetTable (sourceConn, targetConn, tableName, sourceSchema, targetSchema, dateField, pullType, primaryKey, lookbackDays)
        
        time.sleep(5) 
        
    sourceConn.close() # Close connection
    targetConn.close() # Close connection
    
    return

