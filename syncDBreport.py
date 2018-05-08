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
        print("error in source sql:", Query)
        print(source_df.head())
        print(e)
        
    try:
        target_df = pd.read_sql(Query, targetConn)
    except:
        e = sys.exc_info()[0]
        print("error in target sql:", Query)
        print(e)
          
    print ("Comparing ",table," on ",dateCol)
        
    _df = source_df.merge(target_df, how='outer', on='runDate', suffixes=['_1', '_2'], indicator=True)
    _df['check'] = _df.recs_1 != _df.recs_2
    df = _df[(_df.check == True)]  # Keep only where differences exist
    
    # add schema and table info
    df = df.copy()
    df['Schema'] = srcSchema
    df['Table'] = table
    df['Column'] = dateCol
      
    return df
   
def checkDFsize(df):
    return len(df.index)


def columnExists(colname, df):

    if colname in df.columns:
        return True


def printResults (df, outFile):
    
    df.to_csv (outFile, index=False)

    print ("Done report ...")
    
    return
    

def initializeSync (paramFile):
    
    sourceConn = databaseMethods.openDBConnection('source') # Open connection
    targetConn = databaseMethods.openDBConnection('target') # Open connection
        
    result_df = pd.DataFrame() 
    
    for i in range (0,len(paramFile["syncTable"])):
    
        tableName      = paramFile["syncTable"][i]["tableName"]
        sourceSchema   = paramFile["syncTable"][i]["sourceSchema"]
        targetSchema   = paramFile["syncTable"][i]["targetSchema"]
        dateField      = paramFile["syncTable"][i]["dateField"]
        pullType       = paramFile["syncTable"][i]["pullType"]
        primaryKey     = paramFile["syncTable"][i]["primaryKey"]
        lookbackDays   = paramFile["syncTable"][i]["lookbackDays"]        
                 
        print('>>>> Checking for differences in counts: '+sourceSchema+"."+tableName+" <<<<")
    
        check_df = dataDiffSourceTarget(sourceConn, targetConn, tableName, sourceSchema, targetSchema, dateField) 
    
        if not check_df.empty: 
            result_df = result_df.append(check_df, ignore_index=True)

            
    sourceConn.close() # Close connection
    targetConn.close() # Close connection
            
    return result_df
    
    
#########################    
# Main program
#########################    


if __name__ == '__main__':
    
    # initialize global variables in initialize.readConfigFile()
    initialize.readConfigFile()
    
    # get path+filename to dictionary
    dictFilename = os.path.join(initialize.dataDirectory, initialize.dictFile)
    outFilename = os.path.join(initialize.dataDirectory, "results.csv")
    
    # read dictionary
    y = readDictionary(dictFilename)

    df = initializeSync (y)

    if not df.empty:    
        printResults (df, outFilename)
    else:
        print ("DBs in complete Sync, nothing to report")