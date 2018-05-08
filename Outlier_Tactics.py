#! /bin/python
# coding: utf-8

#   Let Python load it's ODBC connecting tool pyodbc
import pyodbc
#   Let Python load it's datetime functions
import datetime
#
import csv
import numpy
import pandas as pd
import initialize 
import databaseMethods

def readData(srcFile, tgtConn):
    
    #
    print (".... reading data from source csv file")
    #

    # exec query on source DB
    
    # read query output into dataframe
    source_df = pd.read_csv(sourceFile)

    # read target df
    
    targetquery =  "select distinct CampaignID, AsofDate from [RPT].[outlier_tactics];"   
    
    # read query output into dataframe
    target_df = pd.read_sql(targetquery, tgtConn)
    
    # Create dataset with new records for insertion into target    
    target_df["inTarget"] = 1 # insert dummy variable to check outer join

    _merge_df = source_df.merge(target_df, on=["CampaignID","AsofDate"], how="left")    
    insert_df = _merge_df[_merge_df.inTarget.isnull()]
    
    # delete dummy columns
    insert_df = insert_df.drop(["inTarget"], 1)
    
    return insert_df


#########################    
# Main program
#########################    


if __name__ == '__main__':
    
    # initialize and read dict of global variables
    initialize.readConfigFile()
    
    print ("\n","<<< Refreshing Outlier_Tactics >>>")
    
    # read data
    sourceConn = databaseMethods.openDBConnection("source")    
    sourceFile = "/home/mallinath.biswas/outlier_output/OutlierAnalysis_All_Tactics.csv"
    targetConn = databaseMethods.openDBConnection("target")    
    incremental_df = readData(sourceFile, targetConn)

    if not incremental_df.empty:
        
        # insert output dataframe into target schema    
        databaseMethods.insertRecs(targetConn, "outlier_tactics", "RPT", incremental_df)
        
    else:
               
        print (".... Nothing to insert in Outlier Tactics ...")
    
