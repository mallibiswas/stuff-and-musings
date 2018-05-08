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
import requests
import numpy as np
import initialize 
import databaseMethods

def readData(srcConn, tgtConn):
    
    #
    print (".... reading data from source")
    #

    # exec query on source DB
    
    sourcequery="SELECT CPM.CampaignKey \
    ,C.CampaignID \
    ,CPM.BeginDateKey \
    ,CPM.EndDateKey \
    ,CPM.AggregateType \
    ,CM.LookUpKey as  CalculationMethodKey \
    ,CM.LookUpName AS CalculationMethod \
    ,ME.LookUpCode AS MetricCode \
    ,CPM.TestStoreCount \
    ,CPM.ControlStoreCount \
    ,clst.TVCStoreCount \
    ,CPM.MetricValue \
    FROM (SELECT CPA.*,AggregateType,RANK() OVER(PARTITION BY CPA.CampaignKey,AggregateType ORDER BY CPA.Productkey DESC) AS AggregateRnk \
      FROM [RPT].[CPMByProductAggregate] CPA \
      JOIN [DSS].CampaignProducts CP ON CPA.CampaignKey = CP.CampaignKey  AND CPA.ProductKey = CP.ProductKey \
         ) CPM \
    JOIN [DSS].[Campaigns] C ON CPM.CampaignKey = C.CampaignKey AND CPM.AggregateRnk = 1 \
    JOIN [DSS].[LookUpValues] CM ON CM.LookUpType = 'CalculationMethod' AND CM.LookUpKey = CPM.CalculationMethodKey  \
    JOIN [DSS].[LookUpValues] ME ON ME.LookUpType = 'Metrics' AND  ME.LookUpKey = CPM.MetricKey \
    CROSS APPLY (SELECT COUNT(*) AS TVCStoreCount \
         FROM [DSS].[vwCampaignClusters] \
     WHERE CampaignKey = c.CampaignKey AND Clustertype = 'TVC' AND Clustername IN ('T','C') \
              ) clst \
    WHERE ME.LookUpCode = 'SLM';"

    # read query output into dataframe
    source_df = pd.read_sql(sourcequery, srcConn)

    # read target df
    
    targetquery =  "select CampaignKey from [RPT].[DisplayMethodologyLookup];"   
    
    # read query output into dataframe
    target_df = pd.read_sql(targetquery, tgtConn)
    
    # Create dataset with new records for insertion into target    
    target_df["inTarget"] = 1 # insert dummy variable to check outer join

    _merge_df = source_df.merge(target_df, on=["CampaignKey"], how="left")    
    insert_df = _merge_df[_merge_df.inTarget.isnull()]
    
    # delete dummy columns
    insert_df = insert_df.drop(["inTarget"], 1)
    
    return insert_df


def dataPrep (df):

    #
    print ("deriving everything needed")
    #
    
    # Derive the variables needed for the logic
    df['StorePct'] = (df['TestStoreCount']+df['ControlStoreCount'])/df['TVCStoreCount']
    df['Lift'] = df['MetricValue'] - 1  

    df['Delta'] = LIFT_TSHLD - df['Lift'] # Lift below threshold are negative
    df['Delta'] = df['Delta'].abs() # take the absolute delta from threshold

    # 
    df['StoreCtCheck'] = 0 # Fail Store Threshold
    df.loc[(df['StorePct'] >= STORECT_TSHLD) & (df['Lift'] >= 0), 'StoreCtCheck'] = 1 # Pass Store Threshold

    # Sort dataframe by campaignkey, aggregate type and delta desc
    df.sort_values(['CampaignKey','AggregateType','Delta'], axis=0, ascending=True, inplace=True, kind='quicksort', na_position='last')
        
    return df


def pickResult (df):
# function to pick the best result
    
# Scan each campaign to pick the best methodology
# Default is the first record
# Scan Featured first and then scan Halo
# Pick the first record that passes all tests (StoreCountCheck and Lift > 0 ), else pick the default

    
    for i, rec in df.iterrows():
        if rec['StoreCtCheck'] == 1 and rec['Lift'] > 0: 
            return {'CampaignKey': [rec['CampaignKey']],
                    'CalculationMethodKey':rec['CalculationMethodKey']} # first match 
            
    return {'CampaignKey': [df.iloc[0]['CampaignKey']],
            'CalculationMethodKey':df.iloc[0]['CalculationMethodKey']}
            # default record, valid rec not found


def outputDF(df):

    # df: input
    # df_: temp df, 6 recs for each campaign sent to pickresults
    # _df: temp df, single rec per campaign back from pickresults
    # out_df: output
    
    #
    print ("creating output df")
    #

    out_df = pd.DataFrame() # null df
    for campaignkey, df_ in df.groupby('CampaignKey'):
            _df = pd.DataFrame.from_dict(pickResult(df_), orient='columns')
            out_df = out_df.append(_df)
    
    out_df = out_df[["CampaignKey","CalculationMethodKey"]]
    
    out_df = out_df.copy()
    out_df['updatedOn'] = datetime.datetime.now()
    out_df['updatedby'] = 'Malli Biswas'

    return out_df




#########################    
# Main program
#########################    


if __name__ == '__main__':
    
    # initialize and read dict of global variables
    initialize.readConfigFile()
    
    print ("\n","<<< Refreshing DisplayMethodologyLookup >>>")
    
    # Define thresholds
    STORECT_TSHLD = .4 # = 40%
    LIFT_TSHLD = .20 # = 20%
    
    # read data
    sourceConn = databaseMethods.openDBConnection("source")    
    targetConn = databaseMethods.openDBConnection("target")    
    incremental_df = readData(sourceConn, targetConn)

    if not incremental_df.empty:
        
        # derive needed variables
        result_df = dataPrep (incremental_df)
        
        # create output dataframe
        output_df = outputDF(result_df)
        
        # insert output dataframe into target schema    
        databaseMethods.insertRecs(targetConn, "DisplayMethodologyLookup", "RPT", output_df)
        
    else:
               
        print (".... Nothing to insert in DisplayMethodologyLookup ...")
    
