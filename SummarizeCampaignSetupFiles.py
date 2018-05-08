import os
import pyodbc
#   Let Python load it's datetime functions
import datetime, time
#from datetime import datetime
#
import csv
import pandas as pd
import requests
import numpy as np
import glob
import pymssql
import random
import hashlib
import databaseMethods
import initialize



def summarizeDF (df, smryType):
    
    # Summary type is Store (=S) or Aggregate (=A)
        
    if smryType == 'S':
        result_df = df[["CampaignKey", "CampaignID", "StoreId", "AggregateType", "AggregateName", "SalesAmount", "FirstScanDate", "LastScanDate"]]
    elif smryType == 'A':
        groupby_df = df.groupby(["CampaignKey", "CampaignID", "AggregateType", "AggregateName"])
        result_df=groupby_df.agg({"SalesAmount"  : "sum",
                                  "FirstScanDate": "min",
                                  "LastScanDate" : "max"})
        
    else:
        print (".... Error: summary type is A(ggregate) or S(tore)")
        return pd.Dataframe()
    
    
    return result_df


def createRefDFs (srcConn, tgtConn):
        
    # fetch campaign ref data
    cQuery = "select CampaignID, CampaignKey  \
            from DSS.Campaigns \
            where campaignkey not in (479,540)"

    campaign_ref_df = pd.read_sql (cQuery, srcConn)                

    # fetch campaign prodcts ref data
    cpQuery = "select CampaignID, AggregateType, AggregateName  \
            from DSS.vwCampaignProducts cp \
            join DSS.Campaigns c on c.campaignkey = cp.campaignkey \
            where cp.campaignkey not in (479,540)"

    campaignProducts_ref_df = pd.read_sql (cpQuery, srcConn)                
        
    # fetch ref data for aggregate summary
    aQuery = "select distinct CampaignID from RPT.AggregateSales"

    aggregate_ref_df = pd.read_sql (aQuery, tgtConn)                

    # fetch ref data for store summary
    sQuery = "select distinct CampaignID from RPT.StoreSales"

    store_ref_df = pd.read_sql (sQuery, tgtConn)                
    
    # fetch ref data for store summary (campaign setup file names)
    csQuery = "select distinct FileName from RPT.CampaignSetup"

    campaignSetup_ref_df = pd.read_sql (csQuery, tgtConn)                
    
    
    print(".... Created lookup files ...")
        
    
    return {"campaigns":campaign_ref_df, 
            "campaignproducts":campaignProducts_ref_df, 
            "aggregates":aggregate_ref_df, 
            "stores":store_ref_df, 
            "setup":campaignSetup_ref_df}
        

def processDF (lookup_df, df, cp_df):    
    
    #
    # Inputs: Lookup_df is the campaign reference df mapping campaignid to campaignkey
    # cp_df: campaign products df
    # df is the file loaded from campaign setup
    # this function will merge with campaignkey, then lookup campaignproducts 
    # and perform data cleanup if needed
    #
    
    # UTRunTime has coding issues - check and rename correctly
    cols = df.columns.tolist()
    FileExtractTime = ["UTCRunTime"]
    FileExtractTime = [s for s in cols if "UTCRunTime" in s]
    df = df.rename(columns={FileExtractTime[0] : "UTCRunTime", "Store" : "StoreId"})
    
    # Check if AggregateType exists
    AggregateTypeCheck = [s for s in cols if "AggregateType" in s]
    
    if not AggregateTypeCheck: # Create placeholder column ..
            df["AggregateType"] = "unknown" # cannot set to null - will blow up summary
            # more processing to update AggregateType is done in processDF()
            
    # 1> merge with campaignkey
    _df = df.reset_index().merge(lookup_df.reset_index(), on=["CampaignID"], how="inner")

    # 2> merge with campaignProduct
    
    # create key to merge on
    _df.reset_index (inplace=True)
    _df["CampaignProductKey"]=_df["CampaignID"].str.lower()+_df["AggregateName"].str.lower() 
    cp_df["CampaignProductKey"]=cp_df.reset_index()["CampaignID"].str.lower()+cp_df.reset_index()["AggregateName"].str.lower()
    
    # merge on campaignProductkey    
    merge_df = _df.merge(cp_df, on=["CampaignProductKey"], how="left")

    # Cleanup AggregateType data ...
    
    # Update with campaignproducts.AggregateType if needed
    merge_df["AggregateType"] = merge_df["AggregateType_x"] # create new field

    merge_df["AggregateType"] = np.where(pd.isnull(merge_df["AggregateType_x"])|( merge_df["AggregateType_x"].str.match("unknown")==True), merge_df["AggregateType_y"], merge_df["AggregateType_x"])
 
    merge_df["AggregateType"] = np.where((
            (pd.isnull(merge_df["AggregateType"])|( merge_df["AggregateType"].str.match("unknown")==True)) & (merge_df["AggregateName_x"].str.contains("helo|halo|brand|bdi", case=False))), "Halo", merge_df["AggregateType"])

    merge_df["AggregateType"] = np.where(((pd.isnull(merge_df["AggregateType"])|( merge_df["AggregateType"].str.match("unknown")==True)) & (merge_df["AggregateName_x"].str.contains("fea|pdi", case=False))), "Featured", merge_df["AggregateType"])

    merge_df["AggregateType"] = np.where(((pd.isnull(merge_df["AggregateType"])|( merge_df["AggregateType"].str.match("unknown")==True)) & (merge_df["AggregateName_x"].str.contains("base", case=False))), "Base", merge_df["AggregateType"])

    
    # Cleanup unnecessary columns            
    merge_df.drop(["AggregateType_x", "AggregateType_y", "CampaignID_y", "AggregateName_y", "CampaignProductKey"], axis=1, inplace=True)
            
    merge_df.rename(columns={"CampaignID_x":"CampaignID",
                             "AggregateType_x": "AggregateType",
                              "AggregateName_x": "AggregateName" }, inplace=True)
        
    
    return merge_df


def isCampaignIDexists (df, matchKey, matchOn):
    
    # check if a particular value exists in df.matchkey column
    
    # True if df is a valid dataframe and matchOn exists in the key list
    if df.empty: 
        return False
    else:
        keyList = df[matchKey].tolist()
        if matchOn in keyList: 
            return True
        else:
            return False


def createCampaignSetup (shareDir, subdir, name):
    
    # Create full path and filename
    filename = os.path.join (shareDir, subdir, name)                 
    fileFullPath = os.path.normpath (filename)

    # convert file timestamp from string to datetime
    _fileLastModified = time.ctime(os.path.getmtime(filename))
    fileLastModified = datetime.datetime.strptime(_fileLastModified, '%a %b %d %H:%M:%S %Y')

    # parse filename to get campaign id and datestamps
    wordsList = name.split("_")
    fileCampaignId = wordsList[0]
    # format the created on date
    try:
        _fileCreateDate = wordsList[len(wordsList)-1].split(".")[0]
        fileCreateDate = datetime.datetime.strptime(_fileCreateDate, '%Y%m%d')
    except: # some file(s) can have a date and a subscript, e.g. _20160808_1.csv ...
        _fileCreateDate = wordsList[len(wordsList)-2] # if so, pick the second last word
        fileCreateDate = datetime.datetime.strptime(_fileCreateDate, '%Y%m%d')
        
    # parse filepath to get client name
    clientName = os.path.basename(subdir)

    """
    print("Checking:", name, "\n",
          "located:", fileFullPath, "\n",
          "client:",clientName, "\n", 
          "Campaign Id:", fileCampaignId, "\n", 
          "last modified:", fileLastModified, "\n",
          "created on:", fileCreateDate)
    """
    
    # Create dict to insert to CampaignSetup
    d = {"CampaignID": pd.Series([fileCampaignId], index=['0']),
         "CustomerCode":pd.Series([clientName], index=['0']),
         "FileName":pd.Series([name], index=['0']),
         "FileFullPath":pd.Series([fileFullPath], index=['0']),
         "FileCreatedOn":pd.Series([fileCreateDate], index=['0']),
         "FileUpdatedOn":pd.Series([fileLastModified], index=['0']),
         "UpdatedOn":pd.Series([datetime.datetime.now()], index=['0']),
         "UpdatedBy":pd.Series(["AnsaReports"], index=['0'])}
    
    # flag recently modified file(s) in log
    if (fileLastModified >= datetime.datetime.now() + datetime.timedelta(weeks=-1)):
        print (".... Note: >>>>",filename," IS RECENT <<<<")

    # create dataframe record from campaignsetup dict
    fileSetup_df = pd.DataFrame(d)
        
    return fileSetup_df # return 1-record dataframe for a particular campaign setup file


def createCheckList (setupRef_df, campaignRef_df, aggregateRef_df, storeRef_df, name, fileCampaignId):
    
    #
    # Inputs: reference data from source and target databases
    # name of file
    # campaignId extracted from filename
    # this function creates 4 variables to indicate whether the file/data has been preiously inserted 
    # and whether the campaignid is a valid campaignid (i.e. a corresponding campaignkey exists)
    #
    
    # check if the current setup file exists in database
    setupCheck = not isCampaignIDexists (setupRef_df, "FileName", name)     # True/False, False means file does not exist in DB 
        
    # check if the current campaign id exists in RPT.campaigns
    campaignCheck = isCampaignIDexists (campaignRef_df, "CampaignID", fileCampaignId)   # bool, True=campaignid exists in DSS.Campaigns
    # check if the current campaign id exists in RPT.AggregateSales
    aggregateCheck = not isCampaignIDexists (aggregateRef_df, "CampaignID", fileCampaignId) # boolean, False is good > apply NOT
    # check if the current campaign id exists in RPT.StoreSales
    storeCheck = not isCampaignIDexists (storeRef_df, "CampaignID", fileCampaignId)     # boolean, False is good > apply NOT 
    
    insertCheckDict = {"campaigns":campaignCheck,
                       "agrgegates":aggregateCheck,
                       "stores":storeCheck,
                       "setup":setupCheck} # create dict to track where to insert
    
    return insertCheckDict

    
def getFiles (sourceConn, targetConn, shareDir, tableRefDict):
    
    # 
    # Inputs: 
    # Shared drive with campaign setup files 
    # python dictionary with the target table and schema info 
    # DB Connection to source DB
    # DB Connection to target DB
    #
    
    # Create reference dataframes to check against current data in DSc DB
    createRefDFDict = createRefDFs(sourceConn, targetConn) # dict to store output dataframes
    
    campaignRef_df = createRefDFDict["campaigns"] # get reference dataframe from DSS.campaigns
    aggregateRef_df = createRefDFDict["aggregates"] # get reference dataframe from RPT.AggregateSales
    storeRef_df = createRefDFDict["stores"] # get reference dataframe from RPT.StoreSales
    setupRef_df = createRefDFDict["setup"] # get reference dataframe from RPT.CampaignSetup
    campaignProductsRef_df = createRefDFDict["campaignproducts"] # get reference dataframe from RPT.vwCampaignProducts
    
    # read files
    #
    for subdir, dirs, files in os.walk (shareDir):
        
        for name in files:
            
           
            if name.endswith (".csv"):  

                # df to insert into RPT.CampaignSetup
                fileSetup_df = createCampaignSetup (shareDir, subdir, name) 
                fileCampaignId = fileSetup_df.iloc[0]["CampaignID"] # Campaign id in filename
                fileFullPath = fileSetup_df.iloc[0]["FileFullPath"] # full path of file to be processed
                
                insertCheckDict = createCheckList (setupRef_df, campaignRef_df, aggregateRef_df, storeRef_df, name, fileCampaignId)
                setupFileCheck = insertCheckDict ["setup"] # boolean, is a new campaign setup file available?
                campaignCheck = insertCheckDict ["campaigns"]  # boolean, is a corresponding campaignKey available in prod?

                # if campaignid does not exist in dss.campaigns yet                
                if campaignCheck == True: 
                    
                    try:
                        # if this is a new file then insert in rpt.campaignsetup, rpt.aggregatesales and rpt.storesales
                        if setupFileCheck == True: # = found new file!
                            try:
                                print ("\n","*" * 50)
                                print (".... Found new file to insert:", name)
                                print (".... CampaignId:", fileCampaignId, " exists in the DSS.Campaigns table ")
                                processFile (targetConn, name, fileFullPath, fileSetup_df, campaignRef_df, campaignProductsRef_df, tableRefDict)
                            except:
                                print (".... error in processing file:", name)
                                raise
                                return
                    except:
                        raise
                        return
                else:
                    print (".... CampaignId:",fileCampaignId," does not exist in the DSS.Campaigns table. Quitting ...")
                    
    return               

def createHashKey (inStr):
    
    hash_object = hashlib.md5(inStr.encode())
    
    return hash_object.hexdigest()


def addMetaData (df, name):

    # Adds standard metadata fields to dataframe before loading to DataBase    
    df.is_copy = False
    df["UpdatedBy"]="AnsaReports"
    df["UpdatedOn"]=datetime.datetime.now()                             
    df["setupFileKey"]=createHashKey(name)

    return df

def processFile (conn, name, filename, fileRef_df, campaignRef_df, campaignProductsRef_df, insertDict):
    
            #
            # This function is called only when there is a new campaign setup file is available  
            # 
            # Inputs: 
            # Full path of the file to extract
            # name of the file
            # Setup file reference df to insert into RPT schema 
            # Campaign reference df to lookup campaignKey from CampaignID 
            # python dictionary with the target table and schema info 
            #

            print (".... running processfile for:", filename)
            
            # 1> Read file into dataframe
            try:
                                
                input_df = pd.read_csv (os.path.normpath(filename), header = 0, names = ["UTCRunTime","CampaignID","BeginDate","EndDate","AggregateType","AggregateName","Store","FirstScanDate","LastScanDate","UniqueItemsScanning","SalesAmount"], usecols = [0,1,2,3,4,5,6,7,8,9,10], dtype = {"Store": str}, parse_dates=["FirstScanDate","LastScanDate"])
                
                print (".... read csv for:", name)
            except:
                print (".... Cannot process file:", os.path.normpath (filename))
                raise
                return            

            
            # 4> Merge input dataframe with campaignkey  and clean up AggregateType                          
            try:
                scrubbed_df = processDF (campaignRef_df, input_df, campaignProductsRef_df)
                print (".... merged store summary with lookup table for:", name)
            except:
                print (".... Cannot merge store summary to lookup file:")
                raise
                return
            
            # 2> summarize dataframe, by Store + aggregates
            try:
                storeSmry_df = summarizeDF (scrubbed_df, "S")
                print (".... store level summary complete for:", name)
            except:
                print (".... error in store level summarization")
                raise
                return

            # 3> summarize dataframe, by aggregates only
            try:
                aggSmry_df = summarizeDF (scrubbed_df, "A")
                print (".... aggregate level summary complete for:", name)
            except:
                print (".... error in aggregate level summarization")
                raise
                return

            
            # 7> Append Primary Key to all 3 dataframes before inserting            
            fileRef_df = addMetaData (fileRef_df, name)
            # Add metadata columns [StoreSales].updatedOn and [StoreSales].updatedBy
            storeSmry_df = addMetaData (storeSmry_df, name)
            # Add metadata columns [AggregateSales].updatedOn and [AggregateSales].updatedBy
            aggSmry_df = addMetaData (aggSmry_df, name)
            
            # 8> Insert Campaign Setup dataframe into target table RPT.CampaignSetup                           
            try:
                table = insertDict["setup"][0]
                schema = insertDict["setup"][1]
                _df=fileRef_df[["CampaignID", "CustomerCode", "FileName", "FileCreatedOn", "FileUpdatedOn", "UpdatedOn", "UpdatedBy", "setupFileKey"]]
                # select the correct set of columns to insert into RPT.CampaignSetup
                databaseMethods.insertRecs(conn, table, schema, _df)
                print (".... Inserted campaign setup recs into target table for:", name)
            except:
                print (".... Cannot insert campaign setup recs into target table:")
                raise
                return
                
                
            # 9> Insert Store summary dataframe into target table RPT.StoreSales                           
            try:
                table = insertDict["store"][0]
                schema = insertDict["store"][1]
                _df = storeSmry_df[["CampaignKey", "CampaignID", "StoreId", "AggregateType", "AggregateName", "FirstScanDate", "LastScanDate", "SalesAmount", "UpdatedOn", "UpdatedBy", "setupFileKey"]]
                databaseMethods.insertRecs(conn, table, schema, _df)
                print (".... Inserted store summary recs into target table for:", name)
            except:
                print (".... Cannot insert store summary recs into target table:")
                raise
                return

            # 10> Insert Aggregate summary dataframe into target table RPT.AggregateSales 
            try:
                table = insertDict["aggregate"][0]
                schema = insertDict["aggregate"][1]
                df = pd.DataFrame(aggSmry_df.to_records()) # multiindex become columns and new index is integers only
                _df = df[["CampaignKey", "CampaignID", "AggregateType", "AggregateName", "FirstScanDate", "LastScanDate", "SalesAmount", "UpdatedOn", "UpdatedBy", "setupFileKey"]]
                databaseMethods.insertRecs(conn, table, schema, _df)
                print (".... Inserted aggregate summary recs into target table for:", name)
            except:
                print (".... Cannot insert aggregate summary recs into target table:")
                raise
                return

            print (".... completed processing of:", filename)
    
            return 


if __name__ == '__main__':

    print ("\n","<<<< Checking for new Campaign Setup FIles >>>>") 
    # initialize global variables
    initialize.readConfigFile()
    
    # tableRefDict = {"Summary Type": [Database Table, Database Schema]
    tableRefDict = {"aggregate": ["AggregateSales", "RPT"],
                  "store": ["StoreSales", "RPT"],
                  "setup": ["CampaignSetup", "RPT"],
                 } 
    
    # open DB connections
    sourceConn = databaseMethods.openDBConnection("source")    
    targetConn = databaseMethods.openDBConnection("target")    
    
    getFiles (sourceConn, targetConn, initialize.setupFileShare, tableRefDict) 
    
    # close DB connections
    sourceConn.close()
    targetConn.close()
