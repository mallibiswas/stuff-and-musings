#!/bin/sh

fileExt=$(date +%Y%m%d_%H:%M:%S)
logPath=$HOME/scripts/prod/ETL/log
scriptPath=$HOME/scripts/prod/ETL
binPath=$HOME/anaconda3/bin
syncDBScript=mainSyncDB.py
methodologyScript=DisplayMethodologyLookup.py
campaignSetupScript=SummarizeCampaignSetupFiles.py
hiperfStoresScript=hiperfstores.py
outlierTacticsScript=Outlier_Tactics.py
logFile=DataScienceDB_$fileExt.log  
configFile=$scriptPath/config.json
decryptKey='13fd5<ca%0ec97cf1d|1b7fz'
statusFile=$HOME/scripts/prod/MAIN/data/JOBS/KF_jobStatus.csv


#
# Scripts kicks off the following jobs:
# 1. Sync DataScience DB to Prod
# 2. Calculate Methodology selected for new campaigns in DataScience DB 
# 3. Report on High Performance Stores in DataScience DB
# 4. Load Campaign Setup files in DataScience DB
#

echo '>>>>' begin sync analytics DB on $(date) > $logPath/$logFile

starttm=$(date +%x:%T)
$binPath/python $scriptPath/$syncDBScript $configFile  $decryptKey  >> $logPath/$logFile 
rc1=$?
endtm=$(date +%x:%T)
echo 'sync DB',$syncDBScript,$starttm,$endtm,$rc1 >> $statusFile

starttm=$(date +%x:%T)
$binPath/python $scriptPath/$outlierTacticsScript $configFile  $decryptKey  >> $logPath/$logFile 
rc2=$?
endtm=$(date +%x:%T)
echo 'Outlier Tactics',$outlierTacticsScript,$starttm,$endtm,$rc2 >> $statusFile

starttm=$(date +%x:%T)
$binPath/python $scriptPath/$hiperfStoresScript $configFile  $decryptKey  >> $logPath/$logFile 
rc3=$?
endtm=$(date +%x:%T)
echo 'HiPerfStores',$hiperfStoresScript,$starttm,$endtm,$rc3 >> $statusFile

starttm=$(date +%x:%T)
$binPath/python $scriptPath/$campaignSetupScript $configFile  $decryptKey  >> $logPath/$logFile 
rc4=$?
endtm=$(date +%x:%T)
echo 'Campaign Setup',$campaignSetupScript,$starttm,$endtm,$rc4 >> $statusFile

echo '>>>>' end sync analytics DB on $(date) >> $logPath/$logFile

exit 
