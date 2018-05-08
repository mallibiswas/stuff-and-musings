#/!bin/python3.x

import initialize 
import syncInsertUpdate
import os
import initialize

#########################    
# Main program
#########################    


if __name__ == '__main__':
    
    # initialize global variables in initialize.readConfigFile()
    initialize.readConfigFile()
    
    # get path+filename to dictionary
    dictFilename = os.path.join(initialize.dataDirectory, initialize.dictFile)
    
    # read dictionary
    y = syncInsertUpdate.readDictionary(dictFilename)

    syncInsertUpdate.initializeSync (y)
    
            
#    is this needed?    
#    syncTargetTable ('CampaignHistoricalValues',   'DSS', 'DSS', "UpdatedOn",  "HistoricalKey",     True, '60', prodConn, dscConn)
#    Exception: No updatedOn field in this table
