import hashlib
import json
#import commentjson as json
import logging
import os
import tempfile

import pandas as pd
import pyodbc

from datetime import datetime

import numpy as np
import pandas as pd
import pymssql

import initialize 
import databaseMethods

##########################################
# writing.py
##########################################


def _pretty_feature(col):
    
    PRETTY_COLUMN_NAMES = {
        'High Above4PersonHH': r'High % 4+ Person Households',
        'High African': r'High % African-American Population',
        'High Age18To34': r'High % Population 18-34',
        'High Age35To49': r'High % Population 35-49',
        'High Age50To64': r'High % Population 50-64',
        'High AgeAbove65': r'High % Population 65+',
        'High AgeUnder18': r'High % Population LT 18',
        'High Asian': r'High % Asian Population',
        'High HasChildrenUnder18': r'High % Households with Children',
        'Low HasChildrenUnder18': r'Low % Households with Children',
        'High HispanicOrLatino': r'High % Hispanic/Latino Population',
        'Low HispanicOrLatino': r'Low % Hispanic/Latino Population',
        'High MedianIncome': r'High Average Income',
        'Low MedianIncome': r'Low Average Income',
        'High NowMarried': r'High % Married Population',
        'Low NowMarried': r'Low % Married Population',
        'High OnePersonHH': r'High % 1-Person Households',
        'High PopulationDensity': r'High Population Density',
        'Low PopulationDensity': r'High Population Density',
        'High SomeCollegeOrMore': r'High % College Education or Greater',
        'Low SomeCollegeOrMore': r'Low % College Education or Greater',
        'High TwoToThreePersonHH': r'High % 2-3 Person Households',
        'High White': r'High % White Population',
    }


    return PRETTY_COLUMN_NAMES.get(col, col)


SALES_TYPES = {
    'New Item': 'SAI',
    'Existing Item': 'SPR'
}

def log_results(conn, df, groups, campaign_id, campaign_type, msg='Results'):
    
    '''
    Print a summary of the results.

    Displays a table of the provided store groups, compared to the overall
    SPR.

    Args:
        df: Input dataframe.
        groups: List of HighLowStoreGroup objects.
        campaign_name: To show with results.
    '''
    
    median_sales = df.Sales.median()
    groups = sorted(groups, key=lambda g: g.sales, reverse=True)
    sales_type = SALES_TYPES[campaign_type]

    results = '\n{}:\n'.format(msg)
    results += '    Campaign: {}\n'.format(campaign_id)
    results += '    Number of stores: {}\n'.format(len(df))
    results += '    Median campaign {t}: {m:.2f}\n'.format(t=sales_type, m=median_sales)

    results += '    Groups of stores with high performance:\n'
    for group in groups:
        feature_fill = max(len(_pretty_feature(g.feature_name)) for g in groups) + 2
        results += '        {f:<{ff}} {v:4.2f} ({i:04.1f})\n'.format(
            f=_pretty_feature(group.feature_name),
            ff=feature_fill,
            v=group.sales,
            i=abs(group.index),
        )
        
    median_sales = df.Sales.median()
    groups = sorted(groups, key=lambda g: g.sales, reverse=True)
    sales_type = SALES_TYPES[campaign_type]
    values = [g.index for g in groups]
    features = [_pretty_feature(g.feature_name) for g in groups]
    values, features = (list(t) for t in zip(*sorted(zip(values, features))))
    values = np.array(values)

    output_df = pd.DataFrame({'featureName': features, 'featureValue': values})    
    output_df['CampaignID']=campaign_id
    output_df['UpdatedOn']=datetime.now()   
    output_df['UpdatedBy']="Malli Biswas"
    
    # write to database RPT.HighPerformingStores
    databaseMethods.insertRecs(conn, "HighPerformingStores","RPT",output_df)
    
    
    return


##########################################
#  reading.py
#########################################

# These params are documented in the CLI help in __main__.py
DEFAULT_PARAMS = {
    'cache_sql_queries': False,
    'dsn': r'ignore: Cannot use text sting in open scripts;',
    'max_n_groups': 4,
    'plot_results': False,
    'log_level': 'INFO',
}

REQUIRED_PARAMS = ['campaign_id']
OPTIONAL_PARAMS = ['output_folder']

# A subset of the columns in dbo.FullAttributesDemographics. Some columns are
# excluded from this due to small values, a narrow range of values, or for 
# being unlikely to explain SPR.
DEMOGRAPHIC_COLS = [
    'White',
    'African',
    'Asian',
    'HispanicOrLatino',
    'SomeCollegeOrMore',
    'OnePersonHH',
    'TwoToThreePersonHH',
    'Above4PersonHH',
    'MedianIncome',
    'ChildrenUnder6Only',
    'Children6To17',
    'ChildrenUnder6And6To17',
    'AgeUnder5',
    'Age5To9',
    'Age10To17',
    'Age18To34',
    'Age35To49',
    'Age50To64',
    'AgeAbove65',
    'NowMarried',
    'PopulationDensity',
] 

# Timeout to connect to the database (i.e. not for queries).
DB_TIMEOUT = 2  # Seconds

CACHE_DIR = os.path.join(tempfile.gettempdir(), 'highperformingstores')


def load_params(filepath):
    
    '''
    Loads and validates the parameters file.

    Parses the file containing the parameters for the run. Also ensures all 
    required values are present, and fills missing values with defaults.

    Args:
        filepath: path to params.json file.

    Returns:
        params: a dict of parameters.
    '''
    
    try:
        with open(filepath) as f:
            params = json.load(f)
    except FileNotFoundError as e:
        raise e
    except ValueError as e:
        raise e

    # Ensure required params are present.
    missing_params = [p for p in REQUIRED_PARAMS if p not in params]
    if missing_params:
        msg = 'The following required parameters were missing: '
        msg +=  ', '.join(missing_params)
        raise ValueError(msg)

    # Enforce output saving logic 
    if 'plot_results' in params:
        if 'output_folder' not in params:
            msg = 'Output folder must be provided if saving or plotting results'
            raise ValueError(msg)

    # Check no extra params are provided
    valid_params = REQUIRED_PARAMS + OPTIONAL_PARAMS + list(DEFAULT_PARAMS.keys())
    bad_params = [p for p in params.keys() if p not in valid_params]
    if bad_params:
        msg = 'Invalid parameters provided: {}'.format(bad_params)
        raise ValueError(msg)


    params = dict(DEFAULT_PARAMS, **params)  # Overwrite defaults with params

    return params


def read_sql(query, conn, cache=False, **kwargs):
    
    '''
    Save the results of an SQL query to a pandas dataframe.

    A wrapper around pandas.read_sql that handles caching and DB connection.

    Args:
        query: The SQL query as a string.
        dsn: A DSN string for connecting to the database (see default
            params for an example).
        cache: Whether to save and load identical queries to disk.
        kwargs: Other arguments to be passed to pandas.read_sql.

    Returns:
        df: A pandas dataframe of the results.
    '''
   
    if cache:
        key_to_hash = '|'.join([json.dumps(kwargs), conn, query])
        key = hashlib.md5(key_to_hash.encode()).hexdigest()[:10]
        filename = 'hps-sql-{}.p'.format(key)
        cache_path = os.path.join(CACHE_DIR, filename)
        if os.path.isfile(cache_path):
            return pd.read_pickle(cache_path)

    try:
        df = pd.read_sql_query(query, conn, **kwargs)
    except pyodbc.Error as e:
        raise e
    except pd.io.sql.DatabaseError as e:
        raise e

    if cache:
        os.makedirs(CACHE_DIR, exist_ok=True)  # to_pickle failes if the directory doesn't exist
        df.to_pickle(cache_path)

    return df



def load_data(campaign_id, campaign_type, aggregate_name, conn):
    
    '''
    Get all the store store needed.

    Fetches and merges demographic and campaign data.

    Currently the historical aggregate sales are fetched, computed, and merged
    separately, but the hope is that they will eventually come from the
    database too.

    Args:
        campaign_id: String from params.
        aggregate_name: Which aggregate to use for sales and SPR data.
        dsn: String from params.

    Returns:
        df: DataFrame where each row is a store. 
    '''

    demographic_cols = sorted(DEMOGRAPHIC_COLS)  # For more consistent caching
    formatted_cols = ', '.join('dm.' + c for c in demographic_cols)

    # Build the query for post-campaign SPR, and demographic data. Because the
    # demographic data incorrectly casts StoreId to an integer, a hack is used
    # to account for the lost leading zeroes.
    query = '''
        SELECT
            sp.StoreID,
            dm.StoreID as dm_store_id,
            sp.StorePerformanceRatio,
            sp.SalesAmountIndex,
            {demographic_cols}
        FROM RPT.vwDS_StorePerformanceByProductAggregate as sp
        LEFT OUTER JOIN dbo.RSIDemographics  as dm
            ON sp.StoreId  = dm.StoreId
                AND dm.Retailer = (SELECT TOP(1) RetailerName FROM DSS.Campaigns WHERE CampaignID = '{campaign_id}')
            WHERE sp.CampaignID = '{campaign_id}' AND sp.AggregateName = '{aggregate_name}'
        ;
    '''.format(
        demographic_cols=formatted_cols,
        campaign_id=campaign_id,
        aggregate_name=aggregate_name
    )

    df = read_sql(query, conn)

    if len(df) == 0:
        print ('... This campaign has no stores')
        raise ValueError('... This campaign has no stores')

    if campaign_type == 'New Item':
        df['Sales'] = df.SalesAmountIndex
    elif campaign_type:
        df['Sales'] = df.StorePerformanceRatio
    else:
        msg = 'I don\'t know how to handle campaign type: {}'.format(campaign_type)
        print (msg)
        raise ValueError(msg)
    

    df.set_index('StoreID', inplace=True, verify_integrity=True)  # verify_integrity prevents duplicates (which are not enforced by DB)
    return df



def get_featured_aggregate_details(campaign_id, conn, cache, aggregate_name=None):
    
    '''
    Details of the total featured aggregate for a campaign.

    For campaigns with multiple different featured aggregates, each product
    may be included in 2 aggregates: the actual one (e.g., Featured1) and a
    'meta-aggregate' (e.g., TotalFeatured). This extracts the overall featured
    aggregate, by chossing the one with the most products.

    Some other useful information about the campaign nad aggregate is also
    included.

    Args:
        campaign_id: String from params.
        dsn: String from params.
        cache: Whether to cache queries to disk.

    Returns:
        row: Pandas series with a keys: AggregateName, ProductKey, CampaignName
    '''

    query = '''
        SELECT TOP(1) CP.AggregateName, CP.ProductKey, C.CampaignName
        FROM DSS.vwCampaignProducts AS CP
        JOIN DSS.Campaigns AS C ON CP.CampaignKey = C.CampaignKey
        WHERE C.CampaignId = '{}' AND CP.AggregateType = 'Featured'
        ORDER BY LEN(CP.AggregateMember) DESC
        ;
    '''.format(campaign_id)

    df = read_sql(query, conn, cache=cache)
    if len(df) == 0:
        print ('No aggregate found for campaign {}'.format(campaign_id))
        raise ValueError('No aggregate found for campaign {}'.format(campaign_id))

    row = df.ix[0]
    return row



def get_campaign_details(campaign_id, conn):
    
    '''
    Details about a campaign.

    For campaigns with multiple different featured aggregates, each product
    may be included in 2 aggregates: the actual one (e.g., Featured1) and a
    'meta-aggregate' (e.g., TotalFeatured). This extracts the overall featured
    aggregate, by chossing the one with the most products.

    Some other useful information about the campaign nad aggregate is also
    included.

    Args:
        campaign_id: String from params.
        dsn: String from params.

    Returns:
        row: Pandas series with a keys: AggregateName, ProductKey, CampaignName
    '''
    query = '''
        SELECT TOP(1) 
            CP.AggregateName AS FeaturedAggregateName,
            C.CampaignName,
            C.CampaignType
        FROM DSS.vwCampaignProducts AS CP
        JOIN DSS.Campaigns AS C ON CP.CampaignKey = C.CampaignKey
        WHERE C.CampaignId = '{}' AND CP.AggregateType = 'Featured'
        ORDER BY LEN(CP.AggregateMember) DESC
        ;
    '''.format(campaign_id)
    df = read_sql(query, conn)
    
    if len(df) == 0:
        msg = 'Campaign details not found.'
        print (msg)
        raise ValueError(msg)

    # Case-insensitive databses are such a terrible idea, so much wasted time sorting this out.
    df.CampaignType = df.CampaignType.map(lambda x: x.title())

    row = df.ix[0]
    return row 


##########################################
# processing.py
##########################################

COMBINED_CATEGORIES = {
    'AgeUnder18': ['AgeUnder5', 'Age5To9', 'Age10To17'], 
    'HasChildrenUnder18': ['ChildrenUnder6Only', 'Children6To17', 'ChildrenUnder6And6To17'], 
}



def _combine_columns(df, source_cols, dest_col, how=sum):
    
    '''
    Reduce several columns into one.

    Applies a funcion row-wise to selected columns of a dataframe, saving the 
    result in a new column. The source columns are then deleted.

    Args:
        df: The input dataframe, which is not modified.
        source_cols: List of columns to combine and delete if the dest_col is 
            included, it won't be deleted.
        dest_col: Name of the column to store the result. It will be created
            if it doesn't exist.
        how: Function used to combine the cols.
    '''
    df = df.copy()
    df[dest_col] = df[source_cols].apply(how, axis=1)
    cols_to_drop = [c for c in source_cols if c != dest_col]
    df.drop(cols_to_drop, axis=1, inplace=True)
    return df    


def combine_weak_categories(df, combinations=COMBINED_CATEGORIES):
    '''
    Merge small or irrelevant demographic features together.

    Args:
        df: Input DataFrame.
        combinations: A dictionary where keys are the names of the new columns,
            and values are lists of columns to be combined and removed.

    Return:
        df: A copy of the input df, with merged features.
    '''

    for new_col, old_cols in combinations.items():
        df = _combine_columns(df, old_cols, new_col)
        
    return df


def handle_missing_data(df):
    '''
    Log and fix missing data in the dataset.

    Data can go missing either because a store is missing in one table of the
    join, or when the store exists but has a null value for a particular
    column.

    Rows without SPR are dropped, as these are the only Y variable. Other 
    missing values are left alone.

    Args:
        df: Input DataFrame

    Returns:
        df: Copy of input df with missing data handled.
    ''' 
    df = df.copy()

    # Missing stores
    n_stores = len(df)  
    n_missing_dm_stores = n_stores - df.dm_store_id.count()


    # Drop rows without sales (our Y variable)
    n_nan_value = df.Sales.isnull().sum()
    if n_nan_value:
        msg = '{} stores have missing sales data.'.format(n_nan_value)
        msg += ' These rows will be dropped.'
        df.dropna(axis=0, inplace=True, subset=['Sales']) 

    # Just warn for other features
    n_nan_rows = len(df[df.isnull().any(axis=1)])
    if n_nan_rows:
        n_nans = len(df) - df.count()
        msg = '{} stores have some null cells.'.format(n_nan_rows)
        msg += ' The worst column is \'{}\' with {} nan values.'.format(n_nans.idxmax(), n_nans.max())

    return df



##########################################
# analysis.py
##########################################

LOW_QUANTILE = 25
HIGH_QUANTILE = 75
MAX_N_GROUPS = 4

# We don't want to show negative results, or results that are rounded to 0 
# when displayed.
INDEX_THRESHOLD = 101

LOW_CATEGORIES = ['PopulationDensity', 'HasChildrenUnder18', 'SomeCollegeOrMore', 'MedianIncome']
SALES_COLUMNS = ['StorePerformanceRatio', 'SalesAmountIndex', 'Sales']


class HighLowStoreGroup:
    
    '''
    A collection of stores, high or low in some feature.

    Attributes:
        feature: The column name of the feature used to split the stores.
        is_high: A boolean indicating whether the group is high in the given 
            feature (otherwise low).
        threshold: Float value where the stores were split from the rest.
        spr: Median spr of the group.
    '''

    def __init__(self, feature, df, is_high):
        
        '''
        Makes a store group.

        np.nanpercentile us used for the quantiles, as it igores null values.

        Args:
            feature: Column name to split on.
            df: Input DataFrame.
            is_high: Boolean indicating whethier the group is high (otherwise 
                low).
        '''
        self.is_high = is_high
        self.feature = feature
        
        if self.is_high:
            values = df[feature]
            self.threshold = np.nanpercentile(values, HIGH_QUANTILE)
            self._store_mask = df[feature] >= self.threshold
        else:
            values = df[feature]
            self.threshold = np.nanpercentile(values, LOW_QUANTILE)
            self._store_mask = df[feature] <= self.threshold
            
        self.sales = df[self._store_mask].Sales.median()
        self._df_median_sales = df.Sales.median()

        
    @property
    def index(self):
        '''
        Group SPR/SAI, compared to all stores.

        Percentage difference between the median spr of the group, and the 
        median spr of all stores.

        The formula is the difference divided by the median of all stores.
        Positive values are high-performing stores, and negative values are low
        performing stores.
        '''
        return self.sales / self._df_median_sales * 100
        # if self.spr > self._df_median_sales:
        #     return self.spr / self._df_median_sales * 100 - 100
        # else:
        #     return -1 * (self._df_median_sales / self.spr * 100 - 100)
        
    @property
    def feature_name(self):
        '''Adds a high/low label so the feature name.'''        
        label = 'High ' if self.is_high else 'Low '
        return label + self.feature


def build_store_groups(df):
    '''
    Make high and low store groups for each feature.

    Groups are made for every feature, except Y variable
    StorePerformanceRatio.

    Args:
        df: Input DataFrame.

    Returns:
        groups: list of HighLowStoreGroup objects.
    '''
    features = [f for f in df.columns if f not in SALES_COLUMNS]

    groups = []
    for feature in features:
        high_group = HighLowStoreGroup(feature, df, is_high=True)
        groups.append(high_group)
        if feature in LOW_CATEGORIES:
            low_group = HighLowStoreGroup(feature, df, is_high=False)
            groups.append(low_group)
    
    return groups


def filter_store_groups(groups):
    '''
    Choose the store groups with the best spr.

    The top n are chosen, then limited to those above 0.

    Args:
        groups: List of HighLowStoreGroups

    Returns:
        filtered_groups: List of HighLowStoreGroups with highest relative SPR.
    '''
    groups = groups.copy()
    groups.sort(key=lambda g: (g.sales, g.feature_name), reverse=True)
    sorted_groups = [g for g in groups if not np.isnan(g.sales)]  # Python sorting freaks out with nans, which can happen if a dempgraphic variable is missing
    filtered_groups = sorted_groups[:MAX_N_GROUPS]
    thresholded_groups = [g for g in filtered_groups if g.index > INDEX_THRESHOLD]

    if len(thresholded_groups) < MAX_N_GROUPS:
        top_n_sales = [g.sales for g in filtered_groups]
        msg = 'Unable to find {n} good groups. Top sprs/sais are {s}, with a threshold of {t}. Returning {g} groups.'.format(
            n=MAX_N_GROUPS,
            s=top_n_sales,
            t=INDEX_THRESHOLD,
            g=len(thresholded_groups)
        )

    return thresholded_groups


#################################
# main.py
#################################

def processCampaign (campaignId, campaignType, conn):
    
    agg = get_featured_aggregate_details(campaignId, # campaign id  
                                         conn= conn, 
                                         cache=False) #cache_sql_queries = False
    
    df = load_data(
        campaign_id=campaignId,
        campaign_type=campaignType,
        aggregate_name=agg.AggregateName,
        conn=conn,
    )

    
    # Processing
    df = combine_weak_categories(df)
    
    df = handle_missing_data(df)
    
    # Remove cols only used for processing
    df.drop(['dm_store_id'], axis=1, inplace=True)
    # Find the high and low stores
    all_groups = build_store_groups(df)

    top_groups = filter_store_groups(all_groups)
    
    log_results(conn, df, top_groups, campaignId, campaignType)
    
    return



def getMasterCampaignList (conn):
    
    # pick campaigns that ended in the last 14 days and have not been processed yet
    query = "select distinct a.CampaignId, c.CampaignType, c.CampaignStatus, c.EndDate  from RPT.AggregateSales a, DSS.Campaigns c  \
    where a.CampaignKey = c.CampaignKey  and c.customerkey not in (5,7,8)  \
    and c.CampaignStatus not in ('Cancelled','OnHold') \
    and c.EndDate >= getdate() - 180 \
    and c.EndDate < getdate() \
    and c.CampaignId not in (select distinct CampaignId from RPT.HighPerformingStores);"    

    # read query output into dataframe
    df = pd.read_sql(query, conn)
        
    tuples = [tuple(x) for x in df.values]        
    
    return tuples


def main(): 
    
    print ("\n","<<< Refreshing High Performing Stores >>>")
    
    # initialize and read dict of global variables
    initialize.readConfigFile()
    targetConn = databaseMethods.openDBConnection("target")    
    
    print ("Fetching list campaigns for analysis ...")
    campaigns = getMasterCampaignList (targetConn)  # get campaign list from DataScience DB
        
    cache_sql_queries = False
    
    for campaign_tuple in campaigns:
        try:
            print(">>>> Processing:",campaign_tuple)
            processCampaign (campaign_tuple[0],campaign_tuple[1], targetConn)
        except:
            print(".... Skipping ",campaign_tuple[0])
            pass


if __name__ == '__main__':
    
    
    main()
