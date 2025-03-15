from peanuts.AML.orion import *
# pip install ~/cloudfiles/code/software/PyPeanuts/latest/peanuts-20240226.0-py3-none-any.whl --force-reinstall
# import peanuts
# from peanuts.AML.orion import *
import pandas as pd
pd.set_option('display.max_columns', None)

import os
from deltalake import DeltaTable

from tableone import TableOne

# basic packages
import re
import numpy as np
import pandas as pd

from scipy import stats

# setting the column widths
pd.set_option('display.max_colwidth', None)

import sys 

#from catboost import CatBoostRegressor, Pool
# from sklearn.model_selection import train_test_split    # pip install scikit-learn
import matplotlib.pyplot as plt

import xlsxwriter

print("Imported successfully!")



from datetime import date, datetime, timedelta, timezone
import time
import pytz

# filedate = datetime.now(pytz.timezone('US/Central')).strftime("%Y-%m-%d").replace("-", "_")
# datetime.now(pytz.timezone('US/Central')).strftime("%Y-%m-%d %H:%M:%S") # Central time'

def data_pull(nps_start, nps_end, functional_account_id, password=None):

    # instantiate Orion class
    if functional_account_id == 'None':
        print(f'Using user id {user_id} to pull the data from MOSAIC')
        orion = Orion(user=user_id)
    else:
        print(f'Using functional account {functional_account_id} to pull the data from MOSAIC')
        orion = Orion(functional_account=functional_account_id, password=password)
        
    start = time.time()
    print('Pulling Data...')

    
    query = f'''
    WITH RankedRecords AS (
        SELECT 
            A.SURVEY_PARTY_ID AS PARTY_ID,
            A.SEG_DEP_DT,
            A.NPS_CATEGORY,
            A.JOURNEY_DELAY_CAT,
            A.JOURNEY_DELAY_MINS,
            A.DISRUPTION_IND,
            A.TRANSACTOR_STAGE,
            A.AADV_TENURE,
            CASE
                WHEN A.TRANSACTOR_STAGE = '2' THEN '2 AADV'
                WHEN A.TRANSACTOR_STAGE = '1' THEN '1 No membership'
                WHEN A.TRANSACTOR_STAGE = '3' THEN '3 AADV+Card'
                ELSE A.TRANSACTOR_STAGE
            END AS TRANSACTOR_STAGE2,
            A.GENERATION,
            
            
            B.CUSTOMER_GROUP_CD,
            B.MATCH_IND,
            B.AA_FLT_REV_IND,
            B.EP_OA_SPEND_IND,
            B.EP_NATIONAL_AIRLN_SPEND_IND,
            B.EP_BUDGET_AIRLINES_SPEND_IND,
            B.EP_FOREIGN_OTHER_AIRLN_SPEND_IND,
            B.AA_FLT_REV,
            B.EP_OA_SPEND,
            B.EP_NATIONAL_AIRLN_SPEND,
            B.EP_BUDGET_AIRLN_SPEND,
            B.EP_FOREIGN_OTHER_AIRLN_SPEND,
            B.EST_AIRLN_SPEND,
            B.EST_AA_SOW_PERCENT,
            B.EP_NATIONAL_AIRLN_MERCH,
            B.EP_BUDGET_AIRLN_MERCH,
            B.EP_FOREIGN_OTHER_AIRLN_MERCH,
            B.EST_AIRLN_SPEND_BAND,
            B.EST_AA_SOW_PERCENT_BAND,
            B.EST_AIRLN_SPEND_TIER,
            B.EST_AA_SOW_PERCENT_TIER,
            
            E.AA_FLT_REV AS AA_FLT_REV_1stYr,
            E.EST_AIRLN_SPEND AS EST_AIRLN_SPEND_1stYr,
            
            
            ZEROIFNULL(D.TICKET_REV_NON_AWD) + ZEROIFNULL(D.TOTAL_ANCILLARY_REV) AS AA_FLT_REV_Y2,
            D.FLIGHT_ICC AS FLIGHT_ICC_Y2,
            D.TICKET_REV + D.TOTAL_ANCILLARY_REV AS FLIGHT_REVENUE_Y2,
            D.CURRENCY_ICC AS CURRENCY_NET_REVENUE_Y2,
            (D.TICKET_REV + D.TOTAL_ANCILLARY_REV + D.CURRENCY_ICC) AS TOTAL_REVENUE_Y2,
            
            ZEROIFNULL(C.TICKET_REV_NON_AWD) + ZEROIFNULL(C.TOTAL_ANCILLARY_REV) AS AA_FLT_REV_Y1,
            C.CUST_TRANSACTOR_TYPE AS CUST_TRANSACTOR_TYPE_ICC,
            C.AP_IND,
            C.TIER_EOP,
            C.no_of_trips,
            C.CUST_CHANNEL_TYPE,
            C.CUST_AADV_TENURE,
            C.TOTAL_ICC,
            C.FLIGHT_ICC AS FLIGHT_ICC_Y1,
            C.CURRENCY_ICC,
            C.TICKET_REV + C.TOTAL_ANCILLARY_REV AS FLIGHT_REVENUE_Y1,
            C.CURRENCY_ICC AS CURRENCY_NET_REVENUE_Y1,
            (C.TICKET_REV + C.TOTAL_ANCILLARY_REV + C.CURRENCY_ICC) AS TOTAL_REVENUE_Y1,

            COALESCE(C.RPM, 0) AS RPM,
            (COALESCE(C.TICKET_REV, 0) + COALESCE(C.TOTAL_ANCILLARY_REV, 0) - COALESCE(C.DISPLACEMENT, 0)) AS flight_rev_displacement,
            CASE 
                WHEN C.HOME_AIRPORT IN ('DFW','DAL','MIA','FLL','PBI','PHL','AZA','PHX','CLT') THEN '1 - PRIMARY HUBS'
                WHEN C.HOME_AIRPORT IN ('EWR','ISP','JFK','LGA','SWF','BUR','LAX','LGB','SNA','MDW','ORD','BWI','DCA','IAD','BOS','AUS') THEN '2 - SECONDARY HUBS + FOCUS CITIES'
                WHEN (C.HOME_AIRPORT NOT IN ('DFW','DAL','MIA','FLL','PBI','PHL','AZA','PHX','CLT','EWR','ISP','JFK','LGA','SWF','BUR','LAX','LGB','SNA','MDW','ORD','BWI','DCA','IAD','BOS','AUS') AND C.HOME_CNTRY = 'US') THEN '3 - DOMESTIC SPOKES'
                WHEN C.HOME_CNTRY <> 'US' AND C.HOME_CNTRY IS NOT NULL AND C.HOME_AIRPORT IS NOT NULL THEN '4 - INTERNATIONAL SPOKES'
                ELSE '5 - UNK' 
            END AS HOME_CITY
            
            
            
            , CASE  
                WHEN C.CUST_TRANSACTOR_TYPE = '2_FLT_EARN_ONLY' THEN 'Stage 2: Flight earning + No Currency earning (AADV)'
                WHEN C.CUST_TRANSACTOR_TYPE = '3_FLT_AND_CURR_EARN' THEN 'Stage 3: Flight & Currency earning (AADV)'
                WHEN C.CUST_TRANSACTOR_TYPE = '1_NON_AADV_FLYER' THEN 'Stage 1: Non-AADV'
                WHEN C.CUST_TRANSACTOR_TYPE = '4_CURR_EARN_ONLY_OR_NO_EARN' THEN 'Stage 4: Currency Earning Only + No Flight Earning (AADV)'
                ELSE 'U' 
              END AS Customer_Stage

            --Tier
            , CASE 
                    WHEN C.AP_IND = 'N' THEN 'Non-AAdvantage' 
                    WHEN C.TIER_EOP = '1_C' THEN '1 Concierge Key' 
                    WHEN C.TIER_EOP = '2_E' THEN '2 Executive Platinum' 
                    WHEN C.TIER_EOP = '3_T' THEN '3 Platinum Pro'       
                    WHEN C.TIER_EOP = '4_P' THEN '4 Platinum' 
                    WHEN C.TIER_EOP = '5_G' THEN '5 Gold'       
                    WHEN C.TIER_EOP = '6_R' THEN '6 Regular'        
                END AS Tier

            -- Age Bands
            , CASE
                    WHEN Y.BIRTH_YEAR_NBR BETWEEN 1928 AND 1945 THEN '1_Silent_Generation'
                    WHEN Y.BIRTH_YEAR_NBR BETWEEN 1946 AND 1964 THEN '2_Baby_Boomers'
                    WHEN Y.BIRTH_YEAR_NBR BETWEEN 1965 AND 1980 THEN '3_Gen_X'
                    WHEN Y.BIRTH_YEAR_NBR BETWEEN 1981 AND 1996 THEN '4_Millennials'
                    WHEN Y.BIRTH_YEAR_NBR BETWEEN 1997 AND 2012 THEN '5_Gen_Z'
                    WHEN Y.BIRTH_YEAR_NBR BETWEEN 2013 AND 2025 THEN '6_Alpha'
                    ELSE '7_Age_band_Not_Available' 
                END AS Age_Band


            --trip intent
            , C.CUST_TRVL_TYPE as Trip_Intent
            --frequency preferred over tier
            , CASE
                    WHEN C.rpm = 0 THEN 'Non-Flyers'
                    WHEN C.no_of_trips =  1 THEN '1 Trip'
                    WHEN C.no_of_trips =  2 THEN '2 Trips'
                    WHEN C.no_of_trips =  3 THEN '3 Trips'
                    WHEN C.no_of_trips >= 4 THEN '4+ Trips'
                    ELSE 'Error'
                END AS Travel_Frequency
            
            
    
            
            ,ROW_NUMBER() OVER (PARTITION BY A.SURVEY_PARTY_ID ORDER BY A.SEG_DEP_DT DESC) AS RowNum
            
            
        FROM 
            DTLAB_DIGITAL_SANDBOX.NPS_DASHBOARD                     A
        LEFT JOIN 
            CIADM_CMPN_LAB.EPSILON_SOW_DETAILED_TBL                 B ON A.SURVEY_PARTY_ID = B.PARTY_ID
        LEFT JOIN
            CIADM_CMPN_LAB.EPSILON_SOW_DETAILED_TBL_YE_06_23        E ON A.SURVEY_PARTY_ID = E.PARTY_ID
        LEFT JOIN 
            RM_CUST_INTELLIGENCE.CIAP_CUST_VIEW_2023_YE06_V05_00    C ON A.SURVEY_PARTY_ID = C.PARTY_ID
        LEFT JOIN 
            RM_CUST_INTELLIGENCE.CIAP_CUST_VIEW_2024_YE06_V05_00    D ON A.SURVEY_PARTY_ID = D.PARTY_ID    
        
        LEFT JOIN 
            PROD_LYLTY_METRICS_VW.INDVDL                            Y ON A.SURVEY_PARTY_ID = Y.PARTY_ID
        
        
        WHERE 
            A.SEG_DEP_DT >= '{nps_start}' AND A.SEG_DEP_DT <= '{nps_end}'
    )
    SELECT *
    FROM RankedRecords
    WHERE RowNum = 1;

        
    '''
    
    # '2022-01-01'
    
    print("Executing: \n", query)
    # creating a dataframe with the pulled data
    df_raw = orion.mq(query)
    print(f"The number of entries pulled is {df_raw.shape[0]}")
    print('\n')
    print("Data pulled in %d seconds" % round(time.time()-start,2))
    df_raw.head()       
    
    return df_raw









def balancing (df4, seed):
    ##################### balancing ratio. check table tt1 for the idea. 

    # Step 1: Create the crosstab with column percentages
    tt1 = pd.crosstab(df4['stratify_key'], df4['NPS_CATEGORY'], normalize='columns')
    tt1 = tt1.reset_index()
    tt1.columns = ['stratify_key', 'Detractor','Passive' , 'Promoter']

    tt2 = pd.crosstab(df4['stratify_key'], df4['NPS_CATEGORY'])
    tt2 = tt2.reset_index()
    tt2.columns = ['stratify_key', 'cDetractor','cPassive', 'cPromoter']

    tt1 = pd.merge(tt1, tt2, left_on='stratify_key', right_on='stratify_key', how='inner')

    # Calculate the row-wise minimum between the 'Detractor' and 'Promoter' columns
    tt1['minp'] = tt1[['Detractor', 'Promoter']].min(axis=1)
    tt1['adjustp'] = tt1['minp']/sum(tt1['minp'])
    # print(sum(tt1['minp']))
    tt1['n_detractor']  = np.floor(len(df4[df4['NPS_CATEGORY'] == 'Detractor']) * tt1['minp']).astype(int)
    tt1['n_promoter' ]  = np.floor(len(df4[df4['NPS_CATEGORY'] == 'Promoter' ]) * tt1['minp']).astype(int)
    tt1['n_passive'  ]  = np.floor(len(df4[df4['NPS_CATEGORY'] == 'Passive' ])  * tt1['minp']).astype(int)

    tt1.set_index('stratify_key', inplace=True)
    tt1.loc[tt1['n_detractor'] > tt1['cDetractor'], 'n_detractor'] = tt1['cDetractor']
    tt1.loc[tt1['n_promoter']  > tt1['cPromoter' ], 'n_promoter']  = tt1['cPromoter']
    tt1.loc[tt1['n_passive']   > tt1['cPassive' ] , 'n_passive']  = tt1['cPassive']

    detractor_sample_counts2 = tt1['n_detractor']
    detractor_sample_counts2.name = 'proportion'

    promoter_sample_counts2 = tt1['n_promoter']
    promoter_sample_counts2.name = 'proportion'
    
    passive_sample_counts2 = tt1['n_passive']
    passive_sample_counts2.name = 'proportion'

    #print(detractor_sample_counts2.head())

    # print(tt1.shape)
    # print(  "1. Sampled Detractors Ratio:", round(100*sum(tt1['n_detractor'])/sum(tt1['cDetractor']), 2), "%")
    # print(  "2. Sampled Passives Ratio:" , round(100*sum(tt1['n_passive'])/sum(tt1['cPassive']), 2), "%")
    # print(  "3. Sampled Promoters Ratio:" , round(100*sum(tt1['n_promoter'])/sum(tt1['cPromoter']), 2), "%")

    tt1.head()
    

    ####################################   detractors
    detractors = df4[df4['NPS_CATEGORY'] == 'Detractor']

    # Initialize an empty list to collect the sampled DataFrames
    sampled_detractors = []

    # Iterate over each 'stratify_key' and the corresponding 'n_detractor' value in tt1
    for key, count in tt1['n_detractor'].items():
        if count > 0:
            # Filter the detractors DataFrame based on the current 'stratify_key'
            subset = detractors[detractors['stratify_key'] == key]

            if not subset.empty:
                # Sample 'count' rows from the subset

                if subset.shape[0] < count : print(key, subset.shape, count)

                sampled_subset = subset.sample(count, replace=False, random_state=seed)

                # Append the sampled subset to the list
                sampled_detractors.append(sampled_subset)

    # Concatenate all sampled subsets into the final DataFrame
    detractor_samples = pd.concat(sampled_detractors)


    print( detractor_samples.shape, "Sampled Detractors Ratio:", round(100*detractor_samples.shape[0]/detractors.shape[0], 2), "%")
    detractor_samples.head()
    
    
        
    ####################################   passives
    passives = df4[df4['NPS_CATEGORY'] == 'Passive']

    sampled_passives = []
    for key, count in tt1['n_passive'].items():
        if count > 0:
            subset = passives[passives['stratify_key'] == key]

            if not subset.empty:

                if subset.shape[0] < count : print(key, subset.shape, count, "check tt1 for issue" )

                sampled_subset = subset.sample(count, replace=False, random_state=seed)

                # Append the sampled subset to the list
                sampled_passives.append(sampled_subset)

    # Concatenate all sampled subsets into the final DataFrame
    passive_samples = pd.concat(sampled_passives)


    print( passive_samples.shape, "Sampled Passives Ratio:", round(100*passive_samples.shape[0]/passives.shape[0], 2), "%")
    passive_samples.head()
    
    
    
    
    ####################################   promoters
    promoters = df4[df4['NPS_CATEGORY'] == 'Promoter']

    sampled_promoters = []
    for key, count in tt1['n_promoter'].items():
        if count > 0:
            subset = promoters[promoters['stratify_key'] == key]

            if not subset.empty:

                if subset.shape[0] < count : print(key, subset.shape, count, "check tt1 for issue" )

                sampled_subset = subset.sample(count, replace=False, random_state=seed)

                # Append the sampled subset to the list
                sampled_promoters.append(sampled_subset)

    # Concatenate all sampled subsets into the final DataFrame
    promoter_samples = pd.concat(sampled_promoters)


    print( promoter_samples.shape, "Sampled Promoters Ratio:", round(100*promoter_samples.shape[0]/promoters.shape[0], 2), "%")
    promoter_samples.head()
    
    
    ##################################### join together
    # Combine the samples
    df5 = pd.concat([detractor_samples, passive_samples, promoter_samples])

    # Drop the temporary stratification column
    df5.drop(columns=['stratify_key'], inplace=True)
    df5 = df5.reset_index(drop=True)
    
    return df5




def extract_tableone_results(tableone_obj):
    """
    Extracts specific results from a TableOne object and saves them to a CSV file.
    Returns:
    None: Saves the filtered results to a CSV file.
    """
    
    outcomes_to_keep = ['n', 'AA_FLT_REV, mean (SD)','AA_FLT_REV_1stYr, mean (SD)'
                        , 'Diff_1to2_YoY, mean (SD)', 'Percent_AA_to_All, mean (SD)'
                        , 'EST_AIRLN_SPEND, mean (SD)','EST_AIRLN_SPEND_1stYr, mean (SD)'
                        , 'TTL_Diff_1to2_YoY, mean (SD)', 'Percent_AA_to_All_1stYr, mean (SD)'
                        , 'Churned, n (%)']
    columns_to_keep = ['Detractor', 'Passive', 'Promoter']
    output_path = 'filtered_tableone.csv'

    df = tableone_obj.tableone

    # Filter rows to keep only specified outcomes
    df_filtered = df[df.index.get_level_values(0).isin(outcomes_to_keep)]

    # Handle the "n" row separately
    if 'n' in df_filtered.index.get_level_values(0):
        n_row = df_filtered.loc[('n', ''), :]  # Extract the "n" row
        overall_n = n_row[('Grouped by NPS_CATEGORY', 'Overall')]  # Extract the Overall n value

        # Replace Detractor's "n" with Overall n and remove Passive/Promoter n
        n_row[('Grouped by NPS_CATEGORY', 'Detractor')] = overall_n
        n_row[('Grouped by NPS_CATEGORY', 'Passive')] = ''
        n_row[('Grouped by NPS_CATEGORY', 'Promoter')] = ''

        # Drop unnecessary columns and reformat
        n_row = n_row.loc[('Grouped by NPS_CATEGORY', columns_to_keep)]
        df_filtered.loc[('n', '')] = n_row  # Replace the original n row

    def format_value(value, row_name):
        if isinstance(value, str):
            if row_name in ['AA_FLT_REV, mean (SD)','AA_FLT_REV_1stYr, mean (SD)'
                        , 'Diff_1to2_YoY, mean (SD)', 'EST_AIRLN_SPEND, mean (SD)','EST_AIRLN_SPEND_1stYr, mean (SD)'
                        , 'TTL_Diff_1to2_YoY, mean (SD)'
                           ]:
                return "$" + value.split()[0]  # Keep values before "("
            elif row_name in ['Percent_AA_to_All, mean (SD)', 'Percent_AA_to_All_1stYr, mean (SD)']:
                return value.split()[0] + " %"  # Keep values inside "(" and add "%"
            elif row_name in [ 'Churned, n (%)']:
                return value[value.find("(") + 1:value.find(")")] + " %"  # Keep values inside "(" and add "%"
        return value

    # Apply formatting based on row name
    df_filtered = df_filtered.apply(lambda row: row.map(lambda x: format_value(x, row.name[0])), axis=1)

    # Adjust for MultiIndex column selection
    df_filtered = df_filtered.loc[:, ('Grouped by NPS_CATEGORY', columns_to_keep)]

    if 'Churned, n (%)' in df_filtered.index:
        df_filtered = df_filtered.loc[df_filtered.index != 'Not Churned']
        
     # Rename row labels
    rename_dict = {
        'n': 'Sample Size',
        'AA_FLT_REV, mean (SD)': 'AA Flight Revenue (2nd year, average)',
        'AA_FLT_REV_1stYr, mean (SD)': 'AA Flight Revenue (1st year, average)',
        'Diff_1to2_YoY, mean (SD)': 'AA Rev YoY Change(average)',
        'Percent_AA_to_All, mean (SD)': 'SOW percentage (2nd year)',
        'EST_AIRLN_SPEND, mean (SD)': 'TTL Airline Spend (2nd year, average)',
        'EST_AIRLN_SPEND_1stYr, mean (SD)': 'TTL Airline Spend (1st year, average)',
        'TTL_Diff_1to2_YoY, mean (SD)': 'TTL Rev YoY change (average)',
        'Percent_AA_to_All_1stYr, mean (SD)': 'SOW percentage (1st year)',
        'Churned, n (%)': 'Churned rate (%)'
    }
    df_filtered.index = df_filtered.index.set_levels(
        df_filtered.index.levels[0].map(lambda x: rename_dict.get(x, x)), level=0
    )
    
    df_filtered = df_filtered[df_filtered.index.get_level_values(1) != 'Not Churned']
    df_filtered.index = df_filtered.index.droplevel(1)

    df_filtered.columns = df_filtered.columns.droplevel(0)
    return df_filtered
