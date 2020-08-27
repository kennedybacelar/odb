import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import sys
from datetime import datetime, date
import os
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

def getting_user_input():

    STR_indicator = False
    root_path = input('Please enter the path of the sales.txt file: \n').replace('\\', '/')
    country = input('Please inform the country of the distrbutor: \n').lower()
    STR_country_list = ['paraguay', 'uruguay']

    if (country in STR_country_list):
        STR_indicator = True

    return (True, [root_path, STR_indicator])


def loading_data_frames(root_path, STR_indicator):

    sales_file_path = root_path + '/sales.txt'

    df_sales_columns = ['Country', 'Diageo Customer ID', 'Diageo Customer Name', 
    'Invoice number', 'Type of Invoice',	'Invoice Date', 'Store code', 'Product Code', 
    'Quantity', 'Unit of measure', 'Total Amount WITHOUT TAX', 'Total Amount WITH TAX', 
    'Currency Code', 'Sales Representative Code']

    if STR_indicator:
        sales_header = 0
    else:
        sales_header = None

    #Loading Data Frame of Sales File
    try:
        df_sales = pd.read_csv(sales_file_path, index_col=False, names=df_sales_columns,sep=';', low_memory=False,
        dtype={ 'Quantity':str, 'Store code':str, 'Product Code':str, 'Invoice Date':str,
        'Total Amount WITH TAX':str, 'Total Amount WITHOUT TAX':str  }, header=sales_header).fillna('')
    except Exception as error:
        print('Not possible opening the file - {}\n {}'.format(sales_file_path, error))
        return (False, [])
    
    return (True, [df_sales])


def split_per_distributor(df_sales):

    df_sales.set_index(['Diageo Customer ID'], inplace=True)
    dfs_per_distributors = list()

    for single_dist in df_sales.index.unique():
        try:
            dfs_per_distributors.append((df_sales.loc[single_dist], single_dist))
        except Exception as error:
            print(error)
            return (False, [])

    df_sales.reset_index(inplace=True)
    return (True, [dfs_per_distributors])



def creating_csv_files(dfs_per_distributors, root_path):

    today_date = datetime.today().strftime("%Y%m%d_%H%M%S") 

    for df_sales_per_dist, single_dist in dfs_per_distributors:

        df_sales_per_dist.reset_index(inplace=True)
        csv_sales_file_path = root_path + '/SALES_' + str(single_dist) + '_' + today_date + '.csv'

        try:
            df_sales_per_dist[df_sales_per_dist.columns].to_csv(csv_sales_file_path,
            encoding='mbcs', sep=';', columns=df_sales_per_dist.columns, index=False)
        except Exception as error:
            print('Not possible creating EntrepidusDistributors CSV File\n {}'.format(error))
            return (False, [])
    return (True, [])


def main():

    try:
        successGettingUserInput, content = getting_user_input()
    except Exception as error:
        print(error)
        sys.exit()
    finally:
        if successGettingUserInput:
            root_path = content[0]
            STR_indicator = content[1]

    try:
        print('loading_data_frames')
        successLoadingDataFrames, content = loading_data_frames(root_path, STR_indicator)
    except Exception as error:
        print(error)
        sys.exit()
    finally:
        if successLoadingDataFrames:
            df_sales = content[0]

    try:
        print('split_per_distributor')
        successSplitPerDistributor, content = split_per_distributor(df_sales)
    except Exception as error:
        print(error)
        sys.exit()
    finally:
        if successSplitPerDistributor:
            dfs_per_distributors = content[0]

    try:
        print('creating_csv_files')
        successCreatingCsvFiles, content = creating_csv_files(dfs_per_distributors, root_path)
    except Exception as error:
        print(error)
        sys.exit()
    finally:
        if successCreatingCsvFiles:
            print('Successfully executed')

if __name__ == '__main__':
  main()




