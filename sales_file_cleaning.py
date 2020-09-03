import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import sys
from datetime import datetime, date
import os
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

def getting_user_input():

    root_path = input('Please enter the path of the sales files: \n').replace('\\', '/')
    return (True, [root_path])


def getting_file_names(root_path):

    sales_file_names = list()
    os.chdir(root_path)
    all_files = os.listdir()
    
    for single_file in all_files:
        if(single_file[:5].lower() == 'sales'):
            sales_file_names.append(single_file)

    return (True, [sales_file_names])


def loading_sales_data_frame(file_name, root_path):

    sales_file_path = root_path + '/' + file_name

    df_sales_columns = ['Country', 'Diageo Customer ID', 'Diageo Customer Name', 
        'Invoice number', 'Type of Invoice',	'Invoice Date', 'Store code', 'Product Code', 
        'Quantity', 'Unit of measure', 'Total Amount WITHOUT TAX', 'Total Amount WITH TAX', 
        'Currency Code', 'Sales Representative Code']

    #It's not implemented in this version the functionality to differentiate if the file has header or not.
    #If your file doesn't have a header, change the below parameter <header> from 0 to 'None' (With the quotes)
    try:
        df_sales = pd.read_csv(sales_file_path, index_col=False, names=df_sales_columns,sep=';', low_memory=False,
        dtype=str, header=0).fillna('')
    except Exception as error:
        print(error)
        return (False, [])
    
    return (True, [df_sales])


def generic_sanitizing_sales_file(df_sales):

    #saving original column arrangement to reindex sales data frame later on
    correct_column_order_df_sales = df_sales.columns
    #Removing negative sign from the end of the values (Some samples were found)
    values_that_end_with_negative_sign_quantity = (df_sales['Quantity'].str[-1] == '-')
    df_sales.loc[values_that_end_with_negative_sign_quantity, 'Quantity'] = '-' + df_sales.loc[values_that_end_with_negative_sign_quantity, 'Quantity'].str[:-1]
    
    values_that_end_with_negative_sign_total_with_tax = (df_sales['Total Amount WITH TAX'].str[-1] == '-')
    df_sales.loc[values_that_end_with_negative_sign_total_with_tax, 'Total Amount WITH TAX'] = '-' + df_sales.loc[values_that_end_with_negative_sign_total_with_tax, 'Total Amount WITH TAX'].str[:-1]
    
    values_that_end_with_negative_sign_total_without_tax = (df_sales['Total Amount WITHOUT TAX'].str[-1] == '-')
    df_sales.loc[values_that_end_with_negative_sign_total_without_tax, 'Total Amount WITHOUT TAX'] = '-' + df_sales.loc[values_that_end_with_negative_sign_total_without_tax, 'Total Amount WITHOUT TAX'].str[:-1]

    #Removing spaces and leading zeros from below columns
    df_sales['Product Code'] = df_sales['Product Code'].str.lstrip('0')
    df_sales['Store code'] = df_sales['Store code'].str.lstrip('0')
    df_sales['Store code'] = df_sales['Store code'].str.strip()

    return (True, [df_sales, correct_column_order_df_sales])


def trimming_over_lenght(df_sales):

    df_sales['Country'] = df_sales['Country'].str[:20]
    df_sales['Diageo Customer ID'] = df_sales['Diageo Customer ID'].str[:20]
    df_sales['Diageo Customer Name'] = df_sales['Diageo Customer Name'].str[:50]
    df_sales['Invoice number'] = df_sales['Invoice number'].str[:30]
    df_sales['Type of Invoice'] = df_sales['Type of Invoice'].str[:20]
    df_sales['Invoice Date'] = df_sales['Invoice Date'].str[:8]
    df_sales['Store code'] = df_sales['Store code'].str[:12]
    df_sales['Product Code'] = df_sales['Product Code'].str[:50]
    df_sales['Unit of measure'] = df_sales['Unit of measure'].str[:20]
    df_sales['Currency Code'] = df_sales['Currency Code'].str[:15]
    df_sales['Sales Representative Code'] = df_sales['Sales Representative Code'].str[:20]

    return (True, [df_sales])


def converting_data_types(df_sales):

    #Turning below fields into numeric type
    df_sales['Quantity'] = pd.to_numeric(df_sales['Quantity']).fillna(0)
    df_sales['Total Amount WITH TAX'] = pd.to_numeric(df_sales['Total Amount WITH TAX']).fillna(0)
    df_sales['Total Amount WITHOUT TAX'] = pd.to_numeric(df_sales['Total Amount WITHOUT TAX']).fillna(0)

    return (True, [df_sales])


def verifying_values_with_without_tax(df_sales):

    df_sales['Total Amount WITH TAX'] = pd.to_numeric(df_sales['Total Amount WITH TAX'], errors='coerce').fillna(0)
    df_sales['Total Amount WITHOUT TAX'] = pd.to_numeric(df_sales['Total Amount WITHOUT TAX'], errors='coerce').fillna(0)

    sum_value_with_tax = df_sales['Total Amount WITH TAX'].sum()
    sum_value_without_tax = df_sales['Total Amount WITHOUT TAX'].sum()

    if ( sum_value_without_tax > sum_value_with_tax ):
        df_sales.rename(columns={ 'Total Amount WITH TAX':'Total Amount WITHOUT TAX', 'Total Amount WITHOUT TAX':'Total Amount WITH TAX' }, inplace=True)

    return (True, [df_sales])


def creating_csv_files(df_sales, root_path, single_file_name, correct_column_order_df_sales):

    file_attributes = single_file_name.split('.')
    single_file_name_without_extension = '.'.join(file_attributes[:-1])
    extension = file_attributes[-1]
    df_sales_file_path = root_path + '/' + single_file_name_without_extension + '_CLEAN.' + extension

    df_sales[correct_column_order_df_sales].to_csv(df_sales_file_path,
    encoding='mbcs', sep=';', columns=correct_column_order_df_sales, index=False)

    return (True, [df_sales])


try:
    success_getting_user_input, content_getting_user_input = getting_user_input()
except Exception as error:
    print(error)
    sys.exit()
finally:
    if success_getting_user_input:
        root_path = content_getting_user_input[0]

try:
    print('getting file names...')
    success_getting_file_names, content_getting_file_names = getting_file_names(root_path)
except Exception as error:
    print(error)
    sys.exit()
finally:
    if success_getting_file_names:
        file_names = content_getting_file_names[0]
        

try:
    for single_file_name in file_names:
        try:
            print('loading sales data frame')
            success_loading_sales_data_frame, content_loading_sales_data_frame = loading_sales_data_frame(single_file_name, root_path)
        except Exception as error:
            print('ERROR: {} - {}'.format(error, single_file_name))
        finally:
            if success_loading_sales_data_frame:
                df_sales = content_loading_sales_data_frame[0]
        
        try:
            print('generic sanitizing sales file')
            success_generic_sanitizing_sales_file, content_generic_sanitizing_sales_file = generic_sanitizing_sales_file(df_sales)
        except Exception as error:
            print('ERROR: {} - {}'.format(error, single_file_name))
        finally:
            if success_generic_sanitizing_sales_file:
                df_sales = content_generic_sanitizing_sales_file[0]
                correct_column_order_df_sales = content_generic_sanitizing_sales_file[1]
        
        try:
            print('trimming over lenght...')
            success_trimming_over_lenght, content_trimming_over_lenght = trimming_over_lenght(df_sales)
        except Exception as error:
            print('ERROR: {} - {}'.format(error, single_file_name))
        finally:
            if success_trimming_over_lenght:
                df_sales = content_trimming_over_lenght[0]
        
        try:
            print('converting data types')
            success_converting_data_types, content_converting_data_types = converting_data_types(df_sales)
        except Exception as error:
            print('ERROR: {} - {}'.format(error, single_file_name))
        finally:
            if success_converting_data_types:
                df_sales = content_converting_data_types[0]
        
        try:
            print('verifying values with vs without tax')
            success_verifying_values_with_without_tax, content_verifying_values_with_without_tax = verifying_values_with_without_tax(df_sales)
        except Exception as error:
            print('ERROR: {} - {}'.format(error, single_file_name))
        finally:
            if success_verifying_values_with_without_tax:
                df_sales = content_verifying_values_with_without_tax[0]

        try:
            print('creating csv files')
            success_creating_csv_files, content_creating_csv_files = creating_csv_files(df_sales, root_path, single_file_name, correct_column_order_df_sales)
        except Exception as error:
            print('ERROR: {} - {}'.format(error, single_file_name))
        finally:
            if success_creating_csv_files:
                print('success')
except Exception as error:
    print(error)
    sys.exit()