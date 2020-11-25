import pandas as pd 
import os
import sys
from datetime import datetime, date

def getting_user_input():
    destination_path = input('Please enter the destination path of generated files: \n').replace('\\', '/')
    return destination_path

def setting_template_path():
    root_path = ''
    template_file_name = 'customer_template.csv'
    final_template_file_name = root_path + template_file_name
    return final_template_file_name

def loading_template_dataframe(final_template_file_name):
    df_template_customer = pd.read_csv(final_template_file_name, sep=';', dtype=str, low_memory=False, encoding='mbcs')
    return df_template_customer

def sanitizing_file(df_template_customer):
    for column in df_template_customer.columns:
        df_template_customer[column] = df_template_customer[column].str.strip()
    
    df_template_customer['Key_SAP_Code'] = df_template_customer['SAP_Code']
    return df_template_customer

def writing_files(df_to_be_written, file_name, destination_path):

    today_date = datetime.today().strftime("%Y%m%d_%H%M%S")
    final_file_name = file_name + '_' + today_date + '.csv'
    final_file_name = os.path.join(destination_path, final_file_name).replace('\\', '/')

    df_to_be_written[df_to_be_written.columns].to_csv(
        final_file_name, sep=';', encoding='mbcs',
        columns=df_to_be_written.columns, index=False
    )

    return True

def splitting_data_frames(df_template_customer, destination_path):
    
    df_template_customer.set_index(['Key_SAP_Code'], inplace=True)

    for individual_dist in df_template_customer.index.unique():
        file_name = 'Customer_Catalogue_' + individual_dist
        single_df_customer = df_template_customer.loc[[individual_dist], :]

        try:
            writing_files(single_df_customer, file_name, destination_path)
        except Exception as error:
            print('Not possible saving file - {}\nError - {}'.format(file_name, error))

    return True

def main():

    try:
        print('getting_user_input')
        destination_path = getting_user_input()
    except Exception as error:
        print('Error getting_user_input - {}'.format(error))
        sys.exit()
    
    try:
        print('setting_template_path')
        final_template_file_name = setting_template_path()
    except Exception as error:
        print('Error final_template_file_name - {}'.format(error))
        sys.exit()
    
    try:
        print('loading_template_dataframe')
        df_template_customer = loading_template_dataframe(final_template_file_name)
    except Exception as error:
        print('Error loading_template_dataframe - {}'.format(error))
        sys.exit()
    
    try:
        print('sanitizing_file')
        df_template_customer = sanitizing_file(df_template_customer)
    except Exception as error:
        print('Error sanitizing_file - {}'.format(error))
        sys.exit()
    
    try:
        success_splitting_data_frames = True
        splitting_data_frames(df_template_customer, destination_path)
    except Exception as error:
        success_splitting_data_frames = False
        print('Error splitting_data_frames - {}'.format(error))
    finally:
        if success_splitting_data_frames:
            print('Successfully Executed')



if __name__ == '__main__':
    main()