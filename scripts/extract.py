import os
import glob
import pandas as pd
from datetime import datetime

def extract_data():
    data = {}

    # Directory with CSV files
    data_path = "Path to .csv"
    today_date = datetime.now().strftime('%Y%m%d')
    pattern = f"{data_path}/{today_date}*"
    data_files = glob.glob(pattern)
    
    # Loop through each CSV file in the data directory
    for file in data_files:
        print(file)
        try:
            # Extract table name from the file name
            table_name = os.path.basename(file).split('_')[-1].split('.')[0].lower()
            print(table_name)
            
            # Check if the table name is valid
            if table_name in ['contracts', 'products', 'prices']:
                print(f"Extracting data from {file} into table: {table_name}")
                
                # Read the CSV file into a DataFrame
                df = pd.read_csv(file,delimiter=';')
                
                # Optional: specify data types and handle missing values as necessary
                if table_name == 'contracts':
                    date_columns = ["createdat","startdate","enddate","fillingdatecancellation","modificationdate"]
                    df[date_columns] = df[date_columns].replace('', None)
                    # df = df.astype({
                    #     'id': 'int64',
                    #     'type': 'str',
                    #     'energy': 'str',
                    #     'usage': 'int',
                    #     'usagenet': 'int',
                    #     'createdat': 'str',  # Will convert to datetime later
                    #     'startdate': 'str',
                    #     'enddate': 'str',
                    #     'fillingdatecancellation': 'str',
                    #     'cancellationreason': 'str',
                    #     'city': 'str',
                    #     'status': 'str',
                    #     'productid': 'int',
                    #     'modificationdate': 'str'
                    # })
                
                elif table_name == 'products':
                    df['modificationdate'] = df['modificationdate'].replace('', None)
                    # df = df.astype({
                    #     'id': 'int64',
                    #     'productcode': 'str',
                    #     'productname': 'str',
                    #     'energy': 'str',
                    #     'consumptiontype': 'str',
                    #     'deleted': 'int',
                    #     'modificationdate': 'str'
                    # })
                
                elif table_name == 'prices':
                    date_columns = ["valid_from","valid_until","modificationdate"]
                    df[date_columns] = df[date_columns].replace('', None)
                    # df = df.astype({
                    #     'id': 'int64',
                    #     'productid': 'int',
                    #     'pricecomponentid': 'int',
                    #     'productcomponent': 'str',
                    #     'price': 'float64',
                    #     'unit': 'str',
                    #     'valid_from': 'str',
                    #     'valid_until': 'str',
                    #     'modificationdate': 'str'
                    # })
                
                # Add DataFrame to the data dictionary
                data[table_name] = df
            
            else:
                print(f"Skipping unrecognized file: {file}")
        
        except Exception as e:
            print(f"Error processing file {file}: {e}")
    
    return data
