import pandas as pd

def transform_data(data):
    transformed_data = {}

    # Deduplicate, clean, and transform data for each table
    if 'contracts' in data:
        try:
            contracts = data['contracts']
            
            # Remove duplicates
            contracts = contracts.drop_duplicates(subset='id', keep='last')
            
            # Convert date columns to datetime
            contracts['createdat'] = pd.to_datetime(contracts['createdat'], errors='coerce')
            contracts['modificationdate'] = pd.to_datetime(contracts['modificationdate'], errors='coerce')
            contracts['startdate'] = pd.to_datetime(contracts['startdate'], errors='coerce')
            contracts['enddate'] = pd.to_datetime(contracts['enddate'], errors='coerce')
            contracts['fillingdatecancellation'] = pd.to_datetime(contracts['fillingdatecancellation'], errors='coerce')
            
            # Handle missing values and clean data
            contracts = contracts.dropna(subset=['id', 'productid', 'createdat'])  # Drop rows with essential null values
            contracts['usage'] = contracts['usage'].fillna(0)  # Fill missing usage values with 0
            
            # Remove any rows with invalid IDs
            contracts = contracts[contracts['id'] > 0]
            
            transformed_data['contracts'] = contracts
        
        except Exception as e:
            print(f"Error transforming contracts data: {e}")

    if 'products' in data:
        try:
            products = data['products']
            
            # Remove duplicates
            products = products.drop_duplicates(subset='id', keep='last')
            
            # Convert date columns to datetime
            products['modificationdate'] = pd.to_datetime(products['modificationdate'], errors='coerce')
            
            # Handle missing values and clean data
            products = products.dropna(subset=['id'])  # Drop rows with essential null values
            products['deleted'] = products['deleted'].fillna(0).astype(int)  # Fill missing "deleted" status with 0 and ensure it's an integer
            
            # Remove any rows with invalid IDs
            products = products[products['id'] > 0]
            
            transformed_data['products'] = products
        
        except Exception as e:
            print(f"Error transforming products data: {e}")

    if 'prices' in data:
        try:
            prices = data['prices']
            
            # Remove duplicates
            prices = prices.drop_duplicates(subset='id', keep='last')
            
            # Convert date columns to datetime
            prices['valid_from'] = pd.to_datetime(prices['valid_from'], errors='coerce')
            prices['valid_until'] = pd.to_datetime(prices['valid_until'], errors='coerce')
            prices['modificationdate'] = pd.to_datetime(prices['modificationdate'], errors='coerce')
            
            # Handle missing values and clean data
            prices = prices.dropna(subset=['id', 'productid', 'price'])  # Drop rows with essential null values
            prices['price'] = prices['price'].fillna(0)  # Fill missing prices with 0
            
            # Remove any rows with invalid IDs
            prices = prices[prices['id'] > 0]
            
            transformed_data['prices'] = prices
        
        except Exception as e:
            print(f"Error transforming prices data: {e}")

    return transformed_data
