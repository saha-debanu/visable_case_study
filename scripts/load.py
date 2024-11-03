from sqlalchemy import create_engine, text
import pandas as pd

def load_data(transformed_data):

    # Database configuration
    DB_USER = "dbnsaha"
    DB_PASSWORD = "saha2815"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "postgres"

    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # Load products table data first, as other tables depend on it
    if 'products' in transformed_data:
        products_df = transformed_data['products']
        # print(products_df)
        products_df.to_sql('products', engine, if_exists='replace', index=False)
        print("loaded products")

    # Define UPSERT logic with referential integrity for each dependent table
    for table_name, df in transformed_data.items():

        # schema='etl_data'
        
        # Skip products table since it has already been loaded
        if table_name == 'products':
            continue
        
        # Apply referential integrity filter for `contracts` and `prices` tables
        if table_name in ['contracts', 'prices']:
            product_ids = pd.read_sql("SELECT id FROM products", con=engine)
            df = df[df['productid'].isin(product_ids['id'])]

        # Create a temporary table to load data into first
        temp_table_name = f"{table_name}_temp"
        
        # Load data into temporary table
        df.to_sql(temp_table_name, engine, if_exists='replace', index=False)
        
        # Generate the column list for the UPSERT statement
        columns = ', '.join(df.columns)
        update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != 'id'])
        print(update_columns)
        
        # Execute UPSERT from temp table to target table
        with engine.connect() as conn:
            with conn.begin():  # Automatically handles BEGIN and COMMIT
                try:
                    # Insert or update data in the main table
                    conn.execute(text(f"""
                        INSERT INTO {table_name} ({columns})
                        SELECT {columns} FROM {temp_table_name}
                        ON CONFLICT (id) DO UPDATE 
                        SET {update_columns};
                    """))
                except Exception as e:
                    print(f"Error during UPSERT for table {table_name}: {e}")