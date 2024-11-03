from sqlalchemy import create_engine, text
import pandas as pd

def load_data(transformed_data):

    # Database configuration
    DB_USER = "dbnsaha"
    DB_PASSWORD = "saha2815"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "postgres"

    # Initialize the SQLAlchemy engine
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # Define UPSERT logic for each table
    for table_name, df in transformed_data.items():
        
        # If the table is `contracts` or `prices`, filter for valid `product_id` references
        if table_name in ['contracts', 'prices']:
            product_ids = pd.read_sql("SELECT id FROM products", con=engine)
            df = df[df['product_id'].isin(product_ids['id'])]
        
        # Create a temporary table to load data into first
        temp_table_name = f"{table_name}_temp"
        
        # Load data into temporary table
        df.to_sql(temp_table_name, con=engine, if_exists='replace', index=False)
        
        # Generate the column list for the UPSERT statement
        columns = ', '.join(df.columns)
        update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != 'id'])
        
        # Execute UPSERT from temp table to target table
        with engine.connect() as conn:
            try:
                conn.execute(text("BEGIN;"))  # Start transaction
                # Insert or update data in the main table
                conn.execute(text(f"""
                    INSERT INTO {table_name} ({columns})
                    SELECT {columns} FROM {temp_table_name}
                    ON CONFLICT (id) DO UPDATE 
                    SET {update_columns};
                """))
                conn.execute(text("COMMIT;"))  # Commit transaction
            except Exception as e:
                conn.execute(text("ROLLBACK;"))  # Rollback transaction if there's an error
                print(f"Error during UPSERT for table {table_name}: {e}")
            finally:
                # Drop the temporary table after load
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))
