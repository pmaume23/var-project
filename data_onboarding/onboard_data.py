import os
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse

# PostgreSQL connection details from environment (do not hardcode secrets)
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT", "5432")

missing = [name for name, value in {
    "DB_HOST": DB_HOST,
    "DB_NAME": DB_NAME,
    "DB_USER": DB_USER,
    "DB_PASS": DB_PASS,
}.items() if not value]
if missing:
    raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

# URL-encode the password
encoded_db_pass = urllib.parse.quote_plus(DB_PASS)

try:
    # Create engine
    engine = create_engine(
        f'postgresql+psycopg2://{DB_USER}:{encoded_db_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}',
        connect_args={'connect_timeout': 10}
    )
    
    # Test connection
    with engine.connect() as conn:
        print("Connection successful!")
    
    print("PostgreSQL engine created successfully.")
    
    # Load CSV file
    df_ratios_price = pd.read_csv('sp500_ratios_price_data.csv')
    print(f"Loaded {len(df_ratios_price)} rows from CSV file.")
    
    # Your data upload code
    table_name = 'sp500_ratios_price'
    df_ratios_price.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data successfully written to {table_name} table in PostgreSQL.")
    
except Exception as e:
    print(f"An error occurred: {e}")
    import traceback
    traceback.print_exc()
