# extract_bronze.py

import pandas as pd
from sqlalchemy import create_engine
from config import DATABASE_URL, BRONZE_CSV

def load_bronze_table():
    engine = create_engine(DATABASE_URL)

    df = pd.read_csv(BRONZE_CSV)

    # Clean empty spaces
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    df.to_sql('bronze_cycledata', engine, if_exists='replace', index=False)
    print("✅ Bronze layer loaded successfully.")

if __name__ == "__main__":
    load_bronze_table()
