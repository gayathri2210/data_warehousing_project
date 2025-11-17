# main.py

from extract_bronze import load_bronze_table
from transform_silver import transform_silver
from load_gold import load_gold

def run_pipeline():
    print("🚀 Starting ETL Pipeline...")
    load_bronze_table()
    transform_silver()
    load_gold()
    print("🎉 ETL Pipeline Completed Successfully!")

if __name__ == "__main__":
    run_pipeline()
