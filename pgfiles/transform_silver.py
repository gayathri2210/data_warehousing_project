# transform_silver.py

import pandas as pd
from sqlalchemy import create_engine
from config import DATABASE_URL

def transform_silver():
    engine = create_engine(DATABASE_URL)

    bronze_df = pd.read_sql("SELECT * FROM bronze_cycledata", engine)

    # Remove header duplicates (ClientID == "ClientID")
    bronze_df = bronze_df[bronze_df['ClientID'] != 'ClientID']

    # Convert spaces to None
    bronze_df = bronze_df.replace(" ", None)

    # ---- Silver Patient ----
    silver_patient = bronze_df[['ClientID', 'Age', 'Height', 'Weight', 'BMI']].copy()
    silver_patient['Age'] = pd.to_numeric(silver_patient['Age'], errors='coerce')
    silver_patient['Height'] = pd.to_numeric(silver_patient['Height'], errors='coerce')
    silver_patient['Weight'] = pd.to_numeric(silver_patient['Weight'], errors='coerce')
    silver_patient['BMI'] = pd.to_numeric(silver_patient['BMI'], errors='coerce')

    silver_patient.to_sql('silver_patient', engine, if_exists='replace', index=False)

    # ---- Silver Cycle ----
    cycle_cols = [
        'ClientID','CycleNumber','LengthofCycle','EstimatedDayofOvulation',
        'LengthofLutealPhase','MeanBleedingIntensity','TotalMensesScore',
        'NumberofDaysofIntercourse','IntercourseInFertileWindow','ReproductiveCategory'
    ]

    silver_cycle = bronze_df[cycle_cols].copy()
    for col in cycle_cols[1:]:
        silver_cycle[col] = pd.to_numeric(silver_cycle[col], errors='coerce')

    silver_cycle.to_sql('silver_cycle', engine, if_exists='replace', index=False)

    # ---- Silver Reproductive History ----
    rep_cols = ['ClientID','Numberpreg','Livingkids','Miscarriages','Abortions','Breastfeeding']
    silver_repro = bronze_df[rep_cols].copy()

    for col in rep_cols[1:]:
        silver_repro[col] = pd.to_numeric(silver_repro[col], errors='coerce')

    silver_repro.to_sql('silver_reproductivehistory', engine, if_exists='replace', index=False)

    print("✅ Silver layer created successfully.")

if __name__ == "__main__":
    transform_silver()
