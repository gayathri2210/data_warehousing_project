# load_gold.py

import pandas as pd
from sqlalchemy import create_engine
from config import DATABASE_URL

def load_gold():
    engine = create_engine(DATABASE_URL)

    # Load silver
    silver_patient = pd.read_sql("SELECT * FROM silver_patient", engine)
    silver_cycle = pd.read_sql("SELECT * FROM silver_cycle", engine)
    silver_repro = pd.read_sql("SELECT * FROM silver_reproductivehistory", engine)

    # CLEAN ClientID (fixes join mismatch!)
    for df in [silver_patient, silver_cycle, silver_repro]:
        df['ClientID'] = df['ClientID'].astype(str).str.strip()

    # ---- DimPatient ----
    dimpatient = silver_patient.copy()
    dimpatient['PatientKey'] = range(1, len(dimpatient) + 1)
    dimpatient.to_sql('dimpatient', engine, if_exists='replace', index=False)

    # ---- DimCycle ----
    dimcycle = silver_cycle[['CycleNumber','ReproductiveCategory']].drop_duplicates()
    dimcycle['CycleKey'] = range(1, len(dimcycle) + 1)
    dimcycle.to_sql('dimcycle', engine, if_exists='replace', index=False)

    # ---- FACT TABLE ----
    fact = silver_cycle.merge(
        dimpatient[['ClientID','PatientKey']], on='ClientID', how='left'
    ).merge(
        silver_repro[['ClientID','Numberpreg','Miscarriages','Abortions','Breastfeeding']],
        on='ClientID',
        how='left'
    ).merge(
        dimcycle[['CycleNumber','ReproductiveCategory','CycleKey']],
        on=['CycleNumber','ReproductiveCategory'],
        how='left'
    )

    # Convert blanks to NULL
    fact = fact.replace(r'^\s*$', None, regex=True)

    # Numeric cleanup
    numeric_cols = ['Numberpreg','Miscarriages','Abortions','Breastfeeding']
    for col in numeric_cols:
        fact[col] = pd.to_numeric(fact[col], errors='coerce')

    fact['FactKey'] = range(1, len(fact) + 1)

    # Final safe insert
    with engine.begin() as conn:
        fact.to_sql('factcycleanalytics', conn, if_exists='replace', index=False)

    print("✅ Gold layer created successfully.")
