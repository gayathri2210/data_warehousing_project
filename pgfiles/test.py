from load_gold import load_gold
import pandas as pd
from sqlalchemy import create_engine
from config import DATABASE_URL

engine = create_engine(DATABASE_URL)

silver_cycle = pd.read_sql("SELECT * FROM silver_cycle", engine)
silver_repro = pd.read_sql("SELECT * FROM silver_reproductivehistory", engine)
silver_patient = pd.read_sql("SELECT * FROM silver_patient", engine)

fact_test = silver_cycle.merge(
    silver_patient[['ClientID']], on='ClientID'
).merge(
    silver_repro[['ClientID','Numberpreg','Miscarriages','Abortions','Breastfeeding']],
    on='ClientID'
)

print(fact_test.dtypes)
print(fact_test.head())
