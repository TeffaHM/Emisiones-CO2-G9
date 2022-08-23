import pandas as pd
from datetime import datetime
from airflow.models import Variable

def process_energy(object):
        element = object[0]
        input = f"gs://Energy/{element}"
        output_data = f"gs://{Variable.get('STAGING_BUCKET')}/Energy_CO2/energy_{datetime.now().strftime('%Y-%m')}"
        df_energy = pd.read_csv(input)
        df_energy = df_energy.drop(labels="_c0", axis=1)
        df_energy.fillna(0, inplace=True)
        df_energy.drop_duplicates(inplace=True)
        df_energy = df_energy[df_energy['Year'] >= 2018]
        df_energy.to_parquet(f'{output_data}/{datetime.now().date()}.parquet')