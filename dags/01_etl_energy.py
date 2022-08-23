from pyspark.sql import SparkSession
import pyspark.pandas as pd

def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('dataproc-pyspark') \
        .getOrCreate()
    return spark

def process_energy_data(spark, input_data = "gs://Data_Lake/datalake/datainput/Energy_CO2/energyco2.csv", 
output_data = "gs://Data_Lake/datalake/dataoutput/Energy_CO2/"):
    ''''
    Parameters:
        spark: the cursor object.
        input_path: the path to the bucket containing song data.
        output_path: the path to destination bucket where the parquet files
            will be stored.
    Returns:
        None
    '''

    # Codigo de extraccion
    # delimiter: delimitador en el csv, cambiado segun se observe en el archivo
    # df_schema: Es las estructura de las columnas en el dataset (definirse previamente)
    df_energy = pd.read_csv(input_data)
    df_energy = df_energy.drop(labels="_c0", axis=1)
    df_energy.fillna(0, inplace=True)
    df_energy.drop_duplicates(inplace=True)
    df_energy = df_energy[df_energy['Year'] >= 2018]


    # Codigo de todo el procesamiento y limpieza del dataset
    # Podria usarse SQL spark o Python spark


    # Codigo de carga en la capa final del data lake
    df_energy.to_spark().write.mode("overwrite").format("parquet").save(output_data)
    


def main():
    spark = create_spark_session()
    process_energy_data(spark)

if __name__ == "__main__":
    main()