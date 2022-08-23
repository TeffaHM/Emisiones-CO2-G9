from pyspark.sql import SparkSession
import pyspark.pandas as pd
from datetime import datetime
def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('dataproc-pyspark') \
        .getOrCreate()
    return spark

def process_source_data(spark, input_data = f"gs://data-lake-46c2377c-2322-11ed-afb6-5a6f18944df6/datalake/source/source_{datetime.now().strftime('%Y-%m')}.csv", 
output_data = f"gs://data-lake-46c2377c-2322-11ed-afb6-5a6f18944df6/data_output/source/{datetime.now().strftime('%Y-%m')}/"):
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
    # Codigo de todo el procesamiento y limpieza del dataset
    # Podria usarse SQL spark o Python spark
    df_source = pd.read_csv(input_data)   
    df_source = df_source[['country', 'year', 'population', 'primary_energy_consumption', 'oil_consumption']]
    df_source.fillna(0, inplace=True)

    # Codigo de carga en la capa final del data lake
    df_source.to_spark().write.mode("overwrite").format("parquet").save(output_data)


def main():
    spark = create_spark_session()
    process_source_data(spark)

if __name__ == "__main__":
    main()