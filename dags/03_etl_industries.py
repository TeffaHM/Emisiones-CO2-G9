from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import *


def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('dataproc-pyspark') \
        .getOrCreate()
    return spark

def process_industrial_data(spark, input_data = "gs://Data_Lake/datalake/datainput/industrial/Industrialglobal_power_plant_database.csv.csv", 
output_data = "gs://Data_Lake/datalake/dataoutput/industrial/"):
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
    df_energy = spark.read.format("CSV").option("header","true").option("delimiter",",").load(input_data)    

    # Codigo de todo el procesamiento y limpieza del dataset
    # Podria usarse SQL spark o Python spark

    # Codigo de carga en la capa final del data lake
    df_energy.write.mode("overwrite").format("parquet").save(output_data)
    pass


def main():
    spark = create_spark_session()
    process_industrial_data(spark)

if __name__ == "__main__":
    main()