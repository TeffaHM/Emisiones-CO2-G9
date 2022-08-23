import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max,row_number,count, mean, desc, lit, trim

DATASET = "analytics_dwh_onu"
BUCKET_NAME = "staging-46c2377c-2322-11ed-afb6-5a6f18944df6"
bq_table_poor = "poor"

def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('dataproc-pyspark') \
        .getOrCreate()
    return spark

def dwh_dimenssions(spark):
    spark.conf.set('temporaryGcsBucket', BUCKET_NAME)
    source = spark.read.format("parquet").option("header","true").option('inferSchema','true').load('/FileStore/tables/source/')
    source = source.withColumn("population",col("population").cast("Double"))
    energy = spark.read.format("parquet").option("header","true").option('inferSchema','true').load('/FileStore/tables/parq/')

    # Hive
    energy.createOrReplaceTempView("energytb")
    source.createOrReplaceTempView("sourcetb")
    table_poor = spark.sql("SELECT count(*) FROM sourcetb s join energytb e on s.country = e.country").show()
    table_poor.write \
                   .format("bigquery") \
                   .option("table","{}.{}".format(DATASET, bq_table_poor)) \
                   .option("temporaryGcsBucket", BUCKET_NAME) \
                   .mode('overwrite') \
                   .save()
def main():
    spark = create_spark_session()
    dwh_dimenssions(spark)

if __name__ == "__main__":
    main()