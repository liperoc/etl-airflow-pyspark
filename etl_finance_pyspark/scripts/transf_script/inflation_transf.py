from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName('etl').getOrCreate()

df_spark = spark.read.json('/home/liperoc/projects/etl_finance_pyspark/json/inflation.json')

df_spark = df_spark.select(explode("inflation").alias("inflation"))
df_spark = df_spark.select(
    "inflation.date",
    "inflation.value"
)

df_spark.write.csv('/home/liperoc/projects/etl_finance_pyspark/csv/inflation', header=True, mode='overwrite')

spark.stop()