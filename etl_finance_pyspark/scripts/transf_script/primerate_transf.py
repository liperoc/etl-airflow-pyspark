from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName('etl').getOrCreate()

df_spark = spark.read.json('/home/liperoc/projects/etl_finance_pyspark/json/primerate.json')

df_spark = df_spark.select(explode("prime-rate").alias("prime-rate"))
df_spark = df_spark.select(
    "prime-rate.date",
    "prime-rate.value"
)

df_spark.write.csv('/home/liperoc/projects/etl_finance_pyspark/csv/primerate', header=True, mode='overwrite')

spark.stop()