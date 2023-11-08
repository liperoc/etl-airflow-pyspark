from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName('etl').getOrCreate()

df_spark = spark.read.json('/home/liperoc/projects/etl_finance_pyspark/json/currency.json')

df_spark = df_spark.select(explode("currency").alias("currency"))
df_spark = df_spark.select(
    "currency.fromCurrency",
    "currency.toCurrency",
    "currency.high",
    "currency.low",
    "currency.name",
    "currency.updatedAtDate"
)

df_spark.write.csv('/home/liperoc/projects/etl_finance_pyspark/csv/currency', header=True, mode='overwrite')

spark.stop()