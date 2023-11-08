from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_unixtime, round

spark = SparkSession.builder.appName('etl').getOrCreate()


df_spark = spark.read.json('/home/liperoc/projects/etl_finance_pyspark/json/crypto.json')

df_spark = df_spark.select(explode("coins").alias("coins"))
df_spark = df_spark.select(
    "coins.coin",
    "coins.coinName",
    "coins.currency",
    "coins.regularMarketPrice",
    "coins.regularMarketTime"
)

df_spark = df_spark.withColumn('regularMarketPrice', round(df_spark['regularMarketPrice'], 2)) #
df_spark = df_spark.withColumn('date', from_unixtime('regularMarketTime')) # Transform Epoch to TimeStamp
df_spark = df_spark.drop('regularMarketTime')

df_spark.write.csv('/home/liperoc/projects/etl_finance_pyspark/csv/crypto', header=True, mode='overwrite')

spark.stop()