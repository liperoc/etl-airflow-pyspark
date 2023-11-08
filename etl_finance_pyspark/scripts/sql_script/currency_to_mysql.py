import mysql.connector
from pyspark.sql import SparkSession

##### Creating table MySQL #####

connection = mysql.connector.connect(
    host='localhost',
    port='3306',
    user='admin',
    password='admin123',
    database='fin'
)

cursor = connection.cursor()

create_table_sql = """
CREATE TABLE IF NOT EXISTS currency (
    fromCurrency VARCHAR(255),
    toCurrency VARCHAR(255),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    date TIMESTAMP
    
)
"""

cursor.execute(create_table_sql)
connection.commit()
cursor.close()
connection.close()

#################################

spark = SparkSession.builder \
    .appName('etl') \
    .config('spark.jars', '/usr/share/java/mysql-connector-java-8.2.0.jar') \
    .config("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java-8.2.0.jar") \
    .getOrCreate()


df_spark = spark.read.csv('/home/liperoc/projects/etl_finance_pyspark/csv/currency', header=True)

try:
    df_spark.write.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/fin") \
        .option("dbtable", "currency") \
        .option("user", "admin") \
        .option("password", "admin123") \
        .mode("overwrite") \
        .save()
except Exception as e:
    print("Error: ", e)

spark.stop()