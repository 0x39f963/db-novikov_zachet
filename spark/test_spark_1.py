from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pg-smoke").getOrCreate()

url = "jdbc:postgresql://postgres:5432/hw"
props = {"user":"hw","password":"hwpass","driver":"org.postgresql.Driver"}

df = spark.read.jdbc(url, "(select 1 as ok) t", properties=props)
df.show()

spark.stop()