from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("mongo-smoke")
    .config("spark.mongodb.read.connection.uri", "mongodb://mongo:27017/hw")
    .getOrCreate()
)

try:
    df = spark.read.format("mongodb").option("collection","reviews").load()
    df.printSchema()
    print("rows:", df.limit(1).count())
except Exception as e:
    print("Mongo read failed:", e)

spark.stop()