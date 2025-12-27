from pyspark.sql import SparkSession, functions as F

PG_URL = "jdbc:postgresql://postgres:5432/hw"
PG_PROPS = {"user": "hw", "password": "hwpass", "driver": "org.postgresql.Driver"}

MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "hw"
MONGO_COLL = "reviews"


# docker compose exec -T postgres psql -U hw -d hw -c "TRUNCATE TABLE product_analytics_monthly;"


def pg_exec(spark, sql: str) -> None:
    jvm = spark._sc._gateway.jvm

    # АААААааааа
    jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry.register(
        PG_PROPS["driver"]
    )

    conn = jvm.java.sql.DriverManager.getConnection(
        PG_URL, PG_PROPS["user"], PG_PROPS["password"]
    )
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def main():
    spark = (
        SparkSession.builder.appName("product-analytics-monthly")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.read.database", MONGO_DB)
        .config("spark.mongodb.read.collection", MONGO_COLL)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("SET spark.sql.session.timeZone=UTC")

    # 1. читаем из постгри (jdbc)
    orders_df = spark.read.jdbc(PG_URL, "orders", properties=PG_PROPS).select(
        F.col("order_id").cast("int"),
        F.col("customer_id").cast("int"),
        F.col("order_date").cast("timestamp"),
    )
    orders_df.describe()

    order_items_df = spark.read.jdbc(PG_URL, "order_items", properties=PG_PROPS).select(
        F.col("order_id").cast("int"),
        F.col("product_id").cast("int"),
        F.col("quantity").cast("int"),
        F.col("price").cast("double"),
    )
    print(order_items_df)

    # 2. последний месяц
    # !!! LOOK: в чате писали, что сентябрь можно убрать / что это заапрувлено
    period = orders_df.select(
        F.max("order_date").alias("max_dt"),
        F.add_months(F.date_trunc("month", F.max("order_date")), -1).alias("ms"),
        F.date_trunc("month", F.max("order_date")).alias("me"),
    ).first()

    max_dt = period["max_dt"]
    ms = period["ms"]
    me = period["me"]

    print(f"max_dt: {max_dt} ms:{ms}, me:{me}")

    # 3. фильтр по посл месяцу
    orders_m = orders_df.where(
        (F.col("order_date") >= F.lit(ms)) & (F.col("order_date") < F.lit(me))
    )
    sales = order_items_df.join(
        orders_m.select("order_id"), on="order_id", how="inner"
    ).select("order_id", "product_id", "quantity", "price")

    # 4. отзывы, отфильтрованные по посл месяцу
    reviews_df = (
        spark.read.format("mongodb")
        .load()
        .select(
            F.col("product_id").cast("int").alias("product_id"),
            F.col("rating").cast("int").alias("rating"),
            F.col("created_at").cast("timestamp").alias("created_at"),
        )
    )

    reviews_m = reviews_df.where(
        (F.col("created_at") >= F.lit(ms)) & (F.col("created_at") < F.lit(me))
    )

    # 5. temp view для агрегации
    sales.createOrReplaceTempView("sales")
    reviews_m.createOrReplaceTempView("reviews_m")

    sales_agg = spark.sql(
        """
        SELECT
          product_id,
          COUNT(DISTINCT order_id)      AS order_count,
          SUM(CAST(quantity AS BIGINT)) AS total_quantity,
          SUM(quantity * price)         AS total_revenue
        FROM sales
        GROUP BY product_id
    """
    )
    print("sales_agg:")
    sales_agg.orderBy("product_id").show(50, truncate=False) # p.s. в таблице product у нас 50 записей

    reviews_agg = spark.sql(
        """
        SELECT
          product_id,
          AVG(CAST(rating AS DOUBLE)) AS avg_rating,
          COUNT(1)                    AS total_reviews,
          SUM(CASE WHEN rating IN (4,5) THEN 1 ELSE 0 END) AS positive_reviews,
          SUM(CASE WHEN rating IN (1,2) THEN 1 ELSE 0 END) AS negative_reviews
        FROM reviews_m
        GROUP BY product_id
    """
    )
    print("reviews_agg:")
    reviews_agg.orderBy("product_id").show(50, truncate=False)

    # 6. объединяем
    result = (
        sales_agg.join(reviews_agg, on="product_id", how="full")
        .na.fill(
            {
                "order_count": 0,
                "total_quantity": 0,
                "total_revenue": 0.0,
                "total_reviews": 0,
                "positive_reviews": 0,
                "negative_reviews": 0,
            }
        )
        .withColumn("processing_date", F.to_date(F.lit(ms)))
        .select(
            "product_id",
            "total_quantity",
            "total_revenue",
            "order_count",
            "avg_rating",
            "positive_reviews",
            "negative_reviews",
            "total_reviews",
            "processing_date",
        )
    )

    print("result:")
    result.orderBy("product_id").show(50, truncate=False) 

    # 7. записываем результаты 

    pg_exec(spark, "TRUNCATE TABLE product_analytics_monthly;")

    result.write.mode("append").jdbc(
        PG_URL, "product_analytics_monthly", properties=PG_PROPS
    )

    spark.stop()


if __name__ == "__main__":
    main()
