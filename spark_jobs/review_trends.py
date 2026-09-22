import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# args: <output_bucket> <category1> [<category2> ...]
output_bucket = sys.argv[1]
categories = sys.argv[2:] or ["Electronics", "Books"]

spark = (
    SparkSession.builder.appName("review_trends")
    .config("spark.sql.shuffle.partitions", "96")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

paths = [f"s3://amazon-reviews-pds/parquet/product_category={c}/" for c in categories]

df = (
    spark.read.parquet(*paths)
    .select("product_category", "product_id", "product_title", "star_rating", "review_date")
    .withColumn("year", F.year("review_date"))
    .withColumn("month", F.month("review_date"))
)

trend = df.groupBy("product_category", "year", "month").agg(
    F.avg("star_rating").alias("avg_rating"),
    F.count("*").alias("review_count"),
)

product_counts = (
    df.groupBy("product_category", "product_id", "product_title", "year")
    .agg(
        F.count("*").alias("review_count"),
        F.avg("star_rating").alias("avg_rating"),
    )
)
product_counts.persist()

rank_window = Window.partitionBy("product_category", "year").orderBy(F.desc("review_count"))
top_products = product_counts.withColumn("rank", F.rank().over(rank_window)).filter(F.col("rank") <= 10)

trend.repartition(4, "product_category").write.mode("overwrite").partitionBy(
    "product_category", "year"
).parquet(f"s3://{output_bucket}/analytics/reviews/monthly_trend/")

top_products.repartition(4, "product_category").write.mode("overwrite").partitionBy(
    "product_category", "year"
).parquet(f"s3://{output_bucket}/analytics/reviews/top_products/")

product_counts.unpersist()
spark.stop()
