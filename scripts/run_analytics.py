from pyspark.sql import SparkSession
spark = SparkSession.builder \
.appName("RetailAnalytics") \
.enableHiveSupport() \
.getOrCreate()
df = spark.read.parquet("/tmp/curated_retail_sales")
df.createOrReplaceTempView("curated_retail_sales_hive")
print("---TOP 10 PRODUCTS BY NET REVENUE ---")
spark.sql("""
SELECT
category,
product_id,
SUM(quantity) AS total_units_sold,
CAST(SUM(net_sales) AS DECIMAL(14, 2)) AS total_net_revenue_inr
FROM curated_retail_sales_hive
GROUP BY category, product_id
ORDER BY total_net_revenue_inr DESC
LIMIT 10
""").show(truncate=False)
print("---BATCH AUDIT METRICS---")
spark.sql("""
SELECT
source_fle,
COUNT(*) AS accepted_record_count,
CAST(SUM(net_sales) AS DECIMAL(14, 2)) AS total_batch_revenue_inr
FROM curated_retail_sales_hive
GROUP BY source_file
""").show(truncate=False)
