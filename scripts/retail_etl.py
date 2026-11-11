import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, trim, lower, expr

def run_etl(input_file):
	spark= SparkSession.builder \
		.appName("RetailSalesETLPipeline") \
		.config("spark.sql.warehouse.dir", "/tmp/hive/warehouse") \
		.enableHiveSupport() \
		.getOrCreate()

	print(f"Reading raw data from: {input_file}")
	raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file)

	cleaned_df = raw_df \
		.withColumn("order_id", trim(col("order_id"))) \
		.withColumn("order_line_id", trim(col("order_line_id"))) \
		.withColumn("sales_channel", lower(trim(col("sales_channel")))) \
		.withColumn("quantity", col("quantity").cast("integer")) \
		.withColumn("unit_price", expr("try_cast(unit_price as decimal(12,2))")) \
		.withColumn("discount_amount", expr("try_cast(discount_amount as decimal(12,2))")) \
		.withColumn("source_fle", lit(input_file)) \
		.withColumn("ingestion_ts", current_timestamp())
	is_valid = (
		col("order_id").isNotNull() &
		col("order_line_id").isNotNull() &
		col("quantity").isNotNull() & (col("quantity") > 0) &
		col("unit_price").isNotNull() & (col("unit_price") >= 0.0) &
		col("discount_amount").isNotNull() & (col("discount_amount") >= 0) &
		(col("discount_amount") <= (col("quantity") * col("unit_price"))) &
		col("sales_channel").isin(["online", "store", "marketplace"])
	)

	valid_df = cleaned_df.filter(is_valid)
	rejected_df = cleaned_df.filter(~is_valid).withColumn("rejection_reason", lit("FAILED_BUSINESS_OR_SCHEMA_RULES"))

	accepted_count = valid_df.count()
	rejected_count = rejected_df.count()
	print(f"Accepted rows: {accepted_count} | Rejected rows: {rejected_count}")

	deduped_df = valid_df.dropDuplicates(["order_id", "order_line_id"])
	duplicate_count = accepted_count - deduped_df.count()
	print(f"Duplicate rows detected and removed: {duplicate_count}")

	final_transformed_df = deduped_df \
		.withColumn("gross_amount", col("quantity") * col("unit_price")) \
		.withColumn("net_sales", col("gross_amount") - col("discount_amount"))

	output_path = "/tmp/curated_retail_sales"
	print(f"Writing curated data to Parquet format at {output_path}")
	final_transformed_df.write \
		.mode("append") \
		.partitionBy("store_id", "order_date") \
		.parquet(output_path)

	print("ETL Pipeline Batch completed successfully.")
	spark.stop()

if __name__ == "__main__":
	input_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw_sales_data.csv"
	run_etl(input_path)
