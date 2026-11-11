CREATE EXTERNAL IF NOT EXISTS curated_retail_sales_hive (
	order_id STRING,
	order_line_id STRING,
	sales_channel STRING,
	product_id STRING,
	category STRING,
	quantity INT,
	unit_price DECIMAL(12, 2),
	discount_amount DECIMAL(12, 2),
	currency STRING,
	source_file STRING,
	ingestion_ts TIMESTAMP,
	gross_amount DECIMAL(12, 2),
	net_sales DECIMAL(12, 2)
)
PARTITIONED BY (store_id STRING, order_date STRING)
STORED AS PARQUET
LOCATION '/tmp/curated_retail_sales';

MSCK REPAIR TABLE curated_retail_sales_hive;

SELECT
	order_date,
	store_id,
	sales_channel,
	COUNT(DISTINCT order_id) AS total_orders,
	SUM(quantity) AS total_units_sold,
	CAST(SUM(net_sales) AS DECIMAL(14, 2)) AS total_net_sales_inr,
	CAST(AVG(net_sales) AS DECIMAL(12, 2)) AS avg_order_value_inr
FROM curated_retail_sales_hive
GROUP BY order_date, store_id, sales_channel
ORDER BY order_date DESC, total_net_sales_inr DESC;

SELECT
	category,
	product_id,
	SUM(quantity) AS total_units_sold,
	CAST(SUM(gross_amount) AS DECIMAL(14, 2)) AS total_gross_sales_inr,
	CAST(SUM(discount_amount) AS DECIMAL(14, 2)) AS total_discounts_given_inr,
	CAST(SUM(net_sales) AS DECIMAL(14, 2)) AS total_net_revenue_inr
FROM curated_retail_sales_hive
GROUP BY category, product_id
ORDER BY total_net_revenue_inr DESC
LIMIT 10;

SELECT
	source_file,
	COUNT(*) AS accepted_record_count,
	CAST(SUM(net_sales) AS DECIMAL(14, 2)) AS total_batch_revenue_inr,
	MIN(ingestion_ts) AS batch_start_time,
	MAX(ingestion_ts) AS batch_end_time
FROM curated_retail_sales_hive
GROUP BY source_file;
