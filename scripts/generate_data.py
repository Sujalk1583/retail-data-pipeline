import os
import csv
import random
from datetime import datetime, timedelta
def generate_retail_csv():
	base_dir = "/home/bigdata/retail_pipeline"
	data_dir = os.path.join(base_dir, "data")
	os.makedirs(data_dir, exist_ok=True)
	filename = os.path.join(data_dir, "raw_sales_data.csv")
	num_rows = 1000
	stores = ["STORE_01", "STORE_02", "STORE_03"]
	channels = ["online", "store", "marketplace", "invalid_channel"]
	categories = ["Electronics", "Apparel", "Home", "Groceries"]
	headers = [
		"order_id", "order_line_id", "order_date", "store_id",
		"sales_channel", "product_id", "category", "quantity",
		"unit_price", "discount_amount", "currency"
	]
	print(f"Writing {num_rows} rows to: {filename}")
	with open(filename, mode="w", newline="", encoding="utf-8") as f:
		writer = csv.writer(f)
		writer.writerow(headers)
		base_date = datetime.now() - timedelta(days=10)
		for i in range(1, num_rows + 1):
			order_id = f"ORD_{1000 + (i % 200)}"
			line_id = f"L_{i}"
			order_date = (base_date + timedelta(days=random.randint(0, 9))).strftime("%Y-%m-%d")
			store_id = random.choice(stores)
			channel = random.choice(channels)
			product_id = f"PROD_{random.randint(1, 50)}"
			category = random.choice(categories)
			if i % 50 == 0:
				quantity = -5
			elif i % 37 == 0:
				quantity =""
			else:
				quantity = random.randint(1, 5)
			unit_price = round(random.uniform(150.0, 8500.0), 2)
			discount_amount = round(unit_price * random.choice([0.0, 0.05,  0.1, 0.15]))
			if i % 73 == 0:
				discount_amount = 99999.0
			currency = "INR"
			writer.writerow([
				order_id, line_id, order_date, store_id,
				channel, product_id, category, quantity,
				unit_price, discount_amount, currency
			])
	print("File write complete and closed.")
if __name__ == "__main__":
	generate_retail_csv()

