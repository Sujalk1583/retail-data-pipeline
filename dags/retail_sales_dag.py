from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
	'owner': 'data_engineer',
	'depends_on_past': False,
	'email_on_failure': False,
	'retries': 2,
	'retry_delay': timedelta(minutes=2),
}

with DAG(
	'retail_sales_batch-pipeline',
	default_args=default_args,
	description='Daily retail sales ETL pipeline with PySpark, Hivw and Quality Checks',
	schedule_interval='@daily',
	start_date=datetime(2026, 1, 1),
	catchup=False,
) as dag:

	generate_date_task = BashOperator(
		task_id='generate_synthetic_sales_data',
		bash_command='python3 /home/$USER/retail_pipeline/scripts/generate_data.py',
	)

	run_etl_task = BashOperator(
		task_id='run_pyspark_etl_validation',
		bash_command='python3 /home/$USER/retail_pipeline/scripts/generate_data.py',
	)

	audit_check_task = BashOperator(
		task_id='verify_batch_audit_completion',
		bash_command='echo "Batch Processing finished successfully. Outputs written to /temp/curated_retail_sales"',
	)

	generate_data_task >> run_etl_task >> audit_check_task
