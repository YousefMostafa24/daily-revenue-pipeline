from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import matplotlib.pyplot as plt


# PostgreSQL connection ID configured in Airflow
PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


# Fetch order date including prices from products
def fetch_order_data():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()


    query = """
        select 
	        o.OrderDate as sales_date,
	        od.ProductID,
	        p.ProductName,
	        od.Quantity,
	        p.Price	 
        from orders o join order_details od 
        on o.orderid = od.orderid 
        join products p 
        on p.productid  = od.productid 
    """


    df = pd.read_sql(query ,conn)
    df.to_csv('/home/kiwilytics/Documents/airflow_output/fetch_sales_data.csv' ,index=False)

# process total daily revenue
def process_daily_revenue():
    df = pd.read_csv('/home/kiwilytics/Documents/airflow_output/fetch_sales_data.csv')
    df['total_revenue'] = df['quantity'] * df['price']

    daily_revenue = df.groupby('sales_date')['total_revenue'].sum().reset_index()
    daily_revenue.to_csv('/home/kiwilytics/Documents/airflow_output/daily_revenue.csv' ,index=False)


# Visualization_of_total_revenue
def visualize_total_revenue():
    df =pd.read_csv('/home/kiwilytics/Documents/airflow_output/daily_revenue.csv')
    df['sales_date'] =pd.to_datetime(df['sales_date'])


    plt.figure(figsize=(12,6))
    plt.plot(df['sales_date'], df['total_revenue'], marker='o', linestyle='-')
    plt.title("Dialily Total Sales Revenue")
    plt.xlabel("Date")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()


    output_path = '/home/kiwilytics/Documents/airflow_output/Total_Revenue_Visualization.png'
    plt.savefig(output_path)
    plt.close()
    print(f"Revenue Saved {output_path}")


#DAG
with DAG(
    dag_id ='daily_total_revenue_analysis',
    default_args =default_args,
    start_date =days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    description='Compute and visualize daily revenue using pandas and matplotlib in Airflow',
) as dag:
    
    task_fetch_data = PythonOperator(
        task_id ='fetch_order_data',
        python_callable= fetch_order_data,
    )

    task_process_revenue = PythonOperator(
        task_id ='process_daily_revenue',
        python_callable= process_daily_revenue,
    )

    task_visualization_revenue = PythonOperator(
        task_id ='visualize_total_revenue',
        python_callable= visualize_total_revenue,
    )


task_fetch_data >> task_process_revenue >> task_visualization_revenue
