from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import random
import string
import time

# Function to generate a random name and birth year
def generate_random_record():
    random.seed(time.time())
    random_name = ''.join(random.choices(string.ascii_letters, k=8))  # Random 8-character name
    random_year = random.randint(1900, 2023)  # Random 4-digit birth year
    return random_name, random_year

# Function to insert a record into the MySQL database
def insert_record_to_mysql():
    name, year = generate_random_record()
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_1')  # Using the Airflow connection
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    
    insert_query = "INSERT INTO users (name, year, create_date) VALUES (%s, %s, NOW())"
    cursor.execute(insert_query, (name, year))
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Inserted record: {name}, {year}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),  # Set start date
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'insert_users_to_mysql_with_connection',
    default_args=default_args,
    description='Insert random users into MySQL every minute using Airflow connections',
    schedule_interval='*/1 * * * *',  # Runs every minute
    catchup=False,
)

# Define the PythonOperator
insert_task = PythonOperator(
    task_id='insert_record',
    python_callable=insert_record_to_mysql,
    dag=dag,
)
