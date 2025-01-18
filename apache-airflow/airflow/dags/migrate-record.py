from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def get_last_migration_time_from_mysql():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_1')  # MySQL connection ID
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    
    # Fetch the last migration time
    select_query = "SELECT migration_time FROM migration_log WHERE trigger_by = 'migrate_mysql_to_postgres' AND success = 1 ORDER BY migration_time DESC LIMIT 1"
    cursor.execute(select_query)
    result = cursor.fetchone()
    
    cursor.close()
    connection.close()
    
    # Return the last migration time or a default time if no record found
    if result:
        return result[0]
    else:
        return datetime(1970, 1, 1)  # Default time, for the first run

# Function to check and create the migration_log table in MySQL
def create_migration_log_table_mysql():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_1')  # MySQL connection ID
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    # Create table if not exists for MySQL
    create_table_query = """
    CREATE TABLE IF NOT EXISTS migration_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        migration_time TIMESTAMP NOT NULL,
        success INT NOT NULL,
        migrated_count INT NOT NULL,
        trigger_by VARCHAR(255) NOT NULL
    )
    """
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()
    connection.close()
    print("Created migration_log table in MySQL (if not exists)")

# Function to check and create the migration_log table in PostgreSQL
def create_migration_log_table_postgres():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_1')  # PostgreSQL connection ID
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    # Create table if not exists for PostgreSQL
    create_table_query = """
    CREATE TABLE IF NOT EXISTS migration_log (
        id SERIAL PRIMARY KEY,
        migration_time TIMESTAMP NOT NULL,
        success INT NOT NULL,
        migrated_count INT NOT NULL,
        trigger_by VARCHAR(255) NOT NULL
    )
    """
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()
    connection.close()
    print("Created migration_log table in PostgreSQL (if not exists)")

# Function to fetch only newly added records from MySQL since the last migration
def fetch_records_from_mysql():
    last_migration_time = get_last_migration_time_from_mysql()

    print(f"last_migration_time: {last_migration_time} ")

    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_1')  # MySQL connection ID
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    # Query to fetch records created after the last migration time
    select_query = """
    SELECT id, name, year, create_date FROM users
    WHERE create_date > %s
    """
    cursor.execute(select_query, (last_migration_time,))
    records = cursor.fetchall()
    
    cursor.close()
    connection.close()
    return records

# Function to insert records into PostgreSQL
def insert_records_to_postgres(records):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_1')  # PostgreSQL connection ID
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    
    # Query to insert records into PostgreSQL with the migrate_date column
    insert_query = """
    INSERT INTO users (id, name, year, create_date, migrate_date)
    VALUES (%s, %s, %s, %s, NOW())
    """
    cursor.executemany(insert_query, records)  # Insert multiple records at once
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Inserted {len(records)} records into PostgreSQL")

# Function to insert log into MySQL migration_log table
def log_migration_to_mysql(success, migrated_count):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_1')  # MySQL connection ID
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    # Insert log entry into MySQL migration_log table
    log_query = """
    INSERT INTO migration_log (migration_time, success, migrated_count, trigger_by)
    VALUES (NOW(), %s, %s, %s)
    """
    success_int = 1 if success else 0
    cursor.execute(log_query, (success_int, migrated_count, 'migrate_mysql_to_postgres'))
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Logged migration to MySQL with {migrated_count} records")

# Function to insert log into PostgreSQL migration_log table
def log_migration_to_postgres(success, migrated_count):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_1')  # PostgreSQL connection ID
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    # Insert log entry into PostgreSQL migration_log table
    log_query = """
    INSERT INTO migration_log (migration_time, success, migrated_count, trigger_by)
    VALUES (NOW(), %s, %s, %s)
    """
    success_int = 1 if success else 0
    cursor.execute(log_query, (success_int, migrated_count, 'migrate_mysql_to_postgres'))
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Logged migration to PostgreSQL with {migrated_count} records")

# Function to migrate records from MySQL to PostgreSQL and log the migration
def migrate_mysql_to_postgres():
    migrated_count = 0
    try:
        records = fetch_records_from_mysql()  # Fetch records from MySQL
        if records:
            # Insert records into PostgreSQL
            migrated_count = len(records)
            insert_records_to_postgres(records)
            success = True
        else:
            success = False
    except Exception as e:
        print(f"Migration failed with error: {e}")
        success = False
    
    # Log the migration in both MySQL and PostgreSQL
    log_migration_to_mysql(success, migrated_count)
    log_migration_to_postgres(success, migrated_count)

    if not success:
        raise ValueError("migration not success")

    return success, migrated_count

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),  # Set the start date
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'migrate_mysql_to_postgres',
    default_args=default_args,
    description='Migrate only newly added records from MySQL to PostgreSQL with a migrate_date column and log migration',
    schedule_interval='*/3 * * * *',  # Runs every 10 minutes (or adjust as needed)
    catchup=False,
)

# Define the PythonOperator for table creation and migration
create_mysql_table_task = PythonOperator(
    task_id='create_mysql_table',
    python_callable=create_migration_log_table_mysql,
    dag=dag,
)

create_postgres_table_task = PythonOperator(
    task_id='create_postgres_table',
    python_callable=create_migration_log_table_postgres,
    dag=dag,
)

# Define the PythonOperator for migration task
migrate_task = PythonOperator(
    task_id='migrate_mysql_to_postgres_task',
    python_callable=migrate_mysql_to_postgres,
    dag=dag,
)

# Set task dependencies
create_mysql_table_task >> create_postgres_table_task >> migrate_task
