Start airflow

1. cd airflow
2. echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
3. docker compose up airflow-init
4. docker compose up -d

Start databases

1. cd databases
2. docker compose up -d
3. run sql to init databases

Access to airflow Web UI

1. http://localhost:8080
2. username & password : airflow / airflow
3. Admin > Connections
4. Add connections for mysql(origin) and postgres(target)
5. Trg DAGs > insert_users_to_mysql_with_connection, migrate_mysql_to_postgres

reference :
https://airflow.apache.org/docs/apache-airflow/2.1.1/start/docker.html

python code are generated from ChatGPT
