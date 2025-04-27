import psycopg2

connection = psycopg2.connect(
    dbname="postgres",  # Replace with your database name
    user="postgres",  # Replace with your username
    password="postgres",  # Replace with your password
    host="db",  # Replace with your host (e.g., dev container)
    port=5432  # Default PostgreSQL port
)
cursor = connection.cursor()

cursor.execute(
    "SELECT name FROM users WHERE name IN (%s, %s);", ('Alice', 'Bob'))
users = [row[0] for row in cursor.fetchall()]

print(users)
print("done!")

cursor.close()
connection.close()
