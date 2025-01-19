import unittest
import psycopg2


class TestPostgresDB(unittest.TestCase):
    def setUp(self):
        """
        Set up a connection to the PostgreSQL database.
        """
        self.connection = psycopg2.connect(
            dbname="postgres",  # Replace with your database name
            user="postgres",  # Replace with your username
            password="postgres",  # Replace with your password
            host="db",  # Replace with your host (e.g., dev container)
            port=5432  # Default PostgreSQL port
        )
        self.cursor = self.connection.cursor()

    def tearDown(self):
        """
        Tear down the database connection after each test.
        """
        self.cursor.close()
        self.connection.close()

    def test_users_exist(self):
        """
        Test if Alice and Bob exist in the database.
        """
        self.cursor.execute(
            "SELECT name FROM users WHERE name IN (%s, %s);", ('Alice', 'Bob'))
        users = [row[0] for row in self.cursor.fetchall()]

        # Assert that both Alice and Bob exist
        self.assertIn('Alice', users, "Alice should exist in the database")
        self.assertIn('Bob', users, "Bob should exist in the database")


if __name__ == "__main__":
    unittest.main()
