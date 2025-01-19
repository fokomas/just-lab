-- seed.sql
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INTEGER NOT NULL
);

INSERT INTO users (name, age) VALUES
    ('Alice', 30),
    ('Bob', 25);
