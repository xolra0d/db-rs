CREATE DATABASE default
CREATE TABLE default.users (id UInt64, name String, age UInt8, email String) ENGINE = MergeTree ORDER BY (id, age)
INSERT INTO default.users (id, name, age, email) VALUES (1, 'Alice', 25, 'alice@example.com'), (2, 'Alice', 30, 'alice@example.com')
INSERT INTO default.users (id, name, age, email) VALUES (3, 'Charlie', 35, 'charlie@example.com'), (4, 'Diana', 28, 'diana@example.com')
SELECT name, age FROM default.users
SELECT name, age FROM default.users WHERE id > 2
