-- Create a table with custom index_granularity
CREATE TABLE default.users (
    id UInt64,
    name String,
    age UInt8,
    email String
) ENGINE = MergeTree
ORDER BY (id, age)
SETTINGS index_granularity = 1024;

-- Insert sample data
INSERT INTO default.users (id, name, age, email) VALUES
(1, 'Alice', 25, 'alice@example.com'),
(2, 'Bob', 30, 'bob@example.com'),
(3, 'Charlie', 35, 'charlie@example.com'),
(4, 'Diana', 28, 'diana@example.com'),
(5, 'Eve', 32, 'eve@example.com');

-- Create another table with default index_granularity (8192)
CREATE TABLE default.products (
    product_id UInt32,
    product_name String,
    price UInt32,
    category String
) ENGINE = MergeTree
ORDER BY (category, product_id);

-- Insert sample products
INSERT INTO default.products (product_id, product_name, price, category) VALUES
(101, 'Laptop', 999, 'Electronics'),
(102, 'Mouse', 25, 'Electronics'),
(103, 'Desk', 299, 'Furniture'),
(104, 'Chair', 149, 'Furniture'),
(105, 'Monitor', 399, 'Electronics');
