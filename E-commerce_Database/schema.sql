-- =====================================================
-- E-COMMERCE DATABASE SCHEMA
-- PostgreSQL Database for E-Commerce Platform
-- =====================================================
-- Description: Complete database schema for managing
-- customers, sellers, products, orders, inventory,
-- payments, and shipping operations
-- =====================================================

-- Drop existing tables (if any) to start fresh
DROP TABLE IF EXISTS shipping;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS sellers;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS category;

-- =====================================================
-- TABLE 1: CATEGORY
-- =====================================================
-- Purpose: Store product categories
-- =====================================================
CREATE TABLE category
(
    category_id INT PRIMARY KEY,
    category_name VARCHAR(20) NOT NULL
);

-- =====================================================
-- TABLE 2: CUSTOMERS
-- =====================================================
-- Purpose: Store customer information
-- =====================================================
CREATE TABLE customers
(
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(20) NOT NULL,
    last_name VARCHAR(20) NOT NULL,
    state VARCHAR(20) NOT NULL
);

-- =====================================================
-- TABLE 3: SELLERS
-- =====================================================
-- Purpose: Store seller/vendor information
-- =====================================================
CREATE TABLE sellers
(
    seller_id INT PRIMARY KEY,
    seller_name VARCHAR(25) NOT NULL,
    origin VARCHAR(10) NOT NULL
);

-- =====================================================
-- TABLE 4: PRODUCTS
-- =====================================================
-- Purpose: Store product catalog information
-- Columns:
--   - product_id: Unique identifier
--   - product_name: Name of the product
--   - price: Selling price
--   - cogs: Cost of Goods Sold
--   - category_id: Foreign key to category table
-- =====================================================
CREATE TABLE products
(
    product_id INT PRIMARY KEY,
    product_name VARCHAR(50) NOT NULL,
    price FLOAT NOT NULL,
    cogs FLOAT NOT NULL,
    category_id INT,
    CONSTRAINT product_fk_category FOREIGN KEY(category_id) 
        REFERENCES category(category_id) ON DELETE SET NULL
);

-- =====================================================
-- TABLE 5: ORDERS
-- =====================================================
-- Purpose: Store order information
-- Columns:
--   - order_id: Unique identifier
--   - order_date: Date of order placement
--   - customer_id: Foreign key to customers table
--   - seller_id: Foreign key to sellers table
--   - order_status: Status of the order (Completed, Cancelled, Returned, etc.)
-- =====================================================
CREATE TABLE orders
(
    order_id INT PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id INT NOT NULL,
    seller_id INT NOT NULL,
    order_status VARCHAR(15) NOT NULL,
    CONSTRAINT orders_fk_customers FOREIGN KEY (customer_id) 
        REFERENCES customers(customer_id) ON DELETE CASCADE,
    CONSTRAINT orders_fk_sellers FOREIGN KEY (seller_id) 
        REFERENCES sellers(seller_id) ON DELETE CASCADE
);

-- =====================================================
-- TABLE 6: ORDER_ITEMS
-- =====================================================
-- Purpose: Store line items for each order
-- Columns:
--   - order_item_id: Unique identifier
--   - order_id: Foreign key to orders table
--   - product_id: Foreign key to products table
--   - quantity: Number of units ordered
--   - price_per_unit: Unit price at time of order
-- =====================================================
CREATE TABLE order_items
(
    order_item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price_per_unit FLOAT NOT NULL,
    CONSTRAINT order_items_fk_orders FOREIGN KEY (order_id) 
        REFERENCES orders(order_id) ON DELETE CASCADE,
    CONSTRAINT order_items_fk_products FOREIGN KEY (product_id) 
        REFERENCES products(product_id) ON DELETE CASCADE
);

-- =====================================================
-- TABLE 7: PAYMENTS
-- =====================================================
-- Purpose: Store payment information for orders
-- Columns:
--   - payment_id: Unique identifier
--   - order_id: Foreign key to orders table
--   - payment_date: Date of payment
--   - payment_status: Status (Payment_Successed, Failed, Pending, etc.)
-- =====================================================
CREATE TABLE payments
(
    payment_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    payment_date DATE NOT NULL,
    payment_status VARCHAR(20) NOT NULL,
    CONSTRAINT payments_fk_orders FOREIGN KEY (order_id) 
        REFERENCES orders(order_id) ON DELETE CASCADE
);

-- =====================================================
-- TABLE 8: SHIPPING
-- =====================================================
-- Purpose: Store shipping and delivery information
-- Columns:
--   - shipping_id: Unique identifier
--   - order_id: Foreign key to orders table
--   - shipping_date: Date item was shipped
--   - return_date: Date item was returned (if applicable)
--   - shipping_providers: Carrier name (e.g., FedEx, DHL, etc.)
--   - delivery_status: Status (Delivered, Pending, Delayed, etc.)
-- =====================================================
CREATE TABLE shipping
(
    shipping_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    shipping_date DATE NOT NULL,
    return_date DATE,
    shipping_providers VARCHAR(15) NOT NULL,
    delivery_status VARCHAR(15) NOT NULL,
    CONSTRAINT shippings_fk_orders FOREIGN KEY (order_id) 
        REFERENCES orders(order_id) ON DELETE CASCADE
);

-- =====================================================
-- TABLE 9: INVENTORY
-- =====================================================
-- Purpose: Store inventory/stock information by warehouse
-- Columns:
--   - inventory_id: Unique identifier
--   - product_id: Foreign key to products table
--   - stock: Current stock level
--   - warehouse_id: Warehouse location identifier
--   - last_stock_date: Date of last stock update
-- =====================================================
CREATE TABLE inventory
(
    inventory_id INT PRIMARY KEY,
    product_id INT NOT NULL,
    stock INT NOT NULL,
    warehouse_id INT NOT NULL,
    last_stock_date DATE NOT NULL,
    CONSTRAINT inventory_fk_products FOREIGN KEY (product_id) 
        REFERENCES products(product_id) ON DELETE CASCADE
);

-- =====================================================
-- INDEX CREATION FOR PERFORMANCE
-- =====================================================
-- Create indexes on frequently used columns for faster queries

CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_seller ON orders(seller_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_payments_order ON payments(order_id);
CREATE INDEX idx_shipping_order ON shipping(order_id);
CREATE INDEX idx_inventory_product ON inventory(product_id);

-- =====================================================
-- HELPER VIEW: Add total_sale column to order_items
-- =====================================================
-- This view will be useful for sales calculations
ALTER TABLE order_items
ADD COLUMN total_sale FLOAT;

UPDATE order_items
SET total_sale = quantity * price_per_unit
WHERE total_sale IS NULL;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================
-- Uncomment below to verify table creation

-- SELECT table_name 
-- FROM information_schema.tables 
-- WHERE table_schema = 'public'
-- ORDER BY table_name;

-- =====================================================
-- END OF SCHEMA
-- =====================================================
