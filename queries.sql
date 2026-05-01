-- =====================================================
-- E-COMMERCE DATABASE - BUSINESS PROBLEM QUERIES
-- =====================================================
-- Collection of 20 advanced SQL queries solving
-- real-world business analytics problems
-- =====================================================

-- =====================================================
-- QUERY 1: TOP SELLING PRODUCTS
-- =====================================================
-- Purpose: Identify top 10 products by total sales value
-- Metrics: Product name, quantity sold, total revenue
-- Use Case: Inventory planning, marketing focus
-- =====================================================
SELECT 
    p.product_name, 
    COUNT(oi.order_id) AS total_quantity_sold,
    ROUND(SUM(oi.quantity * oi.price_per_unit)::NUMERIC, 2) AS total_sales
FROM orders AS o
JOIN order_items AS oi ON o.order_id = oi.order_id
JOIN products AS p ON oi.product_id = p.product_id
GROUP BY p.product_name 
ORDER BY total_sales DESC
LIMIT 10;

-- =====================================================
-- QUERY 2: REVENUE BY CATEGORY
-- =====================================================
-- Purpose: Calculate revenue per category with contribution %
-- Metrics: Category, total sales, percentage contribution
-- Use Case: Category performance analysis
-- =====================================================
SELECT 
    p.category_id,
    c.category_name,
    ROUND(SUM(oi.quantity * oi.price_per_unit)::NUMERIC, 2) AS total_sales,
    ROUND((SUM(oi.quantity * oi.price_per_unit) / 
        (SELECT SUM(quantity * price_per_unit) FROM order_items) * 100)::NUMERIC, 2) AS contribution_percentage
FROM order_items AS oi
JOIN products AS p ON p.product_id = oi.product_id
LEFT JOIN category AS c ON c.category_id = p.category_id
GROUP BY p.category_id, c.category_name
ORDER BY total_sales DESC;

-- =====================================================
-- QUERY 3: AVERAGE ORDER VALUE (AOV)
-- =====================================================
-- Purpose: Compute AOV for customers with 5+ orders
-- Metrics: Customer ID, name, AOV, order count
-- Use Case: Customer segmentation and targeting
-- =====================================================
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    ROUND(SUM(oi.quantity * oi.price_per_unit)::NUMERIC / COUNT(o.order_id), 2) AS aov,
    COUNT(o.order_id) AS total_orders
FROM orders AS o
JOIN customers AS c ON c.customer_id = o.customer_id
JOIN order_items AS oi ON oi.order_id = o.order_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING COUNT(o.order_id) > 5
ORDER BY aov DESC;

-- =====================================================
-- QUERY 4: MONTHLY SALES TREND (2-Year Analysis)
-- =====================================================
-- Purpose: Track sales trends over past 2 years with MoM comparison
-- Metrics: Year, Month, Current sales, Previous month sales
-- Use Case: Seasonal trend analysis, growth tracking
-- =====================================================
WITH monthly_sales AS 
(
    SELECT 
        EXTRACT(MONTH FROM o.order_date) AS month,
        EXTRACT(YEAR FROM o.order_date) AS year,
        ROUND(SUM(oi.total_sale)::NUMERIC, 2) AS total_sales
    FROM orders AS o
    JOIN order_items AS oi ON o.order_id = oi.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '2 years'
    GROUP BY EXTRACT(MONTH FROM o.order_date), EXTRACT(YEAR FROM o.order_date)
    ORDER BY year, month
)
SELECT 
    year,
    month,
    total_sales,
    LAG(total_sales, 1) OVER(ORDER BY year, month) AS last_month_sales
FROM monthly_sales;

-- =====================================================
-- QUERY 5: CUSTOMERS WITH NO PURCHASES
-- =====================================================
-- Purpose: Identify registered customers who never ordered
-- Metrics: Customer ID, name, state
-- Use Case: Re-engagement campaigns, customer onboarding
-- =====================================================
SELECT 
    customer_id,
    CONCAT(first_name, ' ', last_name) AS full_name,
    state
FROM customers
WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders)
ORDER BY customer_id;

-- =====================================================
-- QUERY 6: LEAST-SELLING CATEGORIES BY STATE
-- =====================================================
-- Purpose: Identify underperforming categories per state
-- Metrics: State, category, total sales
-- Use Case: Geographic market analysis, product optimization
-- =====================================================
WITH category_sales_by_state AS
(
    SELECT 
        c.state,
        cg.category_name,
        ROUND(SUM(oi.total_sale)::NUMERIC, 2) AS total_sales,
        RANK() OVER(PARTITION BY c.state ORDER BY SUM(oi.total_sale)) AS rank
    FROM customers AS c
    JOIN orders AS o ON c.customer_id = o.customer_id
    JOIN order_items AS oi ON o.order_id = oi.order_id
    JOIN products AS p ON oi.product_id = p.product_id
    JOIN category AS cg ON p.category_id = cg.category_id
    GROUP BY c.state, cg.category_name
)
SELECT 
    state,
    category_name,
    total_sales
FROM category_sales_by_state
WHERE rank = 1
ORDER BY state;

-- =====================================================
-- QUERY 7: CUSTOMER LIFETIME VALUE (CLTV)
-- =====================================================
-- Purpose: Calculate total purchase value per customer
-- Metrics: Customer ID, name, CLTV, ranking
-- Use Case: VIP identification, retention strategy
-- =====================================================
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    SUM(oi.total_sale) AS cltv,
    DENSE_RANK() OVER(ORDER BY SUM(oi.total_sale) DESC) AS customer_ranking
FROM orders AS o
JOIN customers AS c ON c.customer_id = o.customer_id
JOIN order_items AS oi ON oi.order_id = o.order_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY cltv DESC;

-- =====================================================
-- QUERY 8: INVENTORY STOCK ALERTS
-- =====================================================
-- Purpose: Flag products with low stock (< 10 units)
-- Metrics: Inventory ID, product name, current stock, last stock date
-- Use Case: Reorder alerts, supply chain management
-- =====================================================
SELECT 
    i.inventory_id,
    p.product_name,
    i.stock AS current_stock_left,
    i.last_stock_date,
    i.warehouse_id
FROM inventory AS i
JOIN products AS p ON p.product_id = i.product_id
WHERE i.stock < 10
ORDER BY i.stock ASC;

-- =====================================================
-- QUERY 9: SHIPPING DELAYS
-- =====================================================
-- Purpose: Identify orders shipped more than 3 days after order date
-- Metrics: Customer name, product, carrier, dates, delay info
-- Use Case: Logistics optimization, SLA tracking
-- =====================================================
SELECT 
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    p.product_name,
    s.shipping_providers,
    o.order_date,
    s.shipping_date,
    (s.shipping_date - o.order_date) AS days_to_ship
FROM orders AS o
JOIN shipping AS s ON o.order_id = s.order_id
JOIN customers AS c ON o.customer_id = c.customer_id
JOIN order_items AS oi ON oi.order_id = o.order_id
JOIN products AS p ON oi.product_id = p.product_id
WHERE (s.shipping_date - o.order_date) > 3
ORDER BY days_to_ship DESC;

-- =====================================================
-- QUERY 10: PAYMENT SUCCESS RATE
-- =====================================================
-- Purpose: Calculate payment success percentage with status breakdown
-- Metrics: Payment status, count, percentage
-- Use Case: Payment gateway analysis, fraud detection
-- =====================================================
SELECT 
    p.payment_status,
    COUNT(*) AS total_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM payments)::NUMERIC * 100, 2) AS percentage
FROM payments AS p
GROUP BY p.payment_status
ORDER BY total_count DESC;

-- =====================================================
-- QUERY 11: TOP PERFORMING SELLERS
-- =====================================================
-- Purpose: Rank top 5 sellers by sales with success metrics
-- Metrics: Seller ID, name, completed orders, cancelled orders, success rate
-- Use Case: Vendor performance management, commission calculation
-- =====================================================
WITH top_sellers AS
(
    SELECT 
        s.seller_id,
        s.seller_name,
        SUM(oi.total_sale) AS total_sale
    FROM orders AS o
    JOIN sellers AS s ON o.seller_id = s.seller_id
    JOIN order_items AS oi ON oi.order_id = o.order_id
    GROUP BY s.seller_id, s.seller_name
    ORDER BY total_sale DESC
    LIMIT 5
),
seller_order_status AS
(
    SELECT 
        o.seller_id,
        ts.seller_name,
        o.order_status,
        COUNT(*) AS total_orders
    FROM orders AS o
    JOIN top_sellers AS ts ON ts.seller_id = o.seller_id
    WHERE o.order_status NOT IN ('Inprogress', 'Returned')
    GROUP BY o.seller_id, ts.seller_name, o.order_status
)
SELECT 
    seller_id,
    seller_name,
    SUM(CASE WHEN order_status = 'Completed' THEN total_orders ELSE 0 END) AS completed_orders,
    SUM(CASE WHEN order_status = 'Cancelled' THEN total_orders ELSE 0 END) AS cancelled_orders,
    SUM(total_orders) AS total_orders,
    ROUND(SUM(CASE WHEN order_status = 'Completed' THEN total_orders ELSE 0 END)::NUMERIC / 
        SUM(total_orders)::NUMERIC * 100, 2) AS successful_orders_percentage
FROM seller_order_status
GROUP BY seller_id, seller_name
ORDER BY successful_orders_percentage DESC;

-- =====================================================
-- QUERY 12: PRODUCT PROFIT MARGIN
-- =====================================================
-- Purpose: Calculate and rank products by profit margin
-- Metrics: Product ID, name, profit margin %, ranking
-- Use Case: Pricing strategy, product profitability analysis
-- =====================================================
SELECT 
    product_id,
    product_name,
    profit_margin,
    DENSE_RANK() OVER (ORDER BY profit_margin DESC) AS product_ranking
FROM
(
    SELECT 
        p.product_id,
        p.product_name,
        ROUND(
            (SUM((oi.price_per_unit * oi.quantity) - (p.cogs * oi.quantity)) * 100.0 /
            NULLIF(SUM(oi.price_per_unit * oi.quantity), 0))::NUMERIC, 2) AS profit_margin
    FROM order_items AS oi
    JOIN products AS p ON oi.product_id = p.product_id
    GROUP BY p.product_id, p.product_name
) AS profit_analysis
ORDER BY profit_margin DESC;

-- =====================================================
-- QUERY 13: MOST RETURNED PRODUCTS
-- =====================================================
-- Purpose: Identify top 10 returned products with return rate %
-- Metrics: Product ID, name, total sold, total returned, return rate %
-- Use Case: Quality control, customer satisfaction, product issues
-- =====================================================
SELECT 
    p.product_id,
    p.product_name,
    COUNT(*) AS total_units_sold,
    SUM(CASE WHEN o.order_status = 'Returned' THEN 1 ELSE 0 END) AS total_returned,
    ROUND(SUM(CASE WHEN o.order_status = 'Returned' THEN 1 ELSE 0 END)::NUMERIC / 
        COUNT(*)::NUMERIC * 100, 2) AS return_rate_percentage
FROM order_items AS oi
JOIN products AS p ON oi.product_id = p.product_id
JOIN orders AS o ON o.order_id = oi.order_id
GROUP BY p.product_id, p.product_name
ORDER BY return_rate_percentage DESC
LIMIT 10;

-- =====================================================
-- QUERY 14: ORDERS PENDING SHIPMENT
-- =====================================================
-- Purpose: Find paid orders awaiting shipment
-- Metrics: Order details, payment date, customer info
-- Use Case: Fulfillment tracking, order processing
-- =====================================================
SELECT 
    o.order_id,
    o.order_date,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    p.payment_date,
    p.payment_status,
    o.order_status
FROM orders AS o
JOIN payments AS p ON o.order_id = p.order_id
JOIN customers AS c ON o.customer_id = c.customer_id
WHERE p.payment_status = 'Payment_Successed' 
    AND o.order_id NOT IN (SELECT DISTINCT order_id FROM shipping)
ORDER BY o.order_date;

-- =====================================================
-- QUERY 15: INACTIVE SELLERS
-- =====================================================
-- Purpose: Identify sellers inactive for 6+ months
-- Metrics: Seller ID, last sale date, last sale amount
-- Use Case: Seller management, platform optimization
-- =====================================================
WITH inactive_sellers AS
(
    SELECT seller_id FROM sellers
    WHERE seller_id NOT IN (
        SELECT DISTINCT seller_id FROM orders 
        WHERE order_date >= CURRENT_DATE - INTERVAL '6 months'
    )
)
SELECT 
    o.seller_id,
    s.seller_name,
    MAX(o.order_date) AS last_sale_date,
    ROUND(MAX(oi.total_sale)::NUMERIC, 2) AS last_sale_amount
FROM orders AS o
JOIN inactive_sellers AS ins ON ins.seller_id = o.seller_id
JOIN sellers AS s ON s.seller_id = o.seller_id
JOIN order_items AS oi ON o.order_id = oi.order_id
GROUP BY o.seller_id, s.seller_name
ORDER BY last_sale_date DESC;

-- =====================================================
-- QUERY 16: CUSTOMER SEGMENTATION
-- =====================================================
-- Purpose: Categorize customers as "Returning" (5+ returns) or "New"
-- Metrics: Customer name, total orders, total returns, category
-- Use Case: Marketing segmentation, loyalty programs
-- =====================================================
SELECT 
    c_full_name AS customer_name,
    total_orders,
    total_returns,
    CASE
        WHEN total_returns > 5 THEN 'Returning_Customer'
        ELSE 'New_Customer'
    END AS customer_category
FROM
(
    SELECT 
        CONCAT(c.first_name, ' ', c.last_name) AS c_full_name,
        COUNT(o.order_id) AS total_orders,
        SUM(CASE WHEN o.order_status = 'Returned' THEN 1 ELSE 0 END) AS total_returns
    FROM orders AS o
    JOIN customers AS c ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
) AS customer_analysis
ORDER BY total_returns DESC;

-- =====================================================
-- QUERY 17: CROSS-SELL OPPORTUNITIES
-- =====================================================
-- Purpose: Identify customers who bought product A but not product B
-- Metrics: Customer ID, name, products bought, cross-sell suggestions
-- Use Case: Personalized marketing, product recommendations
-- =====================================================
-- Note: This is a template query. Customize with specific products.
-- Example: Find customers who bought phones but not phone cases
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    COUNT(DISTINCT p.product_id) AS products_purchased,
    COUNT(DISTINCT p.category_id) AS categories_purchased
FROM orders AS o
JOIN customers AS c ON c.customer_id = o.customer_id
JOIN order_items AS oi ON o.order_id = oi.order_id
JOIN products AS p ON oi.product_id = p.product_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING COUNT(DISTINCT p.category_id) > 1
ORDER BY products_purchased DESC;

-- =====================================================
-- QUERY 18: TOP 5 CUSTOMERS BY ORDERS PER STATE
-- =====================================================
-- Purpose: Rank top customers in each state by order count
-- Metrics: State, customer name, order count, total sales
-- Use Case: Regional analysis, customer recognition
-- =====================================================
SELECT 
    state,
    customer_name,
    total_orders,
    ROUND(total_sales::NUMERIC, 2) AS total_sales,
    rank
FROM 
(
    SELECT 
        c.state,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        COUNT(o.order_id) AS total_orders,
        SUM(oi.total_sale) AS total_sales,
        DENSE_RANK() OVER(PARTITION BY c.state ORDER BY COUNT(o.order_id) DESC) AS rank
    FROM orders AS o
    JOIN order_items AS oi ON oi.order_id = o.order_id
    JOIN customers AS c ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.state, c.first_name, c.last_name
) AS ranked_customers
WHERE rank <= 5
ORDER BY state, total_orders DESC;

-- =====================================================
-- QUERY 19: REVENUE BY SHIPPING PROVIDER
-- =====================================================
-- Purpose: Analyze performance of each shipping carrier
-- Metrics: Provider, orders handled, total revenue, avg delivery time
-- Use Case: Carrier performance evaluation, logistics optimization
-- =====================================================
SELECT 
    s.shipping_providers,
    COUNT(o.order_id) AS orders_handled,
    ROUND(SUM(oi.total_sale)::NUMERIC, 2) AS total_revenue,
    ROUND(COALESCE(AVG(EXTRACT(DAY FROM (s.return_date - s.shipping_date))), 0)::NUMERIC, 2) AS avg_delivery_days
FROM orders AS o
JOIN order_items AS oi ON oi.order_id = o.order_id
JOIN shipping AS s ON s.order_id = o.order_id
GROUP BY s.shipping_providers
ORDER BY total_revenue DESC;

-- =====================================================
-- QUERY 20: TOP 10 PRODUCTS WITH HIGHEST DECLINING REVENUE (YoY)
-- =====================================================
-- Purpose: Identify products with highest revenue decline year-over-year
-- Metrics: Product ID, 2022 revenue, 2023 revenue, decline ratio %
-- Use Case: Product lifecycle analysis, discontinuation planning
-- Formula: Decline Ratio = (Current Year - Last Year) / Last Year * 100
-- =====================================================
WITH revenue_2022 AS
(
    SELECT 
        p.product_id,
        p.product_name,
        SUM(oi.total_sale) AS revenue_2022
    FROM orders AS o
    JOIN order_items AS oi ON oi.order_id = o.order_id
    JOIN products AS p ON p.product_id = oi.product_id
    WHERE EXTRACT(YEAR FROM o.order_date) = 2022
    GROUP BY p.product_id, p.product_name
),
revenue_2023 AS
(
    SELECT 
        p.product_id,
        p.product_name,
        SUM(oi.total_sale) AS revenue_2023
    FROM orders AS o
    JOIN order_items AS oi ON oi.order_id = o.order_id
    JOIN products AS p ON p.product_id = oi.product_id
    WHERE EXTRACT(YEAR FROM o.order_date) = 2023
    GROUP BY p.product_id, p.product_name
)
SELECT
    r22.product_id,
    r22.product_name,
    ROUND(r22.revenue_2022::NUMERIC, 2) AS revenue_2022,
    ROUND(r23.revenue_2023::NUMERIC, 2) AS revenue_2023,
    ROUND((r22.revenue_2022 - r23.revenue_2023)::NUMERIC, 2) AS revenue_difference,
    ROUND(((r23.revenue_2023 - r22.revenue_2022) / r22.revenue_2022 * 100)::NUMERIC, 2) AS decline_ratio_percentage
FROM revenue_2022 AS r22
JOIN revenue_2023 AS r23 ON r22.product_id = r23.product_id
WHERE r22.revenue_2022 > r23.revenue_2023
ORDER BY decline_ratio_percentage ASC
LIMIT 10;

-- =====================================================
-- END OF QUERIES
-- =====================================================
