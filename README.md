# E-Commerce Database Project

A comprehensive **PostgreSQL** database design for an e-commerce platform, featuring a robust schema with 9 interconnected tables and 20+ advanced SQL queries solving real-world business problems.

## 📋 Project Overview

This project demonstrates:
- **Database Design**: Normalized relational database schema with proper foreign key relationships
- **SQL Expertise**: Complex queries using CTEs, window functions, aggregations, and joins
- **Business Analytics**: Real-world queries addressing key business metrics and KPIs

### Key Features
✅ 9 interconnected tables with proper relationships  
✅ 20 advanced SQL queries for business analytics  
✅ Support for order management, inventory, payments, and shipping  
✅ Customer segmentation and performance analytics  
✅ Real-time inventory and sales tracking  

---

## 🏗️ Database Schema

### Entity Relationship Diagram (ERD)

```
┌─────────────┐
│  Category   │───┐
└─────────────┘   │
                  ↓
┌─────────────────────────┐
│     Products            │◄──┐
└─────────────────────────┘   │
         ↑                     │
         │          ┌──────────┴────────────┐
         │          │                       │
    ┌────────────────────┐          ┌──────────────────┐
    │   Order_Items      │          │   Inventory      │
    └────────────────────┘          └──────────────────┘
         ↑
         │
    ┌─────────────┐
    │   Orders    │◄────────────────┬─────────┬───────┐
    └─────────────┘                 │         │       │
         ↑         ↑         ┌───────────┐ ┌────────┐ ┌──────────┐
         │         │         │ Customers │ │Sellers │ │Payments  │
         │         └────────→└───────────┘ └────────┘ └──────────┘
         │         
         └────────→┌──────────┐
                  │ Shipping │
                  └──────────┘
```

### Tables

| Table | Purpose | Key Fields |
|-------|---------|-----------|
| **category** | Product categories | category_id, category_name |
| **customers** | Customer information | customer_id, first_name, last_name, state |
| **sellers** | Seller/vendor details | seller_id, seller_name, origin |
| **products** | Product catalog | product_id, product_name, price, cogs, category_id |
| **orders** | Order transactions | order_id, order_date, customer_id, seller_id, order_status |
| **order_items** | Line items per order | order_item_id, order_id, product_id, quantity, price_per_unit |
| **payments** | Payment records | payment_id, order_id, payment_date, payment_status |
| **shipping** | Shipping tracking | shipping_id, order_id, shipping_date, return_date, delivery_status |
| **inventory** | Stock management | inventory_id, product_id, stock, warehouse_id, last_stock_date |

---

## 🚀 Getting Started

### Prerequisites
- PostgreSQL 12+ installed
- pgAdmin or any PostgreSQL client (optional)
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/ecommerce-database.git
   cd ecommerce-database
   ```

2. **Create a new database**
   ```bash
   createdb ecommerce_db
   ```

3. **Load the schema**
   ```bash
   psql -U postgres -d ecommerce_db -f schema.sql
   ```

4. **Import sample data** (if provided)
   ```bash
   psql -U postgres -d ecommerce_db -f data.sql
   ```

5. **Run queries**
   ```bash
   psql -U postgres -d ecommerce_db -f queries.sql
   ```

---

## 📊 SQL Queries Overview

The project includes **20 business problem queries**:

### 1. **Top Selling Products**
Identifies the top 10 products by total sales value with quantity sold

### 2. **Revenue by Category**
Calculates total revenue per category with percentage contribution

### 3. **Average Order Value (AOV)**
Computes AOV for customers with more than 5 orders

### 4. **Monthly Sales Trend**
Analyzes sales trends over the past 2 years with month-over-month comparison

### 5. **Customers with No Purchases**
Finds registered customers who never placed an order

### 6. **Least-Selling Categories by State**
Identifies underperforming product categories by state

### 7. **Customer Lifetime Value (CLTV)**
Ranks customers by total purchase value over lifetime

### 8. **Inventory Stock Alerts**
Queries products with low stock levels (< 10 units)

### 9. **Shipping Delays**
Identifies orders shipped more than 3 days after order date

### 10. **Payment Success Rate**
Calculates payment success percentage with status breakdown

### 11. **Top Performing Sellers**
Ranks top 5 sellers by sales with success rate metrics

### 12. **Product Profit Margin**
Calculates and ranks products by profit margin

### 13. **Most Returned Products**
Identifies top 10 returned products with return rate percentage

### 14. **Orders Pending Shipment**
Finds paid orders awaiting shipment

### 15. **Inactive Sellers**
Identifies sellers inactive for 6+ months with their last sale date

### 16. **Customer Segmentation**
Categorizes customers as "Returning" (>5 returns) or "New"

### 17. **Cross-Sell Opportunities**
Suggests products to customers based on purchase history

### 18. **Top 5 Customers by Orders per State**
Ranks top customers in each state by order count

### 19. **Revenue by Shipping Provider**
Analyzes performance of each shipping provider

### 20. **Declining Revenue Products (YoY)**
Identifies top 10 products with highest revenue decline year-over-year

---

## 🛠️ Technologies Used

- **Database**: PostgreSQL
- **Language**: SQL
- **Advanced Concepts**: 
  - CTEs (Common Table Expressions)
  - Window Functions (RANK, DENSE_RANK, LAG)
  - Aggregate Functions
  - Complex JOINs
  - Subqueries

---

## 📁 Project Structure

```
ecommerce-database/
├── README.md                 # Project documentation
├── schema.sql               # Database schema and table creation
├── queries.sql              # 20 business problem queries
├── data.sql                 # Sample data (optional)
├── .gitignore              # Git ignore rules
└── docs/
    ├── ER_DIAGRAM.png      # Visual ER diagram
    ├── QUERIES_GUIDE.md    # Detailed query explanations
    └── SETUP.md            # Detailed setup instructions
```

---

## 💡 Key Insights from Queries

### Sales Analytics
- Identify bestselling products and revenue drivers
- Track sales trends and seasonal patterns
- Analyze average order value and customer segmentation

### Inventory Management
- Monitor stock levels and reorder requirements
- Track inventory by warehouse
- Identify slow-moving products

### Customer Analysis
- Calculate customer lifetime value
- Identify returning vs. new customers
- Segment customers by state and purchase behavior

### Operations
- Track payment success rates
- Monitor shipping performance and delays
- Identify inactive sellers
- Calculate profit margins

---

## 📈 Performance Metrics

The database tracks:
- **Revenue**: Total sales, by category, by seller, by shipping provider
- **Customers**: Lifetime value, order count, purchase behavior
- **Products**: Sales count, profit margin, return rate
- **Operations**: Payment success, shipping delays, inventory levels

---

## 🔐 Best Practices Implemented

✅ **Normalization**: Database follows 3NF (Third Normal Form)  
✅ **Foreign Keys**: Proper referential integrity  
✅ **Indexing**: Primary keys defined on all tables  
✅ **Data Types**: Appropriate data types for each column  
✅ **Constraints**: NOT NULL and UNIQUE constraints where necessary  

---

## 🤝 How to Use This Project

1. **Study**: Use this as a learning resource for database design and SQL
2. **Extend**: Add more tables (e.g., reviews, wishlists, returns management)
3. **Customize**: Modify queries for your specific business needs
4. **Interview Prep**: Reference for SQL and database design interviews
5. **Portfolio**: Showcase your database and SQL skills

---

## 📝 Future Enhancements

- [ ] Add triggers for automatic inventory updates
- [ ] Create stored procedures for complex operations
- [ ] Implement views for common queries
- [ ] Add data validation constraints
- [ ] Create performance indexes
- [ ] Add more complex analytics queries
- [ ] Implement data backup strategies

---

## 📚 Learning Resources

- [PostgreSQL Official Documentation](https://www.postgresql.org/docs/)
- [SQL Window Functions](https://www.postgresql.org/docs/current/functions-window.html)
- [Common Table Expressions (CTEs)](https://www.postgresql.org/docs/current/queries-with.html)
- [Database Normalization](https://en.wikipedia.org/wiki/Database_normalization)

---

## 📞 Support

For questions or improvements, feel free to:
- Open an issue on GitHub
- Submit a pull request
- Contact the project maintainer

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 🙏 Acknowledgments

- Database design inspired by real-world e-commerce platforms
- Queries based on common business analytics challenges

---

**Happy Learning! 🚀**

*Last Updated: 2025*
