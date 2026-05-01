# E-Commerce Database Setup Guide

Complete step-by-step instructions for setting up and using the e-commerce database project.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Database Setup](#database-setup)
4. [Running Queries](#running-queries)
5. [Project Structure](#project-structure)
6. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Software
- **PostgreSQL 12+** - [Download here](https://www.postgresql.org/download/)
- **Git** - [Download here](https://git-scm.com/downloads)
- **Text Editor or IDE** (VS Code, DataGrip, pgAdmin, etc.)

### Optional Tools
- **pgAdmin** - PostgreSQL GUI management tool
- **DBeaver** - Database visualization and management
- **VS Code Extension** - PostgreSQL extension for VS Code

### System Requirements
- Windows, macOS, or Linux
- At least 2GB RAM
- 500MB free disk space

---

## Installation

### Step 1: Install PostgreSQL

#### On Windows
1. Download PostgreSQL from https://www.postgresql.org/download/windows/
2. Run the installer
3. Follow the installation wizard
4. Note the port (default: 5432) and admin password
5. Select pgAdmin to install along with PostgreSQL

#### On macOS
```bash
# Using Homebrew
brew install postgresql
brew services start postgresql
```

#### On Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
```

### Step 2: Install Git
1. Download from https://git-scm.com/downloads
2. Follow the installation wizard
3. Verify installation:
   ```bash
   git --version
   ```

### Step 3: Clone the Repository

```bash
# Clone the repository
git clone https://github.com/yourusername/ecommerce-database.git

# Navigate to project directory
cd ecommerce-database

# Verify files
ls -la
```

---

## Database Setup

### Method 1: Using Command Line (Recommended)

#### Step 1: Open Terminal/Command Prompt

**Windows:**
- Open Command Prompt or PowerShell
- Or open "SQL Shell (psql)" from Windows Start Menu

**macOS/Linux:**
- Open Terminal

#### Step 2: Connect to PostgreSQL

```bash
# Connect to PostgreSQL (replace 'postgres' with your username if different)
psql -U postgres
```

You'll see the PostgreSQL prompt:
```
postgres=#
```

#### Step 3: Create Database

```sql
-- Create the database
CREATE DATABASE ecommerce_db;

-- Connect to the database
\c ecommerce_db

-- Verify connection (you should see ecommerce_db=>)
```

#### Step 4: Load Schema

Exit psql first by typing:
```
\q
```

Then run the schema file:
```bash
psql -U postgres -d ecommerce_db -f schema.sql
```

You should see output confirming table creation:
```
DROP TABLE
CREATE TABLE
CREATE INDEX
...
```

#### Step 5: Verify Tables

```bash
psql -U postgres -d ecommerce_db
```

Then run:
```sql
-- List all tables
\dt

-- Should show:
-- public | category
-- public | customers
-- public | inventory
-- public | order_items
-- public | orders
-- public | payments
-- public | products
-- public | sellers
-- public | shipping
```

---

### Method 2: Using pgAdmin (GUI)

#### Step 1: Open pgAdmin
- Find pgAdmin in your applications
- Open in web browser (usually http://localhost:5050)
- Login with your credentials

#### Step 2: Create Database
1. Right-click on "Databases"
2. Click "Create → Database"
3. Name: `ecommerce_db`
4. Click "Save"

#### Step 3: Load Schema
1. Right-click on `ecommerce_db`
2. Click "Query Tool"
3. Copy the contents of `schema.sql`
4. Paste into Query Tool
5. Click "Execute"

---

### Loading Sample Data

If you have a `data.sql` file:

```bash
psql -U postgres -d ecommerce_db -f data.sql
```

---

## Running Queries

### Method 1: Command Line

```bash
# Run individual query file
psql -U postgres -d ecommerce_db -f queries.sql

# Run specific query
psql -U postgres -d ecommerce_db -c "SELECT * FROM customers LIMIT 5;"
```

### Method 2: Interactive psql

```bash
# Connect to database
psql -U postgres -d ecommerce_db

# At the ecommerce_db=> prompt, run queries:
ecommerce_db=> SELECT * FROM customers LIMIT 5;
ecommerce_db=> SELECT * FROM products ORDER BY price DESC LIMIT 10;
ecommerce_db=> \q  # Exit when done
```

### Method 3: pgAdmin Query Tool

1. Open pgAdmin
2. Navigate to Databases → ecommerce_db
3. Click "Query Tool"
4. Paste your SQL query
5. Click "Execute" or press F5

### Method 4: External IDE (VS Code, DBeaver, etc.)

Most IDEs have PostgreSQL extensions:

**VS Code:**
1. Install "PostgreSQL" extension
2. Configure connection:
   - Host: localhost
   - Port: 5432
   - User: postgres
   - Database: ecommerce_db
3. Run queries directly in the editor

---

## Common Queries to Test

After setup, test these queries to ensure everything works:

```sql
-- 1. Check if tables exist
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public';

-- 2. Count records in each table
SELECT 
    'category' as table_name, COUNT(*) as record_count FROM category
UNION ALL
SELECT 'customers', COUNT(*) FROM customers
UNION ALL
SELECT 'sellers', COUNT(*) FROM sellers
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'payments', COUNT(*) FROM payments
UNION ALL
SELECT 'shipping', COUNT(*) FROM shipping
UNION ALL
SELECT 'inventory', COUNT(*) FROM inventory;

-- 3. Sample data check
SELECT * FROM products LIMIT 5;
SELECT * FROM customers LIMIT 5;
SELECT * FROM orders LIMIT 5;
```

---

## Git Workflow (Pushing to GitHub)

### Step 1: Configure Git

```bash
# Set your name
git config --global user.name "Your Name"

# Set your email
git config --global user.email "your.email@example.com"
```

### Step 2: Create GitHub Repository

1. Go to https://github.com/new
2. Repository name: `ecommerce-database`
3. Description: "E-Commerce Database with Advanced SQL Queries"
4. Choose Public or Private
5. **DO NOT** initialize with README (you already have one)
6. Click "Create repository"

### Step 3: Push Code to GitHub

```bash
# Navigate to project folder
cd ecommerce-database

# Initialize git (if not already done)
git init

# Add all files
git add .

# Commit
git commit -m "Initial commit: E-commerce database schema and 20 business queries"

# Add remote (replace with your GitHub repo URL)
git remote add origin https://github.com/yourusername/ecommerce-database.git

# Push to GitHub
git branch -M main
git push -u origin main
```

### Step 4: Verify on GitHub

1. Go to your GitHub repository
2. Check that all files are present:
   - README.md
   - schema.sql
   - queries.sql
   - .gitignore
   - SETUP.md

---

## Project Structure

```
ecommerce-database/
├── README.md                 # Project overview and documentation
├── SETUP.md                  # This file - installation guide
├── schema.sql               # Database schema (9 tables)
├── queries.sql              # 20 business problem queries
├── .gitignore              # Git configuration
└── docs/
    ├── ER_DIAGRAM.png      # Entity relationship diagram
    └── QUERIES_GUIDE.md    # Detailed query explanations
```

---

## Troubleshooting

### Issue: "psql: command not found"

**Solution:**
- PostgreSQL not in system PATH
- **Windows:** Add PostgreSQL bin folder to PATH or use full path: `C:\Program Files\PostgreSQL\15\bin\psql.exe`
- **macOS:** Use Homebrew: `brew install postgresql`
- **Linux:** Install via package manager: `sudo apt install postgresql-client`

### Issue: "FATAL: role 'postgres' does not exist"

**Solution:**
```bash
# Check available roles
psql -U postgres

# Or create the role
createuser -s -l postgres
```

### Issue: "Database 'ecommerce_db' does not exist"

**Solution:**
```bash
# Create the database first
createdb -U postgres ecommerce_db

# Then load schema
psql -U postgres -d ecommerce_db -f schema.sql
```

### Issue: "Permission denied" error

**Solution:**
- Make sure PostgreSQL service is running
- **Windows:** Check Services (services.msc) for "postgresql-x64-15"
- **macOS:** `brew services list`
- **Linux:** `sudo systemctl status postgresql`

### Issue: "Password authentication failed"

**Solution:**
```bash
# Use -W flag to enter password interactively
psql -U postgres -W

# Or set password in connection string
psql postgresql://postgres:password@localhost:5432/ecommerce_db
```

### Issue: Port 5432 already in use

**Solution:**
```bash
# Use different port
psql -U postgres -h localhost -p 5433 -d ecommerce_db

# Or check which process is using port 5432
# Windows: netstat -ano | findstr :5432
# Linux: sudo lsof -i :5432
```

---

## Performance Tips

### Create Indexes for Better Query Performance

Indexes are already created in schema.sql, but you can add more:

```sql
-- Create index on frequently filtered columns
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_orders_order_status ON orders(order_status);
CREATE INDEX idx_customers_state ON customers(state);
```

### Analyze Query Performance

```sql
-- Show query execution plan
EXPLAIN ANALYZE
SELECT * FROM orders 
WHERE order_date >= '2023-01-01' 
ORDER BY order_date DESC;
```

---

## Next Steps

1. **Explore:** Run the 20 business queries in `queries.sql`
2. **Customize:** Modify queries for your use case
3. **Extend:** Add more tables or queries as needed
4. **Learn:** Study the queries to improve SQL skills
5. **Showcase:** Share on GitHub for portfolio

---

## Additional Resources

- [PostgreSQL Official Docs](https://www.postgresql.org/docs/)
- [SQL Tutorial](https://www.w3schools.com/sql/)
- [Database Design Best Practices](https://en.wikipedia.org/wiki/Database_design)
- [Git Tutorial](https://git-scm.com/doc)

---

**Need Help?** Create an issue on GitHub or check the troubleshooting section above.

Happy Learning! 🚀
