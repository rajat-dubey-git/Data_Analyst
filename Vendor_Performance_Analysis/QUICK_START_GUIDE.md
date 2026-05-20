# Quick Start Guide - Vendor Performance Analysis

## 📦 Project Setup (5 minutes)

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Organize Your Files
Make sure your project structure looks like this:
```
vendor-performance-analysis/
├── data/
│   └── raw/
│       └── inventory.db           # Your SQLite database
├── notebooks/
│   └── 01_vendor_analysis.ipynb   # Your notebook
├── src/
│   ├── __init__.py
│   ├── data_loader.py
│   ├── vendor_analyzer.py
│   └── visualizations.py
├── results/
│   └── (outputs will go here)
├── logs/
│   └── (logs will be created here)
├── main_analysis.py
├── requirements.txt
├── README.md
└── .gitignore
```

### 3. Verify Database Connection
```bash
python -c "from src import get_db_connection; db = get_db_connection(); print('✓ Database connected')"
```

---

## 🚀 Run Analysis

### Option 1: Run Full Analysis Script (Recommended)
```bash
python main_analysis.py
```

**Output**:
- Console summary with key metrics
- CSV files in `results/` directory
- PNG charts in `results/figures/` directory
- Logs in `logs/` directory

### Option 2: Run Interactive Notebook
```bash
jupyter notebook notebooks/01_vendor_analysis.ipynb
```

### Option 3: Use Modules Programmatically
```python
from src import get_db_connection, DataLoader, VendorAnalyzer

# Connect and load data
db = get_db_connection()
loader = DataLoader(db)
vendor_data = loader.load_vendor_sales_summary()

# Analyze
analyzer = VendorAnalyzer(vendor_data)
analyzer.calculate_metrics()

# Get results
print(analyzer.get_summary_statistics())
print(analyzer.get_top_vendors_by_sales(10))

db.disconnect()
```

---

## 📊 What Gets Generated

### 1. Console Output
- Summary statistics
- Top vendors and brands
- Statistical test results

### 2. CSV Files (results/)
```
vendor_performance_analysis.csv     # All vendor metrics
vendor_performance_summary.csv      # Aggregated vendor summary
```

### 3. Charts (results/figures/)
```
top_vendors_brands.png              # Top 10 vendors and brands
pareto_chart.png                    # 80/20 analysis
correlation_heatmap.png             # Variable correlations
bulk_discount_analysis.png          # Bulk purchasing impact
```

### 4. Logs (logs/)
```
data_loading.log                    # Data operations log
ingestion_db.log                    # Database ingestion log
```

---

## 🔍 Key Metrics Explained

| Metric | Formula | What It Means |
|--------|---------|---------------|
| **Gross Profit** | Sales - Purchases | Revenue after cost |
| **Profit Margin %** | (Gross Profit / Sales) × 100 | Profitability percentage |
| **Stock Turnover** | Sales Quantity / Purchase Quantity | How many times inventory sells |
| **Sales/Purchase Ratio** | Total Sales / Total Purchases | Revenue multiplier on purchases |
| **Unit Price** | Total Dollars / Total Quantity | Average price per unit |

---

## 📈 Analysis Questions Answered

### 1. **Top Performers**
✓ Which vendors generate the most revenue?
✓ Which brands have the best sales?
✓ What is the vendor contribution (Pareto analysis)?

### 2. **Profitability**
✓ What are profit margins by vendor?
✓ Which brands are high-margin but low-sales?
✓ Where is profit concentrated?

### 3. **Inventory**
✓ How much inventory is unsold?
✓ Which vendors have poor stock turnover?
✓ What is the capital locked in unsold inventory?

### 4. **Purchasing Patterns**
✓ Does bulk purchasing lead to lower unit prices?
✓ How does order size affect pricing?
✓ What is the optimal purchase quantity?

### 5. **Statistical Insights**
✓ Is there a significant difference between vendor groups?
✓ What is the confidence interval for metrics?
✓ Which insights are statistically valid?

---

## 🎯 Common Use Cases

### Use Case 1: Identify Cost Reduction Opportunities
```python
# Find vendors with low stock turnover (high inventory)
low_turnover = analyzer.identify_low_stock_turnover_vendors()
high_inventory = analyzer.identify_high_unsold_inventory(10)
```

### Use Case 2: Optimize Product Mix
```python
# Find low-sales, high-margin brands to promote
target_brands, _, _ = analyzer.identify_low_sales_high_margin_brands()
```

### Use Case 3: Negotiate Better Pricing
```python
# Analyze bulk discount impact
bulk_analysis = analyzer.get_bulk_discount_analysis()
# Compare unit prices across order sizes
```

### Use Case 4: Vendor Performance Ranking
```python
# Get vendor contribution to total purchases (Pareto)
vendor_perf = analyzer.get_vendor_performance_summary()
```

---

## 🛠️ Customization

### Modify Analysis Parameters
Edit `main_analysis.py` or notebook:

```python
# Top N vendors/brands
TOP_N = 15  # Default is 10

# Statistical confidence level
CONFIDENCE = 0.99  # Default is 0.95

# Filter data
VENDOR_FILTER = [1128, 1129]  # Analyze specific vendors only
DATE_RANGE = ('2023-01-01', '2023-12-31')  # Specific period
```

### Custom Queries
Use `DataLoader` for custom SQL queries:

```python
from src import get_db_connection

db = get_db_connection()

# Run custom query
query = """
SELECT VendorNumber, SUM(TotalSalesDollars) as Sales
FROM vendor_sales_summary
GROUP BY VendorNumber
HAVING Sales > 100000
ORDER BY Sales DESC
"""

result = db.query_to_dataframe(query)
print(result)
```

---

## 📊 Visualization Examples

### Create Custom Charts
```python
from src import VendorVisualizer

viz = VendorVisualizer()

# Distribution plot
fig = viz.plot_distribution(analyzer.df, 'ProfitMargin', 'Profit Margin Distribution')

# Pareto chart
fig = viz.plot_pareto_chart(analyzer.df, top_n=15)

# Correlation heatmap
fig = viz.plot_correlation_heatmap(analyzer.df)

# Save figure
fig.savefig('results/custom_chart.png', dpi=300)
```

---

## ⚠️ Troubleshooting

### Problem: "inventory.db not found"
```bash
# Verify database exists
ls -la data/raw/inventory.db

# If missing, copy from your data source
cp /path/to/inventory.db data/raw/
```

### Problem: "ModuleNotFoundError: No module named 'src'"
```bash
# Add src to Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Or run from project root
cd /path/to/vendor-performance-analysis
python main_analysis.py
```

### Problem: "Table vendor_sales_summary does not exist"
```python
# Run notebook first to create the table
jupyter notebook notebooks/01_vendor_analysis.ipynb
# Or execute the table creation cell
```

### Problem: "Out of memory"
```python
# Filter large dataset
df = analyzer.df[analyzer.df['TotalSalesDollars'] > 1000]

# Or process in chunks
for vendor in analyzer.df['VendorNumber'].unique()[:10]:
    vendor_df = analyzer.df[analyzer.df['VendorNumber'] == vendor]
    # Process vendor_df
```

---

## 📱 For GitHub Portfolio

This project is ready for GitHub! To upload:

```bash
# Initialize git (if not already)
git init

# Add all files
git add .

# Commit
git commit -m "Initial commit: Vendor performance analysis project"

# Add remote
git remote add origin https://github.com/YOUR_USERNAME/vendor-performance-analysis.git

# Push
git push -u origin main
```

---

## 📚 Next Steps

1. **Run Analysis**: `python main_analysis.py`
2. **Review Results**: Check `results/` and `logs/` directories
3. **Customize**: Modify `main_analysis.py` for your needs
4. **Visualize**: Create custom charts with `VendorVisualizer`
5. **Deploy**: Push to GitHub or integrate with dashboards

---

## 🎓 Learning Resources

- **Data Analysis**: `notebooks/01_vendor_analysis.ipynb`
- **Code Modules**: `src/data_loader.py`, `src/vendor_analyzer.py`
- **Documentation**: `README.md`, `docs/` directory
- **Examples**: Comments in Python files

---

## ✅ Checklist Before Sharing

- [ ] Database file is in `data/raw/`
- [ ] All dependencies installed: `pip install -r requirements.txt`
- [ ] Run analysis successfully: `python main_analysis.py`
- [ ] Results generated in `results/`
- [ ] No API keys or credentials in code
- [ ] README is clear and complete
- [ ] Git repository initialized and committed
- [ ] .gitignore properly configured

---

## 📞 Support

For issues or questions:
1. Check `logs/` directory for error details
2. Review notebook comments
3. Check data types: `analyzer.df.dtypes`
4. Verify database connection: `db.get_table_list()`

---

**Version**: 1.0
**Status**: ✅ Ready for use
**Last Updated**: [Current Date]
