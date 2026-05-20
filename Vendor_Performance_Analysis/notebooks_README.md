# Notebooks

This directory contains Jupyter notebooks for the vendor performance analysis, organized by analysis phase.

## Notebook Organization

### 01_vendor_analysis.ipynb (Main Analysis)
**Purpose**: Comprehensive vendor and brand performance analysis

**Covers**:
- Database exploration and data loading
- Data cleaning and preparation
- Vendor performance metrics calculation
- Brand performance analysis
- Inventory analysis
- Statistical testing and hypothesis validation
- Key findings and visualizations

**Time to Run**: ~5-10 minutes (depending on data size)

**Key Outputs**:
- vendor_sales_summary table (database)
- Performance metrics (Profit Margin, Stock Turnover, etc.)
- Visualizations (histograms, boxplots, scatter plots)
- Statistical test results

## How to Run

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt

# Ensure database exists
data/raw/inventory.db
```

### Run the Notebook

#### Option 1: Jupyter Notebook (Interactive)
```bash
jupyter notebook notebooks/01_vendor_analysis.ipynb
```

#### Option 2: Jupyter Lab (Recommended)
```bash
jupyter lab notebooks/01_vendor_analysis.ipynb
```

#### Option 3: Convert to Python Script
```bash
jupyter nbconvert --to script notebooks/01_vendor_analysis.ipynb
python notebooks/01_vendor_analysis.py
```

## Notebook Structure

### Section 1: Setup & Data Loading
- Import libraries
- Initialize database connection
- Load raw data tables

### Section 2: Exploratory Data Analysis
- Table structure inspection
- Data quality assessment
- Null value analysis
- Distribution analysis

### Section 3: Data Preparation
- Join multiple tables
- Aggregate sales and purchase data
- Create vendor_sales_summary table
- Handle missing values

### Section 4: Metrics Calculation
- Gross Profit
- Profit Margin %
- Stock Turnover Ratio
- Sales to Purchase Ratio
- Unit Prices
- Unsold Inventory Value

### Section 5: Exploratory Analysis
- Summary statistics
- Histograms and distributions
- Box plots for outlier detection
- Correlation analysis

### Section 6: Vendor Analysis
- Top 10 vendors by sales
- Top 10 brands by sales
- Pareto analysis (80/20 rule)
- Vendor contribution %

### Section 7: Brand Performance
- Sales vs Profit Margin scatter plot
- Target brands identification
- Category analysis

### Section 8: Advanced Analysis
- Bulk purchasing impact on unit price
- Low stock turnover vendors
- Unsold inventory analysis
- Confidence intervals

### Section 9: Statistical Testing
- Two-sample t-test
- Hypothesis testing
- P-value interpretation

## Key Findings Template

After running the notebook, capture key findings:

**Top Findings:**
1. Top performing vendor: [Vendor Name] - $X revenue
2. Best profit margin brand: [Brand Name] - X% margin
3. Inventory issue: $X unsold inventory value
4. Statistical significance: [P-value result]

## Database Tables Used

| Table | Rows | Purpose |
|-------|------|---------|
| purchases | N | Purchase orders with quantities and prices |
| sales | N | Sales transactions with revenue |
| vendor_invoice | N | Invoice and freight data |
| purchase_prices | N | Product pricing information |
| vendor_sales_summary | N | Aggregated metrics (created in notebook) |

## Troubleshooting

### Issue: Database not found
```
Solution: Ensure inventory.db is in data/raw/ directory
```

### Issue: Missing columns
```
Solution: Check database schema using:
sqlite3 inventory.db ".schema"
```

### Issue: Out of memory
```
Solution: Reduce data range or filter specific vendors:
df = df[df['VendorNumber'].isin([list_of_vendor_numbers])]
```

### Issue: Slow queries
```
Solution: Create database indexes:
CREATE INDEX idx_vendor ON sales(VendorNo);
CREATE INDEX idx_date ON sales(SaleDate);
```

## Advanced Usage

### Run Specific Sections
Use cell tags to run only sections of interest:
- Navigate to View → Tags (in Jupyter)
- Re-run only analysis sections

### Modify Parameters
Change analysis parameters at top of notebook:
```python
# Filtering
TOP_N_VENDORS = 10
TOP_N_BRANDS = 10
MIN_SALES_THRESHOLD = 1000

# Statistical
CONFIDENCE_LEVEL = 0.95
P_VALUE_THRESHOLD = 0.05
```

### Export Results
```python
# Export to CSV
analyzer.df.to_csv('results/analysis_results.csv', index=False)

# Export to Excel
analyzer.df.to_excel('results/analysis_results.xlsx', index=False)

# Export charts
plt.savefig('results/chart_name.png', dpi=300)
```

## Performance Tips

1. **Filter data early**: Reduce rows before groupby operations
2. **Use indexes**: Database queries faster with indexes
3. **Cache results**: Save intermediate dataframes to CSV
4. **Batch processes**: Process vendors in groups if dataset is large

## Further Analysis Ideas

1. **Time Series Analysis**: Analyze trends over months/quarters
2. **Seasonality**: Identify seasonal patterns in sales
3. **Forecasting**: Predict future vendor performance
4. **Clustering**: Group similar vendors
5. **Anomaly Detection**: Find unusual vendor behavior

## Resources

- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [SQLite Tutorial](https://www.sqlite.org/cli.html)
- [Matplotlib Basics](https://matplotlib.org/stable/tutorials/index)
- [Seaborn Gallery](https://seaborn.pydata.org/examples.html)
- [Jupyter Notebook Tips](https://jupyter-notebook.readthedocs.io/)

## Contact & Support

For questions about notebooks:
- Check analysis comments in notebook cells
- Review methodology documentation
- Check logs/ directory for error messages

---

**Last Updated**: [Date]
**Notebook Version**: 1.0
**Data Source**: Blinkit Inventory Database
