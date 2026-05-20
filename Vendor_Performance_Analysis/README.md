# Vendor Performance Analysis 📊

A comprehensive data analysis project exploring vendor performance metrics, purchase patterns, and sales trends using SQL and Python.

## 📌 Project Overview

This project analyzes vendor performance across multiple dimensions including:
- **Purchase Behavior**: Vendor order frequency, order sizes, and payment patterns
- **Sales Performance**: Revenue generation, product mix, and customer satisfaction
- **Cost Analysis**: Purchase prices, margins, and cost-effectiveness
- **Trend Analysis**: Seasonal patterns and performance over time

## 🎯 Key Findings

> *Add your main insights here after analysis*

- Key metric 1: Description
- Key metric 2: Description
- Key metric 3: Description

## 🛠️ Technologies & Libraries

- **Python 3.8+**
- **Pandas**: Data manipulation & analysis
- **SQLite**: Database querying
- **SQLAlchemy**: SQL toolkit
- **Matplotlib & Seaborn**: Data visualization
- **Jupyter**: Interactive analysis notebooks

## 📂 Project Structure

```
vendor-performance-analysis/
├── notebooks/                 # Jupyter notebooks with analysis
├── data/                      # Raw and processed datasets
├── src/                       # Reusable Python modules
├── results/                   # Output files and visualizations
└── docs/                      # Documentation and guides
```

## 🚀 Quick Start

### Prerequisites
```bash
python 3.8+
pip
```

### Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/vendor-performance-analysis.git
   cd vendor-performance-analysis
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the analysis**
   ```bash
   jupyter notebook notebooks/
   ```

## 📓 Notebooks Overview

| Notebook | Description |
|----------|-------------|
| `01_data_exploration.ipynb` | Initial data exploration and understanding |
| `02_vendor_analysis.ipynb` | Detailed vendor performance analysis |
| `03_visualization.ipynb` | Key findings and visualizations |

## 📊 Dataset Information

**Data Source**: SQLite Database (`inventory.db`)

**Tables Included**:
- `purchases`: Purchase order records
- `sales`: Sales transactions
- `vendor_invoice`: Invoice details
- `purchase_prices`: Pricing information

*For detailed data dictionary, see [docs/data_dictionary.md](docs/data_dictionary.md)*

## 🔍 Analysis Methodology

This analysis follows a structured approach:

1. **Data Exploration**: Understanding data structure and quality
2. **Data Cleaning**: Handling missing values and outliers
3. **Feature Engineering**: Creating meaningful metrics
4. **Analysis**: Statistical analysis and pattern identification
5. **Visualization**: Creating insightful charts and dashboards

*See [docs/methodology.md](docs/methodology.md) for detailed methodology*

## 📈 Key Metrics Analyzed

- Average order value by vendor
- Order frequency patterns
- Cost-to-sales ratio
- Vendor reliability metrics
- Seasonal trends

## 💡 How to Use This Project

### For Learning:
- Read notebooks sequentially to understand the analysis flow
- Check comments and markdown cells for explanations
- Review code structure for best practices

### For Reference:
- Use utility functions in `src/` for similar analyses
- Adapt SQL queries for different datasets
- Reference visualization templates

### For Reproducibility:
- All steps are documented in notebooks
- Data pipeline is fully reproducible
- Results can be regenerated with `requirements.txt`

## 🤝 Contributing

Suggestions and improvements welcome! Feel free to:
- Report issues
- Suggest optimizations
- Propose new analyses

## 📝 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## 👤 Author

**Your Name**
- LinkedIn: [Your LinkedIn Profile]
- GitHub: [@yourusername](https://github.com/yourusername)
- Email: your.email@example.com

## 🔗 Related Resources

- [Pandas Documentation](https://pandas.pydata.org/)
- [SQLite Tutorial](https://www.sqlite.org/cli.html)
- [Data Analysis Best Practices](https://github.com/drivendata/cookiecutter-data-science)

---

**Last Updated**: [Date]
**Status**: ✅ Complete | 📊 Analysis Ready
