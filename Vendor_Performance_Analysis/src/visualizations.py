"""
Visualization Module
Contains functions for creating charts and visualizations
"""

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)


class VendorVisualizer:
    """Class for creating vendor analysis visualizations"""
    
    @staticmethod
    def format_value(value):
        """Format number as K or M"""
        if value >= 1000000:
            return f"{value / 1000000:.2f}M"
        elif value >= 1000:
            return f"{value / 1000:.2f}K"
        else:
            return str(int(value))
    
    @staticmethod
    def plot_distribution(df, column, title, bins=30):
        """Plot distribution of a column"""
        plt.figure(figsize=(12, 6))
        sns.histplot(df[column], kde=True, bins=bins)
        plt.title(title, fontsize=14, fontweight='bold')
        plt.xlabel(column)
        plt.ylabel('Frequency')
        plt.tight_layout()
        return plt
    
    @staticmethod
    def plot_all_distributions(df, cols_per_row=4):
        """Plot distributions for multiple numerical columns"""
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        n_cols = len(numerical_cols)
        n_rows = (n_cols + cols_per_row - 1) // cols_per_row
        
        fig, axes = plt.subplots(n_rows, cols_per_row, figsize=(16, 3*n_rows))
        axes = axes.flatten()
        
        for i, col in enumerate(numerical_cols):
            sns.histplot(df[col], kde=True, bins=30, ax=axes[i])
            axes[i].set_title(col, fontweight='bold')
        
        for j in range(i + 1, len(axes)):
            fig.delaxes(axes[j])
        
        plt.tight_layout()
        return fig
    
    @staticmethod
    def plot_all_boxplots(df, cols_per_row=4):
        """Plot boxplots for multiple numerical columns"""
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        n_cols = len(numerical_cols)
        n_rows = (n_cols + cols_per_row - 1) // cols_per_row
        
        fig, axes = plt.subplots(n_rows, cols_per_row, figsize=(16, 3*n_rows))
        axes = axes.flatten()
        
        for i, col in enumerate(numerical_cols):
            sns.boxplot(y=df[col], ax=axes[i])
            axes[i].set_title(col, fontweight='bold')
        
        for j in range(i + 1, len(axes)):
            fig.delaxes(axes[j])
        
        plt.tight_layout()
        return fig
    
    @staticmethod
    def plot_top_vendors_and_brands(df, n=10):
        """Plot top vendors and brands by sales"""
        top_vendors = df.groupby('VendorName')['TotalSalesDollars'].sum().nlargest(n)
        top_brands = df.groupby('Description')['TotalSalesDollars'].sum().nlargest(n)
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Top Vendors
        sns.barplot(y=top_vendors.index, x=top_vendors.values, palette="Blues_r", ax=ax1)
        ax1.set_title(f"Top {n} Vendors by Sales", fontsize=14, fontweight='bold')
        ax1.set_xlabel("Total Sales ($)")
        
        for i, bar in enumerate(ax1.patches):
            ax1.text(bar.get_width() + (bar.get_width() * 0.02),
                    bar.get_y() + bar.get_height() / 2,
                    VendorVisualizer.format_value(bar.get_width()),
                    ha='left', va='center', fontsize=10)
        
        # Top Brands
        sns.barplot(y=top_brands.index, x=top_brands.values, palette="Reds_r", ax=ax2)
        ax2.set_title(f"Top {n} Brands by Sales", fontsize=14, fontweight='bold')
        ax2.set_xlabel("Total Sales ($)")
        
        for i, bar in enumerate(ax2.patches):
            ax2.text(bar.get_width() + (bar.get_width() * 0.02),
                    bar.get_y() + bar.get_height() / 2,
                    VendorVisualizer.format_value(bar.get_width()),
                    ha='left', va='center', fontsize=10)
        
        plt.tight_layout()
        return fig
    
    @staticmethod
    def plot_pareto_chart(df, top_n=10):
        """Plot Pareto chart for vendor contributions"""
        vendor_contrib = df.groupby('VendorName')['TotalPurchaseDollars'].sum().reset_index()
        vendor_contrib = vendor_contrib.sort_values('TotalPurchaseDollars', ascending=False)
        
        top_vendors = vendor_contrib.head(top_n)
        top_vendors['Contribution%'] = (top_vendors['TotalPurchaseDollars'] / 
                                        vendor_contrib['TotalPurchaseDollars'].sum() * 100)
        top_vendors['Cumulative%'] = top_vendors['Contribution%'].cumsum()
        
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        # Bar chart
        sns.barplot(x=top_vendors['VendorName'], y=top_vendors['Contribution%'], 
                   palette="mako", ax=ax1)
        
        for i, value in enumerate(top_vendors['Contribution%']):
            ax1.text(i, value - 1, f'{value:.1f}%', ha='center', fontsize=10, color="white")
        
        # Line chart on secondary axis
        ax2 = ax1.twinx()
        ax2.plot(range(len(top_vendors)), top_vendors['Cumulative%'].values, 
                color='red', marker='o', linestyle='dashed', linewidth=2, markersize=8)
        
        ax1.set_xticklabels(top_vendors['VendorName'], rotation=45, ha='right')
        ax1.set_ylabel('Purchase Contribution %', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Cumulative Contribution %', fontsize=12, fontweight='bold', color='red')
        ax1.set_xlabel('Vendors', fontsize=12, fontweight='bold')
        ax1.set_title(f'Pareto Chart: Top {top_n} Vendor Contribution', 
                     fontsize=14, fontweight='bold')
        
        ax2.axhline(y=100, color='gray', linestyle='dashed', alpha=0.7)
        
        plt.tight_layout()
        return fig
    
    @staticmethod
    def plot_donut_chart(df, top_n=10):
        """Plot donut chart for vendor market share"""
        vendor_sales = df.groupby('VendorName')['TotalSalesDollars'].sum().nlargest(top_n)
        other_sales = df.groupby('VendorName')['TotalSalesDollars'].sum()[top_n:].sum()
        
        labels = list(vendor_sales.index) + ['Others']
        sizes = list(vendor_sales.values) + [other_sales]
        
        fig, ax = plt.subplots(figsize=(10, 8))
        
        wedges, texts, autotexts = ax.pie(sizes, labels=labels, autopct='%1.1f%%',
                                          startangle=90, colors=plt.cm.Paired.colors)
        
        # Draw circle for donut
        centre_circle = plt.Circle((0, 0), 0.70, fc='white')
        fig.gca().add_artist(centre_circle)
        
        ax.set_title(f'Top {top_n} Vendors Sales Share', fontsize=14, fontweight='bold')
        
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        plt.tight_layout()
        return fig
    
    @staticmethod
    def plot_correlation_heatmap(df):
        """Plot correlation heatmap for numerical columns"""
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        
        fig, ax = plt.subplots(figsize=(14, 10))
        correlation_matrix = df[numerical_cols].corr()
        
        sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap="coolwarm",
                   linewidths=0.5, ax=ax, cbar_kws={'label': 'Correlation'})
        
        ax.set_title('Correlation Heatmap of Numerical Variables', 
                    fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        return fig
    
    @staticmethod
    def plot_sales_vs_profit_margin(df, highlight_brands=None):
        """Plot sales vs profit margin scatter plot"""
        fig, ax = plt.subplots(figsize=(12, 8))
        
        brand_perf = df.groupby('Description').agg({
            'TotalSalesDollars': 'sum',
            'ProfitMargin': 'mean'
        }).reset_index()
        
        # Plot all brands
        sns.scatterplot(data=brand_perf, x='TotalSalesDollars', y='ProfitMargin',
                       color='lightblue', s=100, alpha=0.6, ax=ax, label='All Brands')
        
        # Highlight specific brands if provided
        if highlight_brands is not None:
            highlighted = brand_perf[brand_perf['Description'].isin(highlight_brands)]
            sns.scatterplot(data=highlighted, x='TotalSalesDollars', y='ProfitMargin',
                           color='red', s=150, alpha=0.8, ax=ax, label='Target Brands')
        
        ax.set_xlabel('Total Sales ($)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Profit Margin (%)', fontsize=12, fontweight='bold')
        ax.set_title('Sales vs Profit Margin Analysis', fontsize=14, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        return fig
    
    @staticmethod
    def plot_bulk_discount_analysis(df):
        """Plot impact of bulk purchasing on unit price"""
        df_copy = df.copy()
        df_copy['OrderSize'] = pd.qcut(df_copy['TotalPurchaseQuantity'], q=3,
                                       labels=['Small', 'Medium', 'Large'], duplicates='drop')
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        sns.boxplot(data=df_copy, x="OrderSize", y="UnitPurchasePrice", palette="Set2", ax=ax)
        
        ax.set_title("Impact of Bulk Purchasing on Unit Price", fontsize=14, fontweight='bold')
        ax.set_xlabel("Order Size", fontsize=12, fontweight='bold')
        ax.set_ylabel("Average Unit Purchase Price ($)", fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        return fig
    
    @staticmethod
    def plot_confidence_intervals(top_vendors, low_vendors):
        """Plot confidence intervals for vendor groups"""
        from scipy import stats
        
        def calc_ci(data):
            mean = np.mean(data)
            std_err = np.std(data, ddof=1) / np.sqrt(len(data))
            t_crit = stats.t.ppf(0.975, len(data) - 1)
            return mean, mean - t_crit * std_err, mean + t_crit * std_err
        
        top_mean, top_lower, top_upper = calc_ci(top_vendors)
        low_mean, low_lower, low_upper = calc_ci(low_vendors)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        sns.histplot(top_vendors, kde=True, color="blue", bins=20, alpha=0.5, label="Top Vendors", ax=ax)
        sns.histplot(low_vendors, kde=True, color="red", bins=20, alpha=0.5, label="Low Vendors", ax=ax)
        
        ax.axvline(top_mean, color="blue", linestyle="-", linewidth=2, label=f"Top Mean: {top_mean:.2f}")
        ax.axvline(top_lower, color="blue", linestyle="--", linewidth=1.5)
        ax.axvline(top_upper, color="blue", linestyle="--", linewidth=1.5)
        
        ax.axvline(low_mean, color="red", linestyle="-", linewidth=2, label=f"Low Mean: {low_mean:.2f}")
        ax.axvline(low_lower, color="red", linestyle="--", linewidth=1.5)
        ax.axvline(low_upper, color="red", linestyle="--", linewidth=1.5)
        
        ax.set_title("95% Confidence Interval: Top vs Low Vendors (Profit Margin)", 
                    fontsize=14, fontweight='bold')
        ax.set_xlabel("Profit Margin (%)", fontsize=12, fontweight='bold')
        ax.set_ylabel("Frequency", fontsize=12, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        return fig


if __name__ == "__main__":
    # Example usage
    from data_loader import get_db_connection, DataLoader
    
    db = get_db_connection()
    loader = DataLoader(db)
    vendor_data = loader.load_vendor_sales_summary()
    
    if vendor_data is not None:
        visualizer = VendorVisualizer()
        
        # Create visualizations
        fig1 = visualizer.plot_top_vendors_and_brands(vendor_data, n=10)
        fig2 = visualizer.plot_pareto_chart(vendor_data, top_n=10)
        fig3 = visualizer.plot_correlation_heatmap(vendor_data)
        
        plt.show()
    
    db.disconnect()
