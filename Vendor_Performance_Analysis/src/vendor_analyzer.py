"""
Vendor Analysis Module
Contains functions for calculating vendor performance metrics
"""

import pandas as pd
import numpy as np
from scipy import stats
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VendorAnalyzer:
    """Class for vendor performance analysis"""
    
    def __init__(self, df):
        """Initialize with vendor sales summary dataframe"""
        self.df = df.copy()
        self.logger = logger
    
    def calculate_metrics(self):
        """Calculate all key vendor performance metrics"""
        self.df['GrossProfit'] = self.df['TotalSalesDollars'] - self.df['TotalPurchaseDollars']
        self.df['ProfitMargin'] = (self.df['GrossProfit'] / self.df['TotalSalesDollars'] * 100).round(2)
        self.df['StockTurnover'] = (self.df['TotalSalesQuantity'] / self.df['TotalPurchaseQuantity']).round(2)
        self.df['SalesToPurchaseRatio'] = (self.df['TotalSalesDollars'] / self.df['TotalPurchaseDollars']).round(2)
        self.df['UnitPurchasePrice'] = (self.df['TotalPurchaseDollars'] / self.df['TotalPurchaseQuantity']).round(2)
        self.df['UnitSalesPrice'] = (self.df['TotalSalesDollars'] / self.df['TotalSalesQuantity']).round(2)
        self.df['UnsoldInventoryValue'] = ((self.df['TotalPurchaseQuantity'] - 
                                           self.df['TotalSalesQuantity']) * self.df['PurchasePrice']).round(2)
        
        self.logger.info("All metrics calculated")
        return self.df
    
    def get_top_vendors_by_sales(self, n=10):
        """Get top N vendors by sales dollars"""
        return self.df.groupby('VendorName')['TotalSalesDollars'].sum().nlargest(n)
    
    def get_top_brands_by_sales(self, n=10):
        """Get top N brands by sales dollars"""
        return self.df.groupby('Description')['TotalSalesDollars'].sum().nlargest(n)
    
    def get_vendor_performance_summary(self):
        """Get vendor performance summary with contribution percentage"""
        vendor_perf = self.df.groupby('VendorName').agg({
            'TotalPurchaseDollars': 'sum',
            'GrossProfit': 'sum',
            'TotalSalesDollars': 'sum'
        }).reset_index()
        
        vendor_perf['PurchaseContribution%'] = (
            vendor_perf['TotalPurchaseDollars'] / vendor_perf['TotalPurchaseDollars'].sum() * 100
        ).round(2)
        
        vendor_perf = vendor_perf.sort_values('PurchaseContribution%', ascending=False)
        return vendor_perf
    
    def get_brand_performance_summary(self):
        """Get brand performance summary"""
        brand_perf = self.df.groupby('Description').agg({
            'TotalSalesDollars': 'sum',
            'ProfitMargin': 'mean',
            'StockTurnover': 'mean'
        }).reset_index()
        
        return brand_perf.sort_values('TotalSalesDollars', ascending=False)
    
    def identify_low_sales_high_margin_brands(self):
        """Identify brands with low sales but high profit margins"""
        brand_perf = self.get_brand_performance_summary()
        
        low_sales_threshold = brand_perf['TotalSalesDollars'].quantile(0.15)
        high_margin_threshold = brand_perf['ProfitMargin'].quantile(0.85)
        
        target_brands = brand_perf[
            (brand_perf['TotalSalesDollars'] <= low_sales_threshold) &
            (brand_perf['ProfitMargin'] >= high_margin_threshold)
        ]
        
        return target_brands, low_sales_threshold, high_margin_threshold
    
    def identify_high_unsold_inventory(self, n=10):
        """Identify vendors with highest unsold inventory value"""
        inventory_value = self.df.groupby('VendorName')['UnsoldInventoryValue'].sum().reset_index()
        inventory_value = inventory_value.sort_values('UnsoldInventoryValue', ascending=False)
        return inventory_value.head(n)
    
    def identify_low_stock_turnover_vendors(self):
        """Identify vendors with stock turnover < 1"""
        low_turnover = self.df[self.df['StockTurnover'] < 1].groupby('VendorName')['StockTurnover'].mean()
        return low_turnover.sort_values()
    
    def get_bulk_discount_analysis(self):
        """Analyze impact of bulk purchasing on unit price"""
        df_copy = self.df.copy()
        df_copy['OrderSize'] = pd.qcut(df_copy['TotalPurchaseQuantity'], q=3, 
                                       labels=['Small', 'Medium', 'Large'], duplicates='drop')
        
        bulk_analysis = df_copy.groupby('OrderSize')['UnitPurchasePrice'].agg(['mean', 'min', 'max']).round(2)
        return bulk_analysis
    
    def get_statistical_summary(self):
        """Get statistical summary of numerical columns"""
        numerical_cols = self.df.select_dtypes(include=[np.number]).columns
        return self.df[numerical_cols].describe().round(2)
    
    def compare_vendor_segments(self, segment_column='VendorName', metric='ProfitMargin', top_n=3):
        """Compare top and bottom segments"""
        threshold_top = self.df['TotalSalesDollars'].quantile(0.75)
        threshold_low = self.df['TotalSalesDollars'].quantile(0.25)
        
        top_vendors = self.df[self.df['TotalSalesDollars'] >= threshold_top][metric].dropna()
        low_vendors = self.df[self.df['TotalSalesDollars'] <= threshold_low][metric].dropna()
        
        return {
            'top_vendors': top_vendors,
            'low_vendors': low_vendors,
            'top_mean': top_vendors.mean(),
            'low_mean': low_vendors.mean()
        }
    
    def perform_statistical_test(self, segment_column='VendorName', metric='ProfitMargin'):
        """Perform two-sample t-test between top and low vendors"""
        comparison = self.compare_vendor_segments(segment_column, metric)
        
        t_stat, p_value = stats.ttest_ind(
            comparison['top_vendors'], 
            comparison['low_vendors'], 
            equal_var=False
        )
        
        return {
            't_statistic': t_stat,
            'p_value': p_value,
            'significant': p_value < 0.05,
            'top_mean': comparison['top_mean'],
            'low_mean': comparison['low_mean']
        }
    
    def calculate_confidence_interval(self, data, confidence=0.95):
        """Calculate confidence interval for a dataset"""
        mean_val = np.mean(data)
        std_err = np.std(data, ddof=1) / np.sqrt(len(data))
        t_critical = stats.t.ppf((1 + confidence) / 2, df=len(data) - 1)
        margin_of_error = t_critical * std_err
        
        return {
            'mean': mean_val,
            'lower_bound': mean_val - margin_of_error,
            'upper_bound': mean_val + margin_of_error,
            'confidence_level': confidence
        }
    
    def get_summary_statistics(self):
        """Get comprehensive summary statistics"""
        summary = {
            'total_vendors': self.df['VendorName'].nunique(),
            'total_brands': self.df['Description'].nunique(),
            'total_sales': self.df['TotalSalesDollars'].sum(),
            'total_purchases': self.df['TotalPurchaseDollars'].sum(),
            'total_gross_profit': self.df['GrossProfit'].sum(),
            'average_profit_margin': self.df['ProfitMargin'].mean(),
            'average_stock_turnover': self.df['StockTurnover'].mean(),
            'total_unsold_inventory_value': self.df['UnsoldInventoryValue'].sum()
        }
        return summary


# Utility functions for common analysis patterns

def format_currency(value):
    """Format number as currency string"""
    if value >= 1000000:
        return f"${value / 1000000:.2f}M"
    elif value >= 1000:
        return f"${value / 1000:.2f}K"
    else:
        return f"${value:.2f}"


def format_percentage(value):
    """Format number as percentage string"""
    return f"{value:.2f}%"


def get_vendor_ranking(df, metric='TotalSalesDollars'):
    """Rank vendors by specified metric"""
    ranking = df.groupby('VendorName')[metric].sum().reset_index()
    ranking['Rank'] = ranking[metric].rank(ascending=False, method='dense').astype(int)
    return ranking.sort_values('Rank')


def identify_outliers(df, column, threshold=1.5):
    """Identify outliers using IQR method"""
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - threshold * IQR
    upper_bound = Q3 + threshold * IQR
    
    outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    return outliers


if __name__ == "__main__":
    # Example usage - load data and perform analysis
    from data_loader import get_db_connection, DataLoader
    
    db = get_db_connection()
    loader = DataLoader(db)
    
    # Load vendor sales summary
    vendor_data = loader.load_vendor_sales_summary()
    
    if vendor_data is not None:
        analyzer = VendorAnalyzer(vendor_data)
        analyzer.calculate_metrics()
        
        print("=== VENDOR PERFORMANCE SUMMARY ===")
        print(analyzer.get_summary_statistics())
        
        print("\n=== TOP 10 VENDORS ===")
        print(analyzer.get_top_vendors_by_sales(10))
        
        print("\n=== STATISTICAL TEST RESULTS ===")
        results = analyzer.perform_statistical_test()
        print(results)
    
    db.disconnect()
