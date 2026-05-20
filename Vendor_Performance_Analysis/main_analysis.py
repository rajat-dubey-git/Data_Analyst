"""
Main Analysis Script
Demonstrates complete workflow of vendor performance analysis
"""

import sys
sys.path.insert(0, 'src')

from data_loader import get_db_connection, DataLoader
from vendor_analyzer import VendorAnalyzer, format_currency, format_percentage
from visualizations import VendorVisualizer
import pandas as pd
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main analysis function"""
    
    logger.info("=" * 60)
    logger.info("VENDOR PERFORMANCE ANALYSIS - MAIN EXECUTION")
    logger.info("=" * 60)
    
    try:
        # 1. Connect to database
        logger.info("Step 1: Connecting to database...")
        db = get_db_connection(db_path='data/raw/inventory.db')
        
        # 2. Load data
        logger.info("Step 2: Loading data...")
        loader = DataLoader(db)
        vendor_data = loader.load_vendor_sales_summary()
        
        if vendor_data is None:
            logger.error("Failed to load vendor_sales_summary. Exiting.")
            return
        
        logger.info(f"Loaded {len(vendor_data)} vendor records")
        
        # 3. Initialize analyzer
        logger.info("Step 3: Initializing analyzer...")
        analyzer = VendorAnalyzer(vendor_data)
        analyzer.calculate_metrics()
        
        # 4. Generate Summary Statistics
        logger.info("Step 4: Generating summary statistics...")
        summary = analyzer.get_summary_statistics()
        
        print("\n" + "=" * 60)
        print("SUMMARY STATISTICS")
        print("=" * 60)
        print(f"Total Vendors: {summary['total_vendors']}")
        print(f"Total Brands: {summary['total_brands']}")
        print(f"Total Sales: {format_currency(summary['total_sales'])}")
        print(f"Total Purchases: {format_currency(summary['total_purchases'])}")
        print(f"Total Gross Profit: {format_currency(summary['total_gross_profit'])}")
        print(f"Average Profit Margin: {format_percentage(summary['average_profit_margin'])}")
        print(f"Average Stock Turnover: {summary['average_stock_turnover']:.2f}")
        print(f"Total Unsold Inventory Value: {format_currency(summary['total_unsold_inventory_value'])}")
        
        # 5. Top Performers
        logger.info("Step 5: Identifying top performers...")
        
        print("\n" + "=" * 60)
        print("TOP 10 VENDORS BY SALES")
        print("=" * 60)
        top_vendors = analyzer.get_top_vendors_by_sales(10)
        for vendor, sales in top_vendors.items():
            print(f"{vendor:<30} {format_currency(sales)}")
        
        print("\n" + "=" * 60)
        print("TOP 10 BRANDS BY SALES")
        print("=" * 60)
        top_brands = analyzer.get_top_brands_by_sales(10)
        for brand, sales in top_brands.items():
            print(f"{brand:<30} {format_currency(sales)}")
        
        # 6. Vendor Performance Summary
        logger.info("Step 6: Generating vendor performance summary...")
        vendor_perf = analyzer.get_vendor_performance_summary()
        
        print("\n" + "=" * 60)
        print("VENDOR PERFORMANCE SUMMARY (Top 10)")
        print("=" * 60)
        print(vendor_perf.head(10).to_string(index=False))
        
        # 7. Brand Performance Analysis
        logger.info("Step 7: Analyzing brand performance...")
        target_brands, low_threshold, high_threshold = analyzer.identify_low_sales_high_margin_brands()
        
        print("\n" + "=" * 60)
        print(f"LOW SALES BUT HIGH PROFIT BRANDS (< {format_currency(low_threshold)}, > {format_percentage(high_threshold)})")
        print("=" * 60)
        if len(target_brands) > 0:
            print(target_brands.to_string(index=False))
        else:
            print("No brands found matching criteria")
        
        # 8. Inventory Analysis
        logger.info("Step 8: Analyzing inventory...")
        high_inventory = analyzer.identify_high_unsold_inventory(10)
        
        print("\n" + "=" * 60)
        print("TOP 10 VENDORS WITH HIGHEST UNSOLD INVENTORY VALUE")
        print("=" * 60)
        for idx, row in high_inventory.iterrows():
            print(f"{row['VendorName']:<30} {format_currency(row['UnsoldInventoryValue'])}")
        
        # 9. Bulk Discount Analysis
        logger.info("Step 9: Analyzing bulk purchasing impact...")
        bulk_analysis = analyzer.get_bulk_discount_analysis()
        
        print("\n" + "=" * 60)
        print("IMPACT OF BULK PURCHASING ON UNIT PRICE")
        print("=" * 60)
        print(bulk_analysis)
        
        # 10. Statistical Testing
        logger.info("Step 10: Performing statistical tests...")
        test_results = analyzer.perform_statistical_test()
        
        print("\n" + "=" * 60)
        print("STATISTICAL TEST: TOP vs LOW PERFORMING VENDORS")
        print("=" * 60)
        print(f"T-Statistic: {test_results['t_statistic']:.4f}")
        print(f"P-Value: {test_results['p_value']:.4f}")
        print(f"Significant Difference: {'Yes' if test_results['significant'] else 'No'}")
        print(f"Top Vendors Mean Profit Margin: {format_percentage(test_results['top_mean'])}")
        print(f"Low Vendors Mean Profit Margin: {format_percentage(test_results['low_mean'])}")
        
        # 11. Generate Visualizations (Optional - uncomment to enable)
        logger.info("Step 11: Generating visualizations...")
        visualizer = VendorVisualizer()
        
        try:
            import matplotlib.pyplot as plt
            
            # Create output directory
            import os
            os.makedirs('results/figures', exist_ok=True)
            
            # Plot and save figures
            logger.info("Creating top vendors and brands plot...")
            fig1 = visualizer.plot_top_vendors_and_brands(analyzer.df, n=10)
            fig1.savefig('results/figures/top_vendors_brands.png', dpi=300, bbox_inches='tight')
            
            logger.info("Creating Pareto chart...")
            fig2 = visualizer.plot_pareto_chart(analyzer.df, top_n=10)
            fig2.savefig('results/figures/pareto_chart.png', dpi=300, bbox_inches='tight')
            
            logger.info("Creating correlation heatmap...")
            fig3 = visualizer.plot_correlation_heatmap(analyzer.df)
            fig3.savefig('results/figures/correlation_heatmap.png', dpi=300, bbox_inches='tight')
            
            logger.info("Creating bulk discount analysis plot...")
            fig4 = visualizer.plot_bulk_discount_analysis(analyzer.df)
            fig4.savefig('results/figures/bulk_discount_analysis.png', dpi=300, bbox_inches='tight')
            
            print("\n✓ Visualizations saved to results/figures/")
            
        except Exception as e:
            logger.warning(f"Could not generate visualizations: {str(e)}")
        
        # 12. Export Results
        logger.info("Step 12: Exporting results to CSV...")
        analyzer.df.to_csv('results/vendor_performance_analysis.csv', index=False)
        vendor_perf.to_csv('results/vendor_performance_summary.csv', index=False)
        
        print("\n✓ Results exported to results/")
        
        # Disconnect
        db.disconnect()
        
        logger.info("=" * 60)
        logger.info("ANALYSIS COMPLETE")
        logger.info("=" * 60)
        
        print("\n✓ Analysis complete! Check results/ and logs/ directories for outputs.")
        
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}", exc_info=True)
        print(f"\n✗ Error: {str(e)}")


if __name__ == "__main__":
    main()
