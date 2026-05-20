"""
Data Loader Module
Handles database connections and data loading from SQLite
"""

import sqlite3
import pandas as pd
import logging
from sqlalchemy import create_engine
import os
from datetime import datetime

# Setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/data_loading.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

class DatabaseConnection:
    """Class to manage SQLite database connections"""
    
    def __init__(self, db_path='inventory.db'):
        """Initialize database connection"""
        self.db_path = db_path
        self.conn = None
        self.engine = None
        self.logger = logging.getLogger(__name__)
        
    def connect(self):
        """Create database connection"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.engine = create_engine(f'sqlite:///{self.db_path}')
            self.logger.info(f"Connected to database: {self.db_path}")
            return self.conn
        except Exception as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")
    
    def query_to_dataframe(self, query):
        """Execute SQL query and return as DataFrame"""
        try:
            df = pd.read_sql_query(query, self.conn)
            self.logger.info(f"Query executed successfully. Rows: {len(df)}")
            return df
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def get_table_list(self):
        """Get list of all tables in database"""
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        return self.query_to_dataframe(query)
    
    def get_table_info(self, table_name):
        """Get column info for a specific table"""
        query = f"PRAGMA table_info({table_name});"
        return self.query_to_dataframe(query)
    
    def get_record_count(self, table_name):
        """Get record count for a table"""
        query = f"SELECT COUNT(*) as records FROM {table_name}"
        result = self.query_to_dataframe(query)
        return result['records'].values[0]
    
    def write_dataframe(self, df, table_name, if_exists='replace'):
        """Write DataFrame to database table"""
        try:
            df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
            self.logger.info(f"DataFrame written to table: {table_name}")
        except Exception as e:
            self.logger.error(f"Failed to write DataFrame: {str(e)}")
            raise


class DataLoader:
    """Class to load specific datasets"""
    
    def __init__(self, db_connection):
        """Initialize with database connection"""
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
    
    def load_purchases(self, vendor_number=None):
        """Load purchases data"""
        query = "SELECT * FROM purchases"
        if vendor_number:
            query += f" WHERE VendorNumber = {vendor_number}"
        return self.db.query_to_dataframe(query)
    
    def load_sales(self, vendor_no=None):
        """Load sales data"""
        query = "SELECT * FROM sales"
        if vendor_no:
            query += f" WHERE VendorNo = {vendor_no}"
        return self.db.query_to_dataframe(query)
    
    def load_purchase_prices(self, vendor_number=None):
        """Load purchase prices"""
        query = "SELECT * FROM purchase_prices"
        if vendor_number:
            query += f" WHERE VendorNumber = {vendor_number}"
        return self.db.query_to_dataframe(query)
    
    def load_vendor_invoice(self, vendor_number=None):
        """Load vendor invoices"""
        query = "SELECT * FROM vendor_invoice"
        if vendor_number:
            query += f" WHERE VendorNumber = {vendor_number}"
        return self.db.query_to_dataframe(query)
    
    def load_vendor_sales_summary(self):
        """Load pre-aggregated vendor sales summary"""
        query = "SELECT * FROM vendor_sales_summary"
        try:
            return self.db.query_to_dataframe(query)
        except:
            self.logger.warning("vendor_sales_summary table not found")
            return None
    
    def load_all_tables(self):
        """Load all tables as dictionary of DataFrames"""
        tables = self.db.get_table_list()['name'].tolist()
        data = {}
        for table in tables:
            try:
                data[table] = self.db.query_to_dataframe(f"SELECT * FROM {table}")
                self.logger.info(f"Loaded table: {table}")
            except Exception as e:
                self.logger.error(f"Failed to load table {table}: {str(e)}")
        return data


def get_db_connection(db_path='inventory.db'):
    """Convenience function to create and connect database"""
    db = DatabaseConnection(db_path)
    db.connect()
    return db


if __name__ == "__main__":
    # Example usage
    db = get_db_connection()
    loader = DataLoader(db)
    
    # Load all tables
    print("Available tables:")
    print(db.get_table_list())
    
    # Load vendor sales summary
    summary = loader.load_vendor_sales_summary()
    print(f"\nVendor Sales Summary loaded: {len(summary)} rows")
    print(summary.head())
    
    db.disconnect()
