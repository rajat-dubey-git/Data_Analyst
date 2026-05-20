"""
Vendor Performance Analysis Package
A comprehensive analysis of vendor performance metrics and trends
"""

__version__ = "1.0.0"
__author__ = "Your Name"
__description__ = "Vendor Performance Analysis using Python, Pandas, and SQLite"

from .data_loader import DatabaseConnection, DataLoader, get_db_connection
from .vendor_analyzer import VendorAnalyzer, format_currency, format_percentage
from .visualizations import VendorVisualizer

__all__ = [
    'DatabaseConnection',
    'DataLoader',
    'get_db_connection',
    'VendorAnalyzer',
    'format_currency',
    'format_percentage',
    'VendorVisualizer'
]
