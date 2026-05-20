# Data Dictionary

## Overview
This document describes all tables and columns in the `inventory.db` SQLite database used for the Vendor Performance Analysis.

---

## Table: `purchases`

**Description**: Contains all purchase orders made from vendors.

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| PurchaseID | INTEGER | Unique identifier for purchase order | 12345 |
| VendorNumber | INTEGER | Vendor identifier | 1128 |
| PurchaseDate | DATE | Date of purchase order | 2023-01-15 |
| OrderAmount | DECIMAL | Total order amount | 5000.00 |
| Quantity | INTEGER | Number of units purchased | 100 |
| Brand | TEXT | Product brand | "Brand A" |
| OrderStatus | TEXT | Status of order (Pending/Completed/Cancelled) | "Completed" |

---

## Table: `sales`

**Description**: Sales transactions resulting from vendor products.

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| SalesID | INTEGER | Unique sales transaction ID | 98765 |
| VendorNo | INTEGER | Vendor identifier | 1128 |
| SaleDate | DATE | Date of sale | 2023-01-20 |
| SalesAmount | DECIMAL | Total sales revenue | 7500.00 |
| Quantity | INTEGER | Units sold | 150 |
| Region | TEXT | Geographic region | "North" |
| Margin | DECIMAL | Profit margin % | 25.5 |

---

## Table: `vendor_invoice`

**Description**: Invoice records for vendor transactions.

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| InvoiceID | INTEGER | Unique invoice identifier | 54321 |
| VendorNumber | INTEGER | Vendor identifier | 1128 |
| InvoiceDate | DATE | Invoice date | 2023-01-15 |
| InvoiceAmount | DECIMAL | Invoice total | 5000.00 |
| DueDate | DATE | Payment due date | 2023-02-15 |
| PaidDate | DATE | Actual payment date | 2023-02-10 |
| PaymentStatus | TEXT | Status (Paid/Pending/Overdue) | "Paid" |
| Currency | TEXT | Currency code | "USD" |

---

## Table: `purchase_prices`

**Description**: Historical pricing information for vendor products.

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| PriceID | INTEGER | Unique price record ID | 11111 |
| VendorNumber | INTEGER | Vendor identifier | 1128 |
| ProductName | TEXT | Name of product | "Widget A" |
| UnitPrice | DECIMAL | Price per unit | 50.00 |
| EffectiveDate | DATE | Date price became effective | 2023-01-01 |
| EndDate | DATE | Date price ended (NULL if current) | 2023-12-31 |
| CostPrice | DECIMAL | Cost to purchase | 40.00 |

---

## Data Quality Notes

### Missing Values
- `PaidDate` in `vendor_invoice`: NULL indicates unpaid invoices
- `EndDate` in `purchase_prices`: NULL indicates current active price

### Data Ranges
- **Date Range**: January 2022 - December 2023
- **Number of Vendors**: 50+
- **Currency**: All values in USD unless specified

### Data Validation Rules
- PurchaseDate ≤ SaleDate
- SalesAmount ≥ OrderAmount
- Margin should be between -50% and 100%
- PaidDate ≤ DueDate (for paid invoices)

---

## Derived Metrics

The following metrics are calculated in the analysis:

| Metric | Formula | Interpretation |
|--------|---------|-----------------|
| Order Value Average | Sum(OrderAmount) / Count(Orders) | Average purchase order size |
| Cost-to-Sales Ratio | Sum(CostPrice) / Sum(SalesAmount) | Profitability efficiency |
| Order Frequency | Count(Orders) / Time Period | How often we order from vendor |
| Payment Days | DatedDiff(PaidDate, InvoiceDate) | Average payment time |
| Vendor Reliability | On-Time Deliveries / Total Orders | Delivery performance % |

---

## Data Sources & Updates

- **Data Collection**: Automated from ERP system
- **Last Updated**: [Current Date]
- **Update Frequency**: Daily
- **Data Owner**: [Department/Person]
- **Backup Location**: [Server/Cloud location]

---

## Sensitive/PII Data

- **Vendor Names**: Not included (only numbers used for privacy)
- **Customer Names**: Removed before analysis
- **Payment Information**: Masked in outputs

---

## For Questions

Contact: [Data Team Lead]
Email: [data.team@company.com]
