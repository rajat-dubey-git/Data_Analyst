🌍 World Layoffs — SQL Data Cleaning & EDA
A complete end-to-end SQL project that takes a raw, messy dataset of global tech layoffs and transforms it into a clean, analysis-ready table — then explores it with meaningful queries to surface real-world trends.
________________________________________

📊 Dataset
Field	Description
company -	Company name
location -	City/region of layoff
industry -	Sector (e.g., Tech, Finance, Crypto)
total_laid_off -	Absolute number of employees laid off
percentage_laid_off -	Fraction of workforce laid off (0–1)
date -	Date of layoff event
stage	- Funding stage (e.g., Series B, Post-IPO)
country -	Country where the layoff occurred
funds_raised_millions -	Total funding raised by the company

Source: Layoffs.fyi — a community-tracked dataset of tech layoffs worldwide.
________________________________________

🛠️ Part 1 — Data Cleaning
The raw dataset had several data quality issues that were resolved systematically:

Problems Found & Fixed
Issue	Fix Applied

Duplicate rows	ROW_NUMBER() window function to identify and delete exact duplicates

Inconsistent industry names	'Crypto Currency', 'CryptoCurrency' → unified to 'Crypto'

Trailing punctuation in country	'United States.' → 'United States' via TRIM(TRAILING '.')

Whitespace in company names	TRIM() applied to all company values

Date stored as TEXT	Converted from 'MM/DD/YYYY' string to proper DATE type

Empty strings in industry	Converted to NULL for consistent handling

Missing industry (self-fillable)	Self-join used to fill NULL industry from same company's other rows

Rows with no layoff data at all	Rows where both total_laid_off and percentage_laid_off are NULL were dropped

Staging Table Strategy
layoffs (raw)
    ↓ copy
layoffs_staging
    ↓ add row_num, deduplicate
layoffs_staging2
    ↓ drop no-data rows, drop row_num column
layoffs_staging3  ← final clean table used for analysis

Raw data is never modified. All transformations happen on staging copies.
________________________________________

🔍 Part 2 — Exploratory Data Analysis
Key Questions Answered

Scale & Extremes
•	What is the largest single layoff event in the dataset?
•	Which companies laid off 100% of their staff (i.e., shut down)?

Aggregations
•	Which companies had the highest total layoffs across all events?
•	Which industries were hit the hardest?
•	Which countries saw the most layoffs?

Time Trends
•	How did layoffs change year over year?
•	What does the rolling cumulative total look like month by month?

Rankings (Window Functions)
•	Which company had the most layoffs in each calendar year?
•	Who were the Top 5 companies per year by total layoffs?
________________________________________

💡 SQL Concepts Used
Concept	Where Used
ROW_NUMBER() -	Duplicate detection and removal
DENSE_RANK() -	Year-wise company ranking
SUM() OVER (ORDER BY …)	- Rolling cumulative total
CTEs (WITH … AS) -	Multi-step logic in EDA and deduplication
STR_TO_DATE() -	Text-to-date conversion
Self-JOIN	- Filling missing industry values
ALTER TABLE … MODIFY COLUMN	- Changing column data type
TRIM() / LIKE	- Text standardization
________________________________________

▶️ How to Run
1.	Open your MySQL client (MySQL Workbench, DBeaver, etc.)
2.	Create the database and import the raw layoffs table
3.	Run scripts/01_data_cleaning.sql — creates layoffs_staging3
4.	Run scripts/02_exploratory_data_analysis.sql — all EDA queries on clean data

-- Quick start
USE world_layoff;
SOURCE scripts/01_data_cleaning.sql;
SOURCE scripts/02_exploratory_data_analysis.sql;
________________________________________
📌 Key Findings (Sample)
•	Amazon, Google, Meta were consistently in the top 5 companies by total layoffs
•	Consumer and Retail industries saw the largest aggregate cuts
•	United States accounted for the majority of global layoffs in the dataset
•	Post-IPO companies laid off far more workers than early-stage startups
•	Layoffs spiked sharply in late 2022 and early 2023, visible in the rolling monthly total
________________________________________
🧰 Tools Used
•	MySQL 8.0
•	MySQL Workbench
________________________________________

