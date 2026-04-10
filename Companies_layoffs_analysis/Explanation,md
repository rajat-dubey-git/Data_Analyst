# 📖 Project Explanation — World Layoffs SQL

This document walks through every decision made in this project: *why* each step was taken, not just *what* was done. Useful if you're reading this code to learn, or if you're reviewing it.

---

## Part 1 — Data Cleaning

---

### Why we never touch the raw table

The very first thing we do is copy `layoffs` into `layoffs_staging`. This is a standard practice in data work:

- If something goes wrong, the original data is safe
- You can always compare your cleaned data against the original
- Reviewers/interviewers can verify your transformations

**Rule: `layoffs` (raw) is read-only for the entire project.**

---

### Step 2 — Removing Duplicates

**The challenge:** SQL tables don't have a built-in row identifier. Two rows can look identical across every column.

**The solution:** `ROW_NUMBER()` — a window function that assigns a number to each row within a "partition" (a group of identical rows). The first occurrence gets `row_num = 1`, any repeat gets `row_num = 2, 3...`

```sql
ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, total_laid_off,
                 percentage_laid_off, date, stage, country, funds_raised_millions
    ORDER BY total_laid_off DESC
) AS row_num
```

We partition by **all meaningful business-key columns** — if every single field matches, it's a true duplicate.

**Why a second staging table?**

MySQL doesn't allow you to `DELETE` from a CTE directly — it throws an error. The workaround is to materialise the row numbers into a real table (`layoffs_staging2`) and then delete from that table where `row_num > 1`.

---

### Step 3 — Standardizing Text

**Company names:** Some had leading or trailing spaces (`' Airbnb'` vs `'Airbnb'`). A simple `TRIM()` fixes this.

**Industry:** The Crypto category had several variants:
- `'Crypto'`
- `'Crypto Currency'`
- `'CryptoCurrency'`

These all represent the same industry. We use `LIKE 'Crypto%'` to catch all variants and set them to `'Crypto'`.

**Country:** One entry was `'United States.'` (trailing period — likely a data entry error). We use `TRIM(TRAILING '.' FROM country)` to fix it cleanly.

---

### Step 4 — Fixing the Date Column

**The problem:** The `date` column was stored as `TEXT` in `MM/DD/YYYY` format. You can't do date math, sorting, or time-series analysis on a text field.

**The fix (two steps):**

Step 1 — Parse the text into a proper date:
```sql
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
```

Step 2 — Change the column's data type:
```sql
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
```

> **Why two steps?** You can't use `ALTER TABLE` to convert a string format directly. First you convert the values, then you change the column type.

---

### Step 5 — Handling NULLs and Blanks

**Empty strings vs NULLs:** Some `industry` values were `''` (empty string) instead of `NULL`. In SQL, `WHERE industry IS NULL` won't catch empty strings — they're not the same thing. We convert all `''` to `NULL` for consistent handling:

```sql
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';
```

**Self-join to fill missing industry:**

Some rows had `NULL` industry, but *another row for the same company* had the industry filled in. We can use that to fill the gap:

```sql
UPDATE layoffs_staging2 l1
JOIN layoffs_staging2 l2 ON l1.company = l2.company
SET l1.industry = l2.industry
WHERE l1.industry IS NULL
  AND l2.industry IS NOT NULL;
```

Example: If `Airbnb` has 3 rows and 1 of them has `industry = NULL` but the other 2 say `Travel`, this join fills in `Travel` for the missing row.

---

### Step 6 — Removing Rows with No Data

Rows where both `total_laid_off` AND `percentage_laid_off` are `NULL` tell us nothing analytically — we don't know how many people were laid off or what fraction of the company it represented.

These rows are removed in the final clean table (`layoffs_staging3`).

> Note: rows where only *one* field is NULL are kept — partial data is still useful.

---

### Why three staging tables?

| Table | Purpose |
|---|---|
| `layoffs_staging` | Direct copy of raw data; safety net |
| `layoffs_staging2` | Working table with `row_num` added for dedup |
| `layoffs_staging3` | Final clean table; `row_num` dropped, uninformative rows removed |

This staged approach makes it easy to debug: if something looks wrong, you can query any intermediate table to find where the issue was introduced.

---

## Part 2 — Exploratory Data Analysis

---

### Section 1 — Extremes

We start EDA by asking: what are the boundaries of this data?

- `MAX(total_laid_off)` — largest single event
- `percentage_laid_off = 1` — companies that laid off 100% of staff (effectively shut down)

Sorting the 100% layoffs by `funds_raised_millions DESC` is interesting — it shows well-funded companies that still failed.

---

### Section 5 — Rolling Monthly Total (CTE + Window Function)

This is one of the more advanced queries in the project. The goal is to see not just how many layoffs happened each month, but the **cumulative running total** — which helps identify acceleration.

```sql
WITH monthly_totals AS (
    SELECT
        SUBSTRING(`date`, 1, 7) AS `month`,  -- Extract 'YYYY-MM'
        SUM(total_laid_off) AS monthly_laid_off
    FROM layoffs_staging3
    GROUP BY SUBSTRING(`date`, 1, 7)
)
SELECT
    `month`,
    monthly_laid_off,
    SUM(monthly_laid_off) OVER (ORDER BY `month`) AS rolling_total
FROM monthly_totals;
```

**Why `SUBSTRING(date, 1, 7)`?**
We group by `YYYY-MM` (the first 7 characters of the date) — this aggregates all layoffs in the same month regardless of exact day.

**Why `SUM() OVER (ORDER BY month)`?**
This is a *running* sum — it adds up all values from the start up to the current row. Without `PARTITION BY`, it accumulates across the entire dataset in chronological order.

---

### Section 7 — Year-wise Rankings with DENSE_RANK

The final and most complex query answers: *"Who was #1 in layoffs in 2022? And 2023?"*

We use **two CTEs stacked together:**

```sql
WITH company_year AS (
    -- Aggregate total layoffs per company per year
    ...
),
ranked AS (
    -- Apply DENSE_RANK within each year
    DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS rank
)
SELECT * FROM ranked WHERE rank <= 5;
```

**`DENSE_RANK` vs `RANK`:**
If two companies are tied for 2nd place, `DENSE_RANK` gives both a `2` and the next company gets `3`. `RANK` would give the next company a `4` (skipping 3). `DENSE_RANK` is generally more intuitive for this type of analysis.

---

## Summary of SQL Concepts Used

| Concept | Applied In |
|---|---|
| `ROW_NUMBER()` | Deduplication |
| `DENSE_RANK()` | Year-wise company ranking |
| `SUM() OVER (ORDER BY …)` | Rolling monthly cumulative total |
| CTEs | Dedup preview, rolling total, rankings |
| `STR_TO_DATE()` + `ALTER TABLE` | Date type conversion |
| Self-JOIN | Filling NULL industry values |
| `TRIM()` / `LIKE` / `TRIM(TRAILING …)` | Text standardization |
| `SUBSTRING()` | Month extraction for time series |
| Staged tables | Safe, auditable transformation pipeline |
