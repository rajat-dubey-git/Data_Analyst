
-- =============================================================================
-- PROJECT  : World Layoffs — Data Cleaning
-- DATABASE : world_layoff
-- AUTHOR   : [Your Name]
-- DATE     : 2024
-- =============================================================================
-- OBJECTIVE:
--   Transform raw layoffs data into a clean, analysis-ready staging table.
--
-- STEPS:
--   1. Create a staging copy (never modify raw data)
--   2. Remove duplicate rows
--   3. Standardize text fields (trim whitespace, unify categories)
--   4. Fix date format and convert to DATE type
--   5. Handle NULL / blank values
--   6. Drop helper columns and rows with no useful data
-- =============================================================================

USE world_layoff;

-- =============================================================================
-- STEP 0 — Inspect the raw table
-- =============================================================================

SELECT * FROM layoffs;
SELECT COUNT(*) AS total_rows FROM layoffs;

-- =============================================================================
-- STEP 1 — Create a staging copy (layoffs_staging)
--           Rule: never alter the original raw table.
-- =============================================================================

CREATE TABLE layoffs_staging LIKE layoffs;
INSERT INTO layoffs_staging SELECT * FROM layoffs;

SELECT COUNT(*) AS staging_rows FROM layoffs_staging;

-- =============================================================================
-- STEP 2 — Remove Duplicates
--
--  Strategy: assign a row_number partitioned over all business-key columns.
--  Any row with row_num > 1 is a duplicate of an earlier row.
--
--  Why a second staging table?
--  MySQL does not allow DELETE on a CTE directly, so we materialise
--  the row_number into a new table (layoffs_staging2) and delete from there.
-- =============================================================================

-- 2a. Preview duplicates before deletion
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry,
                            total_laid_off, percentage_laid_off,
                            `date`, stage, country, funds_raised_millions
               ORDER BY total_laid_off DESC
           ) AS row_num
    FROM layoffs_staging
)
SELECT * FROM duplicate_cte WHERE row_num > 1;

-- 2b. Create staging2 with row_num column
CREATE TABLE `layoffs_staging2` (
    `company`               TEXT,
    `location`              TEXT,
    `industry`              TEXT,
    `total_laid_off`        INT  DEFAULT NULL,
    `percentage_laid_off`   TEXT,
    `date`                  TEXT,
    `stage`                 TEXT,
    `country`               TEXT,
    `funds_raised_millions` INT  DEFAULT NULL,
    `row_num`               INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 2c. Populate staging2 with row numbers
INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry,
                        total_laid_off, percentage_laid_off,
                        `date`, stage, country, funds_raised_millions
           ORDER BY total_laid_off DESC
       ) AS row_num
FROM layoffs_staging;

-- 2d. Verify duplicates
SELECT * FROM layoffs_staging2 WHERE row_num > 1;

-- 2e. Delete duplicates
SET SQL_SAFE_UPDATES = 0;
DELETE FROM layoffs_staging2 WHERE row_num > 1;

SELECT COUNT(*) AS rows_after_dedup FROM layoffs_staging2;

-- =============================================================================
-- STEP 3 — Standardize Text Fields
-- =============================================================================

-- 3a. Trim leading/trailing whitespace from company names
UPDATE layoffs_staging2
SET company = TRIM(company);

-- 3b. Consolidate industry variants — e.g. 'Crypto Currency', 'CryptoCurrency' → 'Crypto'
SELECT DISTINCT industry FROM layoffs_staging2 ORDER BY 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 3c. Fix trailing period in country name  ('United States.' → 'United States')
SELECT DISTINCT country FROM layoffs_staging2 ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- =============================================================================
-- STEP 4 — Fix & Convert the Date Column
--
--  The raw date column is stored as TEXT in 'MM/DD/YYYY' format.
--  Goal: convert to a proper DATE column.
-- =============================================================================

-- 4a. Convert text to DATE
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- 4b. Change column type from TEXT to DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 4c. Verify
SELECT `date` FROM layoffs_staging2 LIMIT 10;

-- =============================================================================
-- STEP 5 — Handle NULL and Blank Values
-- =============================================================================

-- 5a. Convert empty-string industry to NULL (consistent representation)
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- 5b. Self-join: fill in missing industry values using other rows of the same company
--     (e.g., if Airbnb row A has industry=NULL and row B has industry='Travel', fill A)
SELECT l1.company, l1.industry AS missing, l2.industry AS fill_from
FROM layoffs_staging2 l1
JOIN layoffs_staging2 l2
    ON l1.company = l2.company
WHERE l1.industry IS NULL
  AND l2.industry IS NOT NULL;

UPDATE layoffs_staging2 l1
JOIN layoffs_staging2 l2
    ON l1.company = l2.company
SET l1.industry = l2.industry
WHERE l1.industry IS NULL
  AND l2.industry IS NOT NULL;

-- 5c. Check which companies still have NULL industry (only 1 row, cannot self-fill)
SELECT * FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- =============================================================================
-- STEP 6 — Remove Rows with No Useful Layoff Data
--
--  Rows where BOTH total_laid_off and percentage_laid_off are NULL
--  provide no analytical value and are dropped.
-- =============================================================================

-- 6a. Preview rows to be removed
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

SELECT COUNT(*) AS rows_to_drop FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- 6b. Create final clean table
CREATE TABLE layoffs_staging3 LIKE layoffs_staging2;
INSERT INTO layoffs_staging3 SELECT * FROM layoffs_staging2;

-- 6c. Delete uninformative rows from final table
DELETE FROM layoffs_staging3
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- 6d. Drop the helper column no longer needed
ALTER TABLE layoffs_staging3
DROP COLUMN row_num;

-- =============================================================================
-- STEP 7 — Final Verification
-- =============================================================================

SELECT * FROM layoffs_staging3;
SELECT COUNT(*) AS final_row_count FROM layoffs_staging3;
