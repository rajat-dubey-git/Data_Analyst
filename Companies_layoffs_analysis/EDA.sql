-- =============================================================================
-- PROJECT  : World Layoffs — Exploratory Data Analysis (EDA)
-- DATABASE : world_layoff
-- TABLE    : layoffs_staging3  (cleaned, final table)
-- AUTHOR   : [Your Name]
-- DATE     : 2024
-- =============================================================================
-- OBJECTIVE:
--   Explore trends and patterns in global tech layoffs to answer:
--     - Which companies laid off the most people?
--     - Which industries and countries were hardest hit?
--     - How did layoffs trend over time (monthly/yearly)?
--     - Which companies had the biggest single-year layoffs? (ranked)
-- =============================================================================

USE world_layoff;

-- =============================================================================
-- SECTION 1 — Overview & Extremes
-- =============================================================================

-- 1a. Full dataset
SELECT * FROM layoffs_staging3;

-- 1b. Largest single-event layoff and highest percentage
SELECT
    MAX(total_laid_off)       AS max_single_event,
    MAX(percentage_laid_off)  AS max_percentage
FROM layoffs_staging3;

-- 1c. Companies that laid off 100% of their workforce — ordered by headcount impact
--     These are companies that shut down entirely.
SELECT *
FROM layoffs_staging3
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- 1d. Same as above but sorted by funding — shows well-funded companies that still folded
SELECT *
FROM layoffs_staging3
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- 1e. Date range of the dataset
SELECT
    MIN(`date`) AS earliest_layoff,
    MAX(`date`) AS latest_layoff
FROM layoffs_staging3;

-- =============================================================================
-- SECTION 2 — Company-Level Analysis
-- =============================================================================

-- 2a. Total layoffs per company (with industry for context), highest first
SELECT
    company,
    industry,
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging3
GROUP BY company, industry
ORDER BY total_laid_off DESC;

-- =============================================================================
-- SECTION 3 — Industry-Level Analysis
-- =============================================================================

-- 3a. Total layoffs by industry — which sectors were hit hardest?
SELECT
    industry,
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging3
GROUP BY industry
ORDER BY total_laid_off DESC;

-- =============================================================================
-- SECTION 4 — Country-Level Analysis
-- =============================================================================

-- 4a. Total layoffs by country
SELECT
    country,
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging3
GROUP BY country
ORDER BY total_laid_off DESC;

-- =============================================================================
-- SECTION 5 — Time-Series Analysis
-- =============================================================================

-- 5a. Total layoffs by calendar year
SELECT
    YEAR(`date`)              AS `year`,
    SUM(total_laid_off)       AS total_laid_off
FROM layoffs_staging3
GROUP BY YEAR(`date`)
ORDER BY `year` ASC;

-- 5b. Total layoffs by month (across all years) — seasonal patterns
SELECT
    MONTH(`date`)             AS `month`,
    SUM(total_laid_off)       AS total_laid_off
FROM layoffs_staging3
GROUP BY MONTH(`date`)
ORDER BY `month` ASC;

-- 5c. Rolling (cumulative) monthly layoffs
--     Shows acceleration or deceleration of layoffs over time.
WITH monthly_totals AS (
    SELECT
        SUBSTRING(`date`, 1, 7)   AS `month`,   -- 'YYYY-MM'
        SUM(total_laid_off)        AS monthly_laid_off
    FROM layoffs_staging3
    WHERE `date` IS NOT NULL
    GROUP BY SUBSTRING(`date`, 1, 7)
)
SELECT
    `month`,
    monthly_laid_off,
    SUM(monthly_laid_off) OVER (ORDER BY `month`) AS rolling_total
FROM monthly_totals
ORDER BY `month`;

-- =============================================================================
-- SECTION 6 — Funding Stage Analysis
-- =============================================================================

-- 6a. Total layoffs by company funding stage
--     e.g., Post-IPO companies vs Series B startups
SELECT
    stage,
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging3
GROUP BY stage
ORDER BY total_laid_off DESC;

-- =============================================================================
-- SECTION 7 — Year-wise Company Rankings (Window Function)
--
--  For each year, rank companies by total layoffs using DENSE_RANK.
--  Useful for identifying which company led layoffs in each year.
-- =============================================================================

-- 7a. Company totals per year
SELECT
    company,
    industry,
    YEAR(`date`)          AS `year`,
    SUM(total_laid_off)   AS total_laid_off
FROM layoffs_staging3
GROUP BY company, industry, YEAR(`date`)
ORDER BY total_laid_off DESC;

-- 7b. Ranked: Top companies per year by layoffs (DENSE_RANK)
WITH company_year AS (
    SELECT
        company,
        industry,
        YEAR(`date`)          AS `year`,
        SUM(total_laid_off)   AS total_laid_off
    FROM layoffs_staging3
    GROUP BY company, industry, YEAR(`date`)
)
SELECT
    company,
    industry,
    `year`,
    total_laid_off,
    DENSE_RANK() OVER (
        PARTITION BY `year`
        ORDER BY total_laid_off DESC
    ) AS `rank`
FROM company_year
WHERE `year` IS NOT NULL
ORDER BY `year`, `rank`;

-- 7c. Filter to only Top 5 companies per year
WITH company_year AS (
    SELECT
        company,
        industry,
        YEAR(`date`)          AS `year`,
        SUM(total_laid_off)   AS total_laid_off
    FROM layoffs_staging3
    GROUP BY company, industry, YEAR(`date`)
),
ranked AS (
    SELECT
        company,
        industry,
        `year`,
        total_laid_off,
        DENSE_RANK() OVER (
            PARTITION BY `year`
            ORDER BY total_laid_off DESC
        ) AS `rank`
    FROM company_year
    WHERE `year` IS NOT NULL
)
SELECT *
FROM ranked
WHERE `rank` <= 5
ORDER BY `year`, `rank`;
