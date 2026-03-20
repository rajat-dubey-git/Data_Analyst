-- Data Cleaning --

select * from `world_layoff`.layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Fill Null values or blank values
-- 4. Remove any columns

use world_layoff;

drop table layoffs_staging;
drop table layoffs_staging2;

select * from layoffs;

select count(*) as number_of_rows from layoffs;

create table layoffs_staging like layoffs;

insert layoffs_staging select * from layoffs;

select count(*) from layoffs_staging;

select * from layoffs_staging;

select *, row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date` order by total_laid_off desc) as row_num from layoffs_staging;

WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
               ORDER BY total_laid_off DESC
           ) AS row_num
    FROM layoffs_staging
)
-- SELECT *
-- FROM duplicate_cte
-- WHERE company IN (
--     SELECT company
--     FROM duplicate_cte
--     WHERE row_num > 1
-- );
select * from duplicate_cte where row_num > 1;

SET SQL_SAFE_UPDATES = 0;

WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
               ORDER BY total_laid_off DESC
           ) AS row_num
    FROM layoffs_staging
)delete from duplicate_cte where row_num > 1; 
-- (it won't work)

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

insert into layoffs_staging2
SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
               ORDER BY total_laid_off DESC
           ) AS row_num
    FROM layoffs_staging;
    
select * from layoffs_staging2
where row_num > 1;

delete from layoffs_staging2
where row_num > 1;

select count(*) from layoffs_staging2;

-- Standardizing data

select count(distinct(company)) from layoffs_staging2;

select count(company) from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct(industry) from layoffs_staging2
ORDER BY 1;

SELECT * FROM layoffs_staging2
where industry like 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
where industry LIKE 'Crypto%';

select distinct(industry) from layoffs_staging2;

select distinct(location) from layoffs_staging2;

select distinct(country) from layoffs_staging2 order by 1;

select * from layoffs_staging2
where country = 'United States.';

select distinct country, trim(trailing '.' from country) from layoffs_staging2;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country = 'United States.'; 

select distinct country from layoffs_staging2;

select * from layoffs_staging2;

SELECT 
    `date`,
    STR_TO_DATE(`date`, '%m/%d/%Y') AS converted_date,
    DATE_FORMAT(STR_TO_DATE(`date`, '%m/%d/%Y'), '%d/%m/%Y') AS date_formatted
FROM layoffs_staging2;

update layoffs_staging2
set date = DATE_FORMAT(STR_TO_DATE(`date`, '%m/%d/%Y'), '%d/%m/%Y');

select * from layoffs_staging2;

update layoffs_staging2
set date = STR_TO_DATE(`date`, '%d/%m/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT `date`
FROM layoffs_staging2
WHERE STR_TO_DATE(`date`, '%d/%m/%Y') IS NULL
  AND STR_TO_DATE(`date`, '%m/%d/%Y') IS NULL;

-- Error Code: 1292. Incorrect date value: '16/12/2022' for column 'date' at row 1 (fixed)

select * from layoffs_staging2;

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select count(*) from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_staging2
where company = 'Airbnb';

update layoffs_staging2
set industry = null
where industry='';

select l1.industry, l2.industry from layoffs_staging2 l1
join layoffs_staging2 l2
on l1.company=l2.company
where (l1.industry is null or l1.industry = '')
and l2.industry is not null;

update layoffs_staging2 l1
join layoffs_staging2 l2
on l1.company=l2.company
set l1.industry = l2.industry
where l1.industry is null
and l2.industry is not null;

select * from layoffs_staging2
where industry is null or 
industry='';

select * from layoffs_staging2
where company like 'Bally%';

select * from layoffs_staging2;

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

create table layoffs_staging3 like layoffs_staging2;

select * from layoffs_staging3;

insert into layoffs_staging3
select * from layoffs_Staging2;

SELECT 
    (SELECT COUNT(*) FROM layoffs_staging2) AS total_data_of_layoffs_staging2,
    (SELECT COUNT(*) FROM layoffs_staging3) AS total_data_of_layoffs_staging3;
    
delete from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging3
drop column row_num;

select * from layoffs_staging3;

-- Exploratory Data Analysis --

select * from layoffs_staging3;

select max(total_laid_off) from layoffs_staging3;

select max(total_laid_off), max(percentage_laid_off) from layoffs_staging3;

select * from layoffs_staging3
where percentage_laid_off = 1
order by total_laid_off desc;

select * from layoffs_staging3
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off), industry
from layoffs_staging3
group by company, industry
order by 2 desc;

select sum(total_laid_off), industry
from layoffs_staging3
group by industry
order by 1 desc;

select min(`date`), max(`date`) from layoffs_staging3;

select sum(total_laid_off), country
from layoffs_staging3
group by country
order by 1 desc;

select sum(total_laid_off), `date`
from layoffs_staging3
group by `date`
order by 1 desc;

select sum(total_laid_off), year(`date`)
from layoffs_staging3
group by year(`date`)
order by 1 desc;

select sum(total_laid_off), stage
from layoffs_staging3
group by stage
order by 1 desc;

select substring(`date`,6,2) as `Month`, sum(total_laid_off)
from layoffs_staging3
group by `Month`
order by `Month` asc;

WITH rolling_total AS 
(
    SELECT 
        SUBSTRING(`date`, 1, 7) AS `month`, 
        SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging3
    GROUP BY `month`
)
SELECT 
    `month`, total_laid_off,
    SUM(total_laid_off) OVER (ORDER BY `month`) AS rolling
FROM rolling_total;

SELECT 
    company, 
    industry, 
    YEAR(`date`) AS year, 
    SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging3
GROUP BY company, industry, year
ORDER BY total_laid_off DESC;


WITH company_year AS
(
    SELECT 
        company, 
        industry, 
        YEAR(`date`) AS year, 
        SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging3
    GROUP BY company, industry, year
)
SELECT 
    company, 
    industry, 
    year, 
    total_laid_off,
    DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS ranking
FROM company_year;



