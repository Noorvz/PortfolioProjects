-- Data Cleaning SQL Project
-- Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022


-- Make a copy of the table layoff and enter it in a staging table. Raw data remains the same.

select * 
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

select * 
FROM layoffs_staging;

INSERT Layoffs_staging
SELECT * 
FROM layoffs;


-- Steps to clean the data:
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Remove null or blank values
-- 4. Remove any columns and rows



-- 1. Remove duplicates

-- indentify dumplicates. When the row number is greater then 1, then there are duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;


-- create a common table expression of previous SELECT statement. 
-- Only duplicates (row_number greater then 1) are shown because this is specified in WHERE clause

WITH duplicate_cte AS 
     (
       select *,
	   ROW_NUMBER() OVER(
       PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
       AS row_num
       FROM layoffs_staging
      )
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


-- taking a closer look at one of the duplicates

SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';


-- Create a staging2 table (copy of the staging table with addition of the row_num Column as datatype INT) 
-- so that duplicates can be removed

CREATE TABLE `layoffs_staging2`
(
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num INT
) 
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Filling the staging2 table with the data of staging

INSERT INTO layoffs_staging2
SELECT 
 *,
 ROW_NUMBER() OVER(
                   PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
                   stage, country, funds_raised_millions
                   ) AS row_num
FROM layoffs_staging;


-- Checking if the data is now in staging2

SELECT * 
FROM layoffs_staging2

-- selecting the duplicates

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;


-- deleting the duplicates 

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;


-- checking if deletion was succesfull

SELECT * 
FROM layoffs_staging2;



-- 2. Standardizing data

-- There are some extra spaces in the column company, so these should be trimmed
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


-- Not all values of the column industry are right. There is Crypto, Cryptocurrency and Crypto currency. 

SELECT DISTINCT *
FROM layoffs_staging2
WHERE industry LIKE '%crypto%';


-- change the value of industry to Crypto for all similar values

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE '%crypto%';


-- checking if values for crypo got updated

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;


-- countries should only show up once because of the DISTINCT keyword.
-- United states is in the results twice. Once with a "." at the end. 

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


-- Removing the "." 

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
 
 
 -- Checking if the "." got deleted
 
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


-- The column date is a text column. 
-- setting `date` in the date format

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


-- change the actual data type to date

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;



-- 3. Null values or Blank values
-- identified null and bank values in the industry column

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';


-- change all the blanks to null values

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


-- selfjoin to identify all companies which have a null value and a not-null value in the column industry 

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
     ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- change null value of t1 and replace it with the value of t2 

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
   ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;



-- 4. Remove any columns or rows
-- identify the rows where there is no information on the amount or percentage of layoffs

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- delete those rows as there might not even have been any layoffs
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- he column row_num was previously added in order to delete the duplicates.
-- These column is not needed anymore as all duplicates have been deleted
-- remove column row_num

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- view cleaned version of the dataset

SELECT * 
FROM layoffs_staging2;

