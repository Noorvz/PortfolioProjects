-- exploratory data analysis Project SQL
-- Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022
-- data cleaning is done -> layoffs_staging2
-- find out if there are patterns or interesting outliers

SELECT * 
FROM layoffs_staging2;

-- finding out what the biggest amount of layoffs was in one day
SELECT MAX(total_laid_off)
FROM layoffs_staging2;

-- looking at which company has laid off the most people in a day (Google has laid off the most amount of employees)
SELECT *
FROM layoffs_staging2
ORDER BY total_laid_off DESC;

-- Looking at Percentage of the layoffs
SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM layoffs_staging2;

-- Seeing which companies laid off all their staff (went bankrupt)
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

-- seeing the timespan of the data (2020-03-11	AND 2023-03-06 , during Covid Pandemic)
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- seeing which industry had the most layoffs (consumer and retail, makes sense as data is from during the Covid Pandemic)
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

-- seeing the total amount of layoffs per year
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
WHERE YEAR(date) IS NOT NULL
GROUP BY YEAR(date)
ORDER BY 1 ASC;


-- Create two cte's and combine them.
--     The first CTE is "Company_Year" ->  This CTE calculates the total number of layoffs per company per year.
--     The second CTE is "Company_Year_Rank" ->   This CTE assigns a rank to each company per year 
--                                                based on the total number of layoffs in descending order.
-- The main query selects all columns from the Company_Year_Rank CTE.
-- It filters the results to include only the top 5 companies with the highest number of layoffs for each year.

WITH Company_Year (company, years, total_laid_off) AS
(
  SELECT company, YEAR(`date`), SUM(total_laid_off)
  FROM layoffs_staging2
  GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS 
  (SELECT *,
  DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
  FROM Company_Year
  WHERE years IS NOT NULL
  )
  SELECT *
  FROM Company_Year_Rank
  WHERE Ranking <= 5;
  

-- Create CTE named Rolling_Total
-- Withing the CTE
--    Select year and month as well as the sum of the total layoffs for each month
--    order the results in ASC
-- then query the CTE 
--    Calculate the rolling total of layoffs, ordered by month in ascending order
WITH Rolling_Total AS 
(
SELECT SUBSTRING(date,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL 
GROUP BY SUBSTRING(date,1,7)
ORDER BY SUBSTRING(date,1,7) ASC
)
SELECT *
, SUM(total_off) OVER (ORDER BY `MONTH` ASC) AS rolling_total
FROM rolling_total
ORDER BY `MONTH` ASC;

 