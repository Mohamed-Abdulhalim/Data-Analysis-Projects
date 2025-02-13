-- Data Cleaning

-- Select all data from the layoffs table to inspect the data
SELECT *
FROM layoffs;

-- 1. Remove Duplicates: Identifying rows that appear multiple times based on certain columns
-- 2. Standardize the Data: Ensuring consistency in formats (e.g., trimming spaces, standardizing industry names)
-- 3. Null Values or Blank Values: Handling missing or blank values
-- 4. Remove Any Columns or Rows: Deleting unnecessary or problematic data

-- Create a new staging table to mirror the structure of the existing layoffs table
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Select all data from the new layoffs_staging table (which is empty at this point)
SELECT *
FROM layoffs_staging;

-- Insert all data from the original layoffs table into the layoffs_staging table
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Add a row number to each row in layoffs_staging partitioned by certain columns (detecting duplicates)
SELECT *,
ROW_NUMBER() OVER(
    PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Common Table Expression (CTE) to identify duplicates by assigning row numbers based on multiple columns
WITH duplicate_CTE AS
(
    SELECT *,
    ROW_NUMBER() OVER(
        PARTITION BY company, location, industry,
        total_laid_off, percentage_laid_off, `date`,
        stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
-- Select the duplicates (row_num > 1) from the CTE
SELECT *
FROM duplicate_CTE
WHERE row_num > 1;

-- Another CTE for deleting the duplicate rows by using row_num > 1
WITH duplicate_CTE AS
(
    SELECT *,
    ROW_NUMBER() OVER(
        PARTITION BY company, location, industry,
        total_laid_off, percentage_laid_off, `date`,
        stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
-- Delete the duplicate rows from the CTE
DELETE
FROM duplicate_CTE
WHERE row_num > 1;

-- Create a second staging table (layoffs_staging2) with an extra column for row numbers
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

-- Inspect data from layoffs_staging2 where row_num > 1 (to identify duplicates)
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Insert data into layoffs_staging2 with row number included to detect duplicates
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
    PARTITION BY company, location,
    industry, total_laid_off, percentage_laid_off,
    `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

-- Remove duplicates in layoffs_staging2 by deleting rows with row_num > 1
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Inspect layoffs_staging2 after deletion of duplicates
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing data

-- Trim spaces from the company column (leading and trailing spaces)
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Update the layoffs_staging2 table to trim spaces from the company column
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Look for industries starting with 'Crypto' for standardization
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Update rows where the industry starts with 'Crypto' to have a standardized 'Crypto' value
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Clean up country names by trimming trailing periods (.) from country names
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- Update the country names where the country starts with 'United States' to remove trailing periods
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert the 'date' column from a string to a date format (e.g., 'MM/DD/YYYY')
SELECT `date`
FROM layoffs_staging2;

-- Update the 'date' column in layoffs_staging2 to match the correct date format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Alter the column type for 'date' to make it an actual DATE type column (to enforce proper date formatting)
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Check for rows where both total_laid_off and percentage_laid_off are NULL (indicating missing data)
SELECT *
FROM layoffs_staging2
WHERE total_laid_off Is NULL
AND percentage_laid_off IS NULL;

-- Look for rows where the industry column is NULL or an empty string
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Find rows where the company is 'Airbnb' to inspect specific company data
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Identify rows with missing industry data by joining with another row of the same company and location
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Update missing industry data (NULL or empty) by copying the industry value from another row of the same company and location
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL and t2.industry != '');

-- Inspect the company and industry columns to ensure all rows have valid industry data
SELECT company, industry
FROM layoffs_staging2
WHERE (industry IS NULL OR industry = '');

-- Look for companies that start with 'Bally' to filter a specific company
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- Look for rows where both total_laid_off and percentage_laid_off are NULL
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete rows from layoffs_staging2 where total_laid_off and percentage_laid_off are NULL
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Inspect the final data in layoffs_staging2 after cleanup
SELECT *
FROM layoffs_staging2;

-- Remove the row_num column from layoffs_staging2 since it is no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
