USE world_layoffs;

CREATE TABLE layoff_staging
LIKE layoffs;

SELECT *
FROM layoff_staging;

INSERT layoff_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, `date`) AS row_num
FROM layoff_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
`date`, stage, country, funds_raised) AS row_num
FROM layoff_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoff_staging
WHERE company = 'Oda';

SELECT *
FROM layoff_staging
WHERE company = 'Salesforce';

CREATE TABLE `layoff_staging3` (
  `company` text,
  `location` text,
  `total_laid_off` double DEFAULT NULL,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
   `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoff_staging3;

INSERT INTO layoff_staging3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, 
`date`, stage, country, funds_raised) AS row_num
FROM layoff_staging;

SELECT *
FROM layoff_staging3
WHERE row_num > 1;

DELETE
FROM layoff_staging3
WHERE row_num > 1;

SELECT *
FROM layoff_staging;

SELECT company, TRIM(company)
FROM layoff_staging;

UPDATE layoff_staging
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoff_staging3
ORDER BY 1;

SELECT DISTINCT location
FROM layoff_staging3;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoff_staging3;

UPDATE layoff_staging3
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT date
FROM layoff_staging3;

ALTER TABLE layoff_staging3
MODIFY column `date` DATE;

SELECT *
FROM layoff_staging3;

-- null/blanks
SELECT country
FROM layoff_staging3
WHERE country = '';

UPDATE layoff_staging3
SET country = NULL
WHERE country = '';

SELECT funds_raised
FROM layoff_staging3
WHERE funds_raised = '';

UPDATE layoff_staging3
SET funds_raised = NULL
WHERE funds_raised = '';

SELECT stage
FROM layoff_staging3
WHERE stage = '';

UPDATE layoff_staging3
SET stage = NULL
WHERE stage = '';

SELECT percentage_laid_off
FROM layoff_staging3
WHERE percentage_laid_off = '';

SELECT industry
FROM layoff_staging3
WHERE industry = '';

UPDATE layoff_staging3
SET industry = NULL
WHERE industry = '';

UPDATE layoff_staging3
SET percentage_laid_off = NULL
WHERE percentage_laid_off = '';

DELETE
FROM layoff_staging3
WHERE country IS NULL;

DELETE
FROM layoff_staging3
WHERE stage IS NULL;

DELETE
FROM layoff_staging3
WHERE industry IS NULL;

SELECT *
FROM layoff_staging3;

DELETE
FROM layoff_staging3
WHERE percentage_laid_off IS NULL;

DELETE
FROM layoff_staging3
WHERE funds_raised IS NULL;

SELECT *
FROM layoff_staging3;