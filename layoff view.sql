USE world_layoffs;

SELECT *
FROM layoff_staging3;

CREATE OR REPLACE VIEW layoff_cleaned AS 
SELECT company,
location,
total_laid_off,
date,
percentage_laid_off,
industry,
source,
stage,
funds_raised,
country,
date_added
FROM layoff_staging3
WHERE percentage_laid_off IS NOT NULL AND total_laid_off IS NOT NULL;