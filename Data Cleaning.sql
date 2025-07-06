# data cleaning

SELECT* 
FROM layoffs;

# remove duplicates
# standardize the data
# Null Values or blank values
# remove any columns or rows

CREATE TABLE layoffs_staging           #creates columns like that of selcted tables
LIKE layoffs;

select*   
FROM layoffs_staging;

INSERT layoffs_staging
select*
from layoffs;

select*
from layoffs_staging;

# if row_num is greater than 1, means there is a duplicate

select *     # searching duplicate rows by subqueries
from ( 
	select *,row_number() OVER(
	partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
	from layoffs_staging
    ) as rowTable
where row_num>1
;

with duplicate_cte as    # searching duplicate rows by cte's
(
	select*,row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
    from layoffs_staging
)
select *
from duplicate_cte
where row_num >1;

with duplicate_cte as    # searching duplicate rows by cte's
(
	select*,row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
    from layoffs_staging
)
delete
from duplicate_cte
where row_num >1;

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select*
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() OVER(partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num>1
;

select*
from layoffs_staging2
where row_num>1
;

# Standardising data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select*
from layoffs_staging2
where industry like 'crypto%';

# as crypto and crypto currency are same industries so they need to have same industry name

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

select* #updated industry names
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set country = 'United States'
where industry like 'United States%';

# another way of updating country name ( United States and United States. are same)
-- UPDATE layoffs_staging2
-- set country = TRIM(trailing '.' from country)
-- where country like 'United States%'; 

select* #updated county name
from layoffs_staging2
where country like 'United States%';

select `date`,
str_to_date(`date`,'%m/%d/%Y') as str_date
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_staging2   #initially date was in text format. performs operations on columns
modify column `date` DATE;     #changing it to date format


# taking care of null rows
select *
from layoffs_staging2
-- where percentage_laid_off is null and total_laid_off is null
where industry is null or industry =''
;

select *
from layoffs_staging2
where company='airbnb'
;

update layoffs_staging2
set industry = null
where industry='';


select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

# giving null spaces values as they had in other columns of same company name. 
update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null
;

select *
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null
;

delete
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null
;

alter table layoffs_staging2
drop column row_num
;

#cleaned data
select*
from layoffs_staging2
;