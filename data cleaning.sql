select * 
from layoffs;

-- 1. Remove duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove any columns 


-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
Create Table layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select*
from layoffs;



-- 1. Remove Duplicates

--First let's check for duplicates
select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num>1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially


select * 
from layoffs_staging
where company='Casper';

-- Create the table layoffs_staging2 with the duplicate_cte results and delete the rows where row_num >1

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

select * 
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) as row_num
from layoffs_staging;

-- search for the duplicates and delete them
select * 
from layoffs_staging2 
where row_num>1;

delete
from layoffs_staging2
where row_num > 1;



-- 2. Standardize Data
select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company=trim(company);

select distinct industry
from layoffs_staging2;


-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
update layoffs_staging2
set industry ='Crypto'
where industry like 'Crypto%';


-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
select distinct country
from layoffs_staging2
where country like 'United States%';

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;


update layoffs_staging2
set country=trim(trailing '.' from country)
where country like 'United States%';

-- Let's also fix the date columns:
select `date`
from layoffs_staging2;

-- we can use str to date to update this field
update layoffs_staging2
set `date`=str_to_date(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
alter table layoffs_staging2
modify column `date`  date;


-- 3. Look at Null Values
select *
from layoffs_staging2
where industry is null
or industry= '';

select * 
from layoffs_staging2
where company='Airbnb' ;

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all


select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
    and t1.location=t2.location
    where (t1.industry is null or t2.industry='')
    and t2.industry is not null;
    
   -- we should set the blanks to nulls since those are typically easier to work with
    update layoffs_staging2
    set industry =null
    where industry='';
    
    -- now we need to populate those nulls if possible
        update layoffs_staging2 t1
    join layoffs_staging2 t2
    on t1.company=t2.company
    set t1.industry=t2.industry
  where t1.industry is null 
    and t2.industry is not null;
    
-- 4. remove any columns and rows we need to
select * 
from layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null;
-- Delete Useless data we can't really use
delete
from layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2 
drop column row_num;

select * 
from layoffs_staging2;

