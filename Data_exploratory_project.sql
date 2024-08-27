select * from layoffs;

/*
1.remove duplicates  : row numbers and over()
2.standardize the data  :  trim and trailing '.' 
3.null values or blank values : remove rows that has null values or blank
4.remove any columns : remove unuseful columns like row_num column
*/
-- create temp table like original table
create table layoff_staging
like layoffs;

select * from layoff_staging;

-- copy values from temp table to original table
insert into layoff_staging
select * from layoffs;


select * from layoff_staging;

-- create row nums to know the duplicates of rows
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_rum
from layoff_staging;

-- create CTE to filter duplicates from table layoff_staging
with CTE_duplicate as
(
	  
	select *,
	row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
	from layoff_staging

)DELETE from CTE_duplicate
 where row_num > 1;

SET SQL_SAFE_UPDATES = 0; -- to solve safe update error appears when we delete rows

-- create new table
CREATE TABLE `layoff_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT * FROM layoff_staging3;

INSERT INTO layoff_staging3
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoff_staging;

SELECT * FROM layoff_staging3 WHERE row_num > 1;

delete from layoff_staging3 
where row_num > 1;

SELECT * FROM layoff_staging3 where row_num > 1;
-- done (duplicates removed)


-- fix problems: standardizing data
SELECT * FROM layoff_staging3 order by 1;

-- remove spaces
select company, trim(company)
from layoff_staging3;

update layoff_staging3
set company = trim(company);


select * from layoff_staging3;

select distinct location from layoff_staging3;

select distinct country from layoff_staging3 order by 1; -- order by 1 to order column alphabet

select distinct country, trim(trailing '.' from country) -- remove . from cells
from layoff_staging3 order by 1;

update layoff_staging3 
set country = trim(trailing '.' from country)
where country like 'United States%'; 

select distinct industry from layoff_staging3 order by 1;

update layoff_staging3 
set industry = 'Crypto'
where industry like 'Crypto%';

select * from layoff_staging3;


select date from layoff_staging3; -- date is text not date format

update layoff_staging3
set date = str_to_date(date, '%m/%d/%Y');

select date from layoff_staging3;-- still not date format

alter table layoff_staging3
modify column date DATE; -- TEXT CHANGED TO DATE FORMAT

select * from layoff_staging3
where total_laid_off is null
and percentage_laid_off is null;

delete from layoff_staging3 
where total_laid_off is null;

delete from layoff_staging3 
where percentage_laid_off is null;

alter table layoff_staging3
drop column row_num;

select * from layoff_staging3;

select * from layoff_staging3
where industry is null
or industry = '';


delete from layoff_staging3
where industry = '';

select * from layoff_staging3;

delete from layoff_staging3
where funds_raised_millions is null;

delete from layoff_staging3
where country = 'Israel';

select * from layoff_staging3 order by 1;


-- Data exploratory

select max(total_laid_off), max(percentage_laid_off)
from layoff_staging3;

select * from layoff_staging3
where percentage_laid_off = 1
order by funds_raised_millions desc;


select company, sum(total_laid_off)
from layoff_staging3
group by company 
order by 2 desc;


select min(date), max(date)
from layoff_staging3;


select date, sum(total_laid_off)
from layoff_staging3
group by date
order by 1 desc;


select month(date), sum(total_laid_off), sum(percentage_laid_off)
from layoff_staging3
group by month(date)
order by 1 desc;



select stage, sum(total_laid_off)
from layoff_staging3
group by stage
order by 2 desc;


with TOTAL_example as
(
	select month(date), sum(total_laid_off) from layoff_staging3
	group by month(date)
	order by 1 desc
)
select * from TOTAL_example;



select company, year(date) as Year, sum(total_laid_off) as sum_total
from layoff_staging3
group by company, year(date)
order by 3 asc;


with CTE_example as
(

	select company, year(date) as Year, sum(total_laid_off) as sum_total
	from layoff_staging3
	group by company, year(date)
	order by 3 asc

), company_year_rank as
(
	select *,
    dense_rank() over(partition by year order by sum_total desc) as ranking
    from CTE_example
)select * from company_year_rank
 where ranking > 5;


select * from layoff_staging3;

select avg(total_laid_off) as average_total
from layoff_staging3 
where company = 'Amazon';


select country, funds_raised_millions
from layoff_staging3 
where funds_raised_millions > 100
order by 2;


select * from layoff_staging3 
where country like 'U%';


SELECT company from layoff_staging3
where industry = 'Healthcare';



