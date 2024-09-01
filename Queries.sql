/*
1. Data Cleansing Steps

In a single query, perform the following operations and generate a new table in the data_mart schema named clean_weekly_sales:
1. Convert the week_date to a DATE format

2. Add a week_number as the second column for each week_date value, for example any value from the 1st of January to 7th of January will be 1, 8th to 14th will be 2 etc

3. Add a month_number with the calendar month for each week_date value as the 3rd column

4. Add a calendar_year column as the 4th column containing either 2018, 2019 or 2020 values

5. Add a new column called age_band after the original segment column using the following mapping on the number inside the segment value

segment	age_band
1	Young Adults
2	Middle Aged
3 or 4	Retirees

6. Add a new demographic column using the following mapping for the first letter in the segment values:
segment	demographic
C	Couples
F	Families

7. Ensure all null string values with an "unknown" string value in the original segment column as well as the new age_band and demographic columns

8. Generate a new avg_transaction column as the sales value divided by transactions rounded to 2 decimal places for each record
*/

-- Data Cleansing Steps
USE data_mart;
CREATE TABLE data_mart.clean_weekly_sales AS 
SELECT 
  STR_TO_DATE(CONCAT('20', SUBSTRING_INDEX(week_date, '/', -1), '-', SUBSTRING_INDEX(SUBSTRING_INDEX(week_date, '/', 2), '/', -1), '-', SUBSTRING_INDEX(week_date, '/', 1)), '%Y-%m-%d') AS week_date,
  WEEK(STR_TO_DATE(CONCAT('20', SUBSTRING_INDEX(week_date, '/', -1), '-', SUBSTRING_INDEX(SUBSTRING_INDEX(week_date, '/', 2), '/', -1), '-', SUBSTRING_INDEX(week_date, '/', 1)), '%Y-%m-%d'), 3) AS week_number,
  MONTH(STR_TO_DATE(CONCAT('20', SUBSTRING_INDEX(week_date, '/', -1), '-', SUBSTRING_INDEX(SUBSTRING_INDEX(week_date, '/', 2), '/', -1), '-', SUBSTRING_INDEX(week_date, '/', 1)), '%Y-%m-%d')) AS month_number,
  YEAR(STR_TO_DATE(CONCAT('20', SUBSTRING_INDEX(week_date, '/', -1), '-', SUBSTRING_INDEX(SUBSTRING_INDEX(week_date, '/', 2), '/', -1), '-', SUBSTRING_INDEX(week_date, '/', 1)), '%Y-%m-%d')) AS calendar_year,
  region,
  platform,
  CASE 
    WHEN segment = 'null' OR segment IS NULL THEN 'unknown'
    ELSE segment END AS segment,
  CASE 
    WHEN RIGHT(segment, 1) = '1' THEN 'Young Adults'
    WHEN RIGHT(segment, 1) = '2' THEN 'Middle Aged'
    WHEN RIGHT(segment, 1) IN ('3', '4') THEN 'Retirees'
    ELSE 'unknown' END AS age_band,
  CASE 
    WHEN LEFT(segment, 1) = 'C' THEN 'Couples'
    WHEN LEFT(segment, 1) = 'F' THEN 'Families'
    ELSE 'unknown' END AS demographic,
  customer_type,
  transactions,
  sales,
  ROUND(sales / transactions, 2) AS avg_transaction
FROM data_mart.weekly_sales;

-- Data Exploration

-- 2.a What day of the week is used for each week_date value?
SELECT DISTINCT DAYNAME(week_date) AS day_of_week
FROM data_mart.clean_weekly_sales;

-- 2.b What range of week numbers are missing from the dataset?
WITH RECURSIVE date_range AS (
  SELECT MIN(week_date) AS first_date, MAX(week_date) AS last_date FROM data_mart.clean_weekly_sales
),
week_date_series AS (
  SELECT first_date AS week_date FROM date_range
  UNION ALL
  SELECT week_date + INTERVAL 1 WEEK FROM week_date_series, date_range
  WHERE week_date + INTERVAL 1 WEEK <= last_date
)
SELECT week_date
FROM week_date_series
WHERE week_date NOT IN (SELECT DISTINCT week_date FROM data_mart.clean_weekly_sales)
ORDER BY week_date;

-- 2.c How many total transactions were there for each year in the dataset?
SELECT YEAR(week_date) AS year, SUM(transactions) AS total_transactions 
FROM data_mart.clean_weekly_sales
GROUP BY year
ORDER BY year;

-- 2.d What is the total sales for each region for each month?
SELECT region, YEAR(week_date) AS year, MONTH(week_date) AS month, SUM(sales) AS total_sales
FROM data_mart.clean_weekly_sales
GROUP BY region, year, month
ORDER BY region, year, month;

-- 2.e What is the total count of transactions for each platform?
SELECT platform, SUM(transactions) AS total_transactions
FROM data_mart.clean_weekly_sales
GROUP BY platform;

-- 2.f What is the percentage of sales for Retail vs Shopify for each month?
SELECT
  YEAR(week_date) AS year,
  MONTH(week_date) AS month,
  SUM(CASE WHEN platform = 'Shopify' THEN sales ELSE 0 END) AS shopify_sales,
  SUM(CASE WHEN platform = 'Retail' THEN sales ELSE 0 END) AS retail_sales,
  SUM(sales) AS total_sales,
  ROUND(SUM(CASE WHEN platform = 'Shopify' THEN sales ELSE 0 END) / SUM(sales) * 100, 2) AS shopify_percent,
  ROUND(SUM(CASE WHEN platform = 'Retail' THEN sales ELSE 0 END) / SUM(sales) * 100, 2) AS retail_percent
FROM data_mart.clean_weekly_sales
GROUP BY year, month
ORDER BY year, month;

-- 2.g What is the percentage of sales by demographic for each year in the dataset?
SELECT
  sub.year,
  sub.demographic,
  sub.total_sales,
  ROUND((sub.total_sales / yearly_totals.total_sales) * 100, 2) AS sales_percent
FROM (
  SELECT 
    YEAR(week_date) AS year,
    demographic,
    SUM(sales) AS total_sales
  FROM data_mart.clean_weekly_sales
  GROUP BY YEAR(week_date), demographic
) AS sub
JOIN (
  SELECT 
    YEAR(week_date) AS year,
    SUM(sales) AS total_sales
  FROM data_mart.clean_weekly_sales
  GROUP BY YEAR(week_date)
) AS yearly_totals
ON sub.year = yearly_totals.year
ORDER BY sub.year, sales_percent DESC;


-- 2.h Which age_band and demographic values contribute the most to Retail sales?
SELECT demographic, age_band, SUM(sales) AS total_sales
FROM data_mart.clean_weekly_sales
WHERE platform = 'Retail'
GROUP BY demographic, age_band
ORDER BY total_sales DESC;

-- 2.i Can we use the avg_transaction column to find the average transaction size for each year for Retail vs Shopify? If not - how would you calculate it instead?
-- We can't use avg_transaction directly. Instead, we calculate it as total sales divided by total transactions for each year and platform.
SELECT
  YEAR(week_date) AS year,
  platform,
  SUM(sales) AS total_sales,
  SUM(transactions) AS total_transactions,
  ROUND(SUM(sales) / SUM(transactions), 2) AS avg_transaction_size
FROM data_mart.clean_weekly_sales
GROUP BY year, platform
ORDER BY year, platform;

-- Before & After Analysis

-- 3.1 What is the total sales for the 4 weeks before and after 2020-06-15? What is the growth or reduction rate in actual values and percentage of sales?
SELECT 
  SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END) AS sales_before,
  SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-07-06' THEN sales ELSE 0 END) AS sales_after,
  SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-07-06' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END) AS sales_change,
  ROUND((SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-07-06' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END)) / SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END) * 100, 2) AS percent_change
FROM data_mart.clean_weekly_sales;

-- 3.2 What about the entire 12 weeks before and after?
SELECT 
  SUM(CASE WHEN week_date BETWEEN '2020-03-23' AND '2020-06-08' THEN sales ELSE 0 END) AS sales_before,
  SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-08-31' THEN sales ELSE 0 END) AS sales_after,
  SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-08-31' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2020-03-23' AND '2020-06-08' THEN sales ELSE 0 END) AS sales_change,
  ROUND((SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-08-31' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2020-03-23' AND '2020-06-08' THEN sales ELSE 0 END)) / SUM(CASE WHEN week_date BETWEEN '2020-03-23' AND '2020-06-08' THEN sales ELSE 0 END) * 100, 2) AS percent_change
FROM data_mart.clean_weekly_sales;

-- 3.3 How do the sale metrics for these 2 periods before and after compare with the previous years in 2018 and 2019?
SELECT
  '2018' AS year,
  SUM(CASE WHEN week_date BETWEEN '2018-05-18' AND '2018-06-08' THEN sales ELSE 0 END) AS sales_before,
  SUM(CASE WHEN week_date BETWEEN '2018-06-15' AND '2018-07-06' THEN sales ELSE 0 END) AS sales_after,
  SUM(CASE WHEN week_date BETWEEN '2018-06-15' AND '2018-07-06' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2018-05-18' AND '2018-06-08' THEN sales ELSE 0 END) AS sales_change,
  ROUND((SUM(CASE WHEN week_date BETWEEN '2018-06-15' AND '2018-07-06' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2018-05-18' AND '2018-06-08' THEN sales ELSE 0 END)) / SUM(CASE WHEN week_date BETWEEN '2018-05-18' AND '2018-06-08' THEN sales ELSE 0 END) * 100, 2) AS percent_change
FROM data_mart.clean_weekly_sales WHERE YEAR(week_date) = 2018

UNION ALL

SELECT
  '2019' AS year,
  SUM(CASE WHEN week_date BETWEEN '2019-05-18' AND '2019-06-08' THEN sales ELSE 0 END) AS sales_before,
  SUM(CASE WHEN week_date BETWEEN '2019-06-15' AND '2019-07-06' THEN sales ELSE 0 END) AS sales_after,
  SUM(CASE WHEN week_date BETWEEN '2019-06-15' AND '2019-07-06' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2019-05-18' AND '2019-06-08' THEN sales ELSE 0 END) AS sales_change,
  ROUND((SUM(CASE WHEN week_date BETWEEN '2019-06-15' AND '2019-07-06' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2019-05-18' AND '2019-06-08' THEN sales ELSE 0 END)) / SUM(CASE WHEN week_date BETWEEN '2019-05-18' AND '2019-06-08' THEN sales ELSE 0 END) * 100, 2) AS percent_change
FROM data_mart.clean_weekly_sales WHERE YEAR(week_date) = 2019

UNION ALL

SELECT
  '2020' AS year,
  SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END) AS sales_before,
  SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-07-06' THEN sales ELSE 0 END) AS sales_after,
  SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-07-06' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END) AS sales_change,
  ROUND((SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-07-06' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END)) / SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END) * 100, 2) AS percent_change
FROM data_mart.clean_weekly_sales WHERE YEAR(week_date) = 2020;
