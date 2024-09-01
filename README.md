# SQL CASE STUDY  

## Schema and TABLE Creation

### Creating the Schema
```sql
CREATE SCHEMA data_mart;
SET search_path = data_mart;
```

### Creating The TABLE weekly_sales
```sql
DROP TABLE IF EXISTS data_mart.weekly_sales;
CREATE TABLE data_mart.weekly_sales (
  "week_date" VARCHAR(7),
  "region" VARCHAR(13),
  "platform" VARCHAR(7),
  "segment" VARCHAR(4),
  "customer_type" VARCHAR(8),
  "transactions" INTEGER,
  "sales" INTEGER
);
```

### Insert the data from the `Data_Mart_DATA` (which is Given Above)

## Data Cleansing for Weekly Sales Data

Data cleansing process for the `weekly_sales` table in the `data_mart` schema. The resulting cleansed data is saved in a new table named `clean_weekly_sales`. 

### Overview for data Cleansing

The SQL query performs several data transformation and cleansing steps:
1. **Convert `week_date` to DATE format**: Reformats the week date from a string format to a DATE data type.
2. **Add `week_number`**: Computes the week number based on the `week_date` value.
3. **Add `month_number`**: Extracts the calendar month number from the `week_date`.
4. **Add `calendar_year`**: Extracts the calendar year from the `week_date` (2018, 2019, or 2020).
5. **Add `age_band` column**: Maps the `segment` value to a new age band category.
6. **Add `demographic` column**: Maps the first letter of the `segment` value to a demographic category.
7. **Replace null string values**: Converts null or 'null' string values to 'unknown' for `segment`, `age_band`, and `demographic`.
8. **Calculate `avg_transaction`**: Computes the average transaction value by dividing `sales` by `transactions`, rounded to 2 decimal places.


### 1. SQL Query for Data Cleansing

```sql
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
```

## Data Exploration

### 2.a What day of the week is used for each week_date value?

```sql
SELECT DISTINCT DAYNAME(week_date) AS day_of_week
FROM data_mart.clean_weekly_sales;
```


### 2.b What range of week numbers are missing from the dataset?
```sql
WITH RECURSIVE date_range AS (
  SELECT MIN(week_date) AS first_date, MAX(week_date) AS last_date
  FROM data_mart.clean_weekly_sales
),
week_date_series AS (
  SELECT first_date AS week_date
  FROM date_range
  UNION ALL
  SELECT week_date + INTERVAL 1 WEEK
  FROM week_date_series, date_range
  WHERE week_date + INTERVAL 1 WEEK <= last_date
)
SELECT week_date
FROM week_date_series
WHERE week_date NOT IN (SELECT DISTINCT week_date FROM data_mart.clean_weekly_sales)
ORDER BY week_date;
```


### 2.c How many total transactions were there for each year in the dataset?
```sql
SELECT YEAR(week_date) AS year, SUM(transactions) AS total_transactions 
FROM data_mart.clean_weekly_sales
GROUP BY year
ORDER BY year;
```

### 2.d What is the total sales for each region for each month?
```sql
SELECT region, YEAR(week_date) AS year, MONTH(week_date) AS month, SUM(sales) AS total_sales
FROM data_mart.clean_weekly_sales
GROUP BY region, year, month
ORDER BY region, year, month;
```

### 2.e What is the total count of transactions for each platform?
```sql
SELECT platform, SUM(transactions) AS total_transactions
FROM data_mart.clean_weekly_sales
GROUP BY platform;
```

### 2.f What is the percentage of sales for Retail vs Shopify for each month?
```sql
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
```

### 2.g What is the percentage of sales by demographic for each year in the dataset?
```sql
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
```


### 2.h Which age_band and demographic values contribute the most to Retail sales?
```sql
SELECT demographic, age_band, SUM(sales) AS total_sales
FROM data_mart.clean_weekly_sales
WHERE platform = 'Retail'
GROUP BY demographic, age_band
ORDER BY total_sales DESC;
```

### 2.i Can we use the avg_transaction column to find the average transaction size for each year for Retail vs Shopify? If not - how would you calculate it instead?
```sql
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
```


### 3.1 What is the total sales for the 4 weeks before and after 2020-06-15? What is the growth or reduction rate in actual values and percentage of sales?
```sql
SELECT 
  SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END) AS sales_before,
  SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-07-06' THEN sales ELSE 0 END) AS sales_after,
  SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-07-06' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END) AS sales_change,
  ROUND((SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-07-06' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END)) / SUM(CASE WHEN week_date BETWEEN '2020-05-18' AND '2020-06-08' THEN sales ELSE 0 END) * 100, 2) AS percent_change
FROM data_mart.clean_weekly_sales;
```

### 3.2 What about the entire 12 weeks before and after?
```sql
SELECT 
  SUM(CASE WHEN week_date BETWEEN '2020-03-23' AND '2020-06-08' THEN sales ELSE 0 END) AS sales_before,
  SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-08-31' THEN sales ELSE 0 END) AS sales_after,
  SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-08-31' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2020-03-23' AND '2020-06-08' THEN sales ELSE 0 END) AS sales_change,
  ROUND((SUM(CASE WHEN week_date BETWEEN '2020-06-15' AND '2020-08-31' THEN sales ELSE 0 END) - SUM(CASE WHEN week_date BETWEEN '2020-03-23' AND '2020-06-08' THEN sales ELSE 0 END)) / SUM(CASE WHEN week_date BETWEEN '2020-03-23' AND '2020-06-08' THEN sales ELSE 0 END) * 100, 2) AS percent_change
FROM data_mart.clean_weekly_sales;
```

### 3.3 How do the sale metrics for these 2 periods before and after compare with the previous years in 2018 and 2019?
```sql
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
```
