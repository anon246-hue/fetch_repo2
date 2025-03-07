
import duckdb

def create_db():
    #Date dim
    query= """
    CREATE OR REPLACE TABLE dim_date as 
    WITH RECURSIVE date_series AS (
        SELECT DATE '2000-01-01' AS date_value -- Start date
        UNION ALL
        SELECT date_value + INTERVAL 1 DAY
        FROM date_series
        WHERE date_value < DATE '2050-12-31' -- End date
    )
    SELECT 
    CAST(STRFTIME('%Y%m%d', date_value) AS BIGINT) AS date_id,
        date_value AS date_id,
        date_value AS full_date,
        YEAR(date_value) AS year,
        QUARTER(date_value) AS quarter,
        MONTH(date_value) AS month,
        --LPAD(CAST(MONTH(date_value) AS STRING), 2, '0') AS month_padded,
        --FORMAT_TIMESTAMP('%B', date_value) AS month_name, -- Full month name (e.g., January)
        DAY(date_value) AS day_of_month,
        DAYOFWEEK(date_value) AS day_of_week, -- Sunday = 1, Saturday = 7
        CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS weekend_flag,
        --FORMAT_TIMESTAMP('%A', date_value) AS day_name, -- Full day name (e.g., Monday)
        WEEKOFYEAR(date_value) AS week_of_year,
        DAYOFYEAR(date_value) AS day_of_year,
        DATE_TRUNC('week', date_value) AS start_of_week,
        DATE_TRUNC('month', date_value) AS start_of_month,
        DATE_TRUNC('quarter', date_value) AS start_of_quarter,
        DATE_TRUNC('year', date_value) AS start_of_year
    FROM date_series
    """
    duckdb.sql(query)
    print('Created Dim Date')



    ##Users
    query= """
    CREATE OR REPLACE TABLE users_raw as 
    SELECT 
        *
    FROM
        '..\\data\\users.json'
    """
    duckdb.sql(query)
    print('Create users_raw')


    query= """
    create or replace table dim_users (
        user_id VARCHAR PRIMARY KEY
        ,created_date_id INTEGER
        ,is_active boolean
        ,state varchar
    )
    """
    duckdb.sql(query)


    query= """
    insert into dim_users
    SELECT DISTINCT
        json_extract_string(_id, '$.$oid') as user_id
        ,CAST(STRFTIME('%Y%m%d',epoch_ms(CAST(json_value(createddate, '$.$date')AS BIGINT))) AS BIGINT) AS created_date_id
        ,active as is_active
        ,state as state
    from users_raw
    """
    duckdb.sql(query)
    print('created dim_users')


    #Brands
    query= """
    CREATE OR REPLACE TABLE brands_raw as 
    SELECT 
        *
    FROM
        '..\\data\\brands.json'
    """
    duckdb.sql(query)
    print('created brands_raw')

    query= """
    create or replace table dim_brands (
        brand_id VARCHAR PRIMARY KEY
        ,brand_name varchar
        ,brand_code varchar
        ,category varchar
        ,barcode varchar
    )
    """
    duckdb.sql(query)


    query= """
    insert into dim_brands 
    SELECT 
        json_extract_string(_id, '$.$oid') as brand_id
        ,name as brand_name
        ,brandCode as brand_code
        ,category
        ,barcode
    FROM
        brands_raw
    """
    duckdb.sql(query)
    print('created dim_brands')

    #Receipts
    query= """
    CREATE OR REPLACE TABLE receipts_raw as 
    SELECT 
        *
    FROM
        '..\\data\\receipts.json'
    """
    duckdb.sql(query)
    print('created receipts_raw')

    query= """
    create or replace table receipts (
        receipt_id VARCHAR PRIMARY KEY
        ,user_id varchar
        ,status varchar
        ,date_scanned integer
        ,purchase_date integer
        ,points_awarded_date integer
        ,total_spent float
        ,purchased_item_count integer
        ,points_earned float
        ,bonus_points_earned float
    )
    """
    duckdb.sql(query)


    query= """
    create or replace table receipt_line_item (
        receipt_id varchar
        ,brand_code varchar
        ,barcode varchar
        ,user_id varchar
        ,status varchar
        ,date_scanned integer
        ,description varchar
        ,points_earned float
        ,item_price float
    )
    """
    duckdb.sql(query)


    query= """
    INSERT INTO receipts
    SELECT
        json_extract_string(_id, '$.$oid') as receipt_id
        ,userid as user_id
        ,rewardsReceiptStatus as status
        ,CAST(STRFTIME('%Y%m%d',epoch_ms(CAST(json_value(datescanned, '$.$date')AS BIGINT))) AS BIGINT) AS date_scanned
        ,CAST(STRFTIME('%Y%m%d',epoch_ms(CAST(json_value(purchaseDate, '$.$date')AS BIGINT))) AS BIGINT) AS purchase_date
        ,CAST(STRFTIME('%Y%m%d',epoch_ms(CAST(json_value(pointsAwardedDate, '$.$date')AS BIGINT))) AS BIGINT) AS points_awarded_date
        ,totalSpent as total_spent
        ,purchasedItemCount as purchased_item_count
        ,pointsEarned as points_earned
        ,bonusPointsEarned as bonus_points_earned
    FROM
        receipts_raw
    """
    duckdb.sql(query)
    print('created receipts')

    query= """
    INSERT INTO receipt_line_item
    with t1 as (
        SELECT
            json_extract_string(_id, '$.$oid') as receipt_id
            ,unnest(rewardsReceiptItemList) as line_item
            ,*
        FROM
            receipts_raw
    )
    SELECT 
        receipt_id
        ,json_extract_string(line_item, 'brandCode') as brand_code
        ,json_extract_string(line_item, 'barcode') as barcode
        ,userid as user_id
        ,rewardsReceiptStatus as status
        ,CAST(STRFTIME('%Y%m%d',epoch_ms(CAST(json_value(datescanned, '$.$date')AS BIGINT))) AS BIGINT) AS date_scanned
        ,json_extract_string(line_item, 'description') as description
        ,json_extract_string(line_item, 'pointsEarned') as points_earned
        ,json_extract_string(line_item, 'itemPrice') as item_price
    FROM t1
    """
    duckdb.sql(query)
    print('created receipt_line_item')

    print('Fetch DWH ready for analytics!!!')