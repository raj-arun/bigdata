/*Create new directory in HDFS*/
hdfs dfs -mkdir stockprice

/*Move the data file to DFS*/
hdfs dfs -put NYSE_daily_prices_Q.csv stockprice

#Type hive after moving the file to DFS.

/*Create a new Database*/
CREATE DATABASE stockprice;

/*Use this database to create the table and load the data*/
USE stockprice;

/*Create the table to load the data from the csv file*/
CREATE TABLE IF NOT EXISTS nyse (
    exchange_code String,
    stock_code String,
    stock_date String,
    opening_price Double,
    lowest_price Double,
    highest_price Double,
    closing_price Double,
    stock_volumne Double,
    adjusted_closing_price Double)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';

/*Describe the table to see the structure*/
DESCRIBE nyse;

/*Load the data into the table
Since the file is in DFS, the file will be removed after the data is loaded into the table*/
LOAD DATA INPATH 'stockprice/NYSE_daily_prices_Q.csv' INTO TABLE nyse;

/*Issue some select statements*/
SELECT stock_code, COUNT(*)
FROM nyse
GROUP BY stock_code;

/*Max and Average prices for each stock by year*/
SELECT stock_code, YEAR(stock_date), MAX(highest_price), AVG(highest_price)
FROM nyse
GROUP BY stock_code, YEAR(stock_date);
