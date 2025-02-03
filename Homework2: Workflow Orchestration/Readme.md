1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?<br/>
**Ans: 128.3 MB**

1. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution? <br/>
**Ans: green_tripdata_2020-04.csv**

1. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020? <br/>
**Ans: 24,648,663** <br/>
<code>
select count(1) from `marine-base-449315-s5.zoomcamp.yellow_tripdata` where extract(YEAR from tpep_pickup_datetime) = 2020
</code>

1. How many rows are there for the Green Taxi data for all CSV files in the year 2020? <br/>
**Ans: 1,734,039** <br/>
<code>
select count(1) from `marine-base-449315-s5.zoomcamp.green_tripdata` where extract(YEAR from lpep_pickup_datetime) = 2020
</code>

1. How many rows are there for the Yellow Taxi data for the March 2021 CSV file? <br/>
**Ans: 1,925,130** <br/>
<code>
SELECT count(1) AS month_year
FROM `marine-base-449315-s5.zoomcamp.yellow_tripdata`
where FORMAT_TIMESTAMP('%Y-%m', tpep_pickup_datetime) = '2021-03'
</code>

1. How would you configure the timezone to New York in a Schedule trigger? <br/>
**Ans: Add a timezone property set to America/New_York in the Schedule trigger configuration**