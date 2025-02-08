> - Create an external table using the Yellow Taxi Trip Records.
> - Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).

<code>
CREATE OR REPLACE EXTERNAL TABLE `marine-base-449315-s5.zoomcamp2.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://marine-base-449315-s5-kestra-bucket-2/yellow_tripdata_2024-01.parquet', 
          'gs://marine-base-449315-s5-kestra-bucket-2/yellow_tripdata_2024-02.parquet',
          'gs://marine-base-449315-s5-kestra-bucket-2/yellow_tripdata_2024-03.parquet',
          'gs://marine-base-449315-s5-kestra-bucket-2/yellow_tripdata_2024-04.parquet',
          'gs://marine-base-449315-s5-kestra-bucket-2/yellow_tripdata_2024-05.parquet',
          'gs://marine-base-449315-s5-kestra-bucket-2/yellow_tripdata_2024-06.parquet']
);
</code>

<code>
CREATE OR REPLACE TABLE marine-base-449315-s5.zoomcamp2.yellow_tripdata_non_partitoned AS
SELECT * FROM marine-base-449315-s5.zoomcamp2.external_yellow_tripdata;
</code>

### Question 1: What is count of records for the 2024 Yellow Taxi Data?
Ans: 20,332,093

### Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
### What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
Ans: 0 MB for the External Table and 155.12 MB for the Materialized Table

<code>
select count(distinct(PULocationID)) from  `marine-base-449315-s5.zoomcamp2.external_yellow_tripdata`;

select count(distinct(PULocationID)) from  `marine-base-449315-s5.zoomcamp2.yellow_tripdata_non_partitoned`;

</code>

### Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
Ans: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

### Question 4: How many records have a fare_amount of 0?
Ans: 8,333

<code>
select count(1) from  `marine-base-449315-s5.zoomcamp2.yellow_tripdata_non_partitoned`
where fare_amount = 0;
</code>

### Question 5: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
Ans: Partition by tpep_dropoff_datetime and Cluster on VendorID

<code>
CREATE OR REPLACE TABLE `marine-base-449315-s5.zoomcamp2.yellow_tripdata_partitoned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `marine-base-449315-s5.zoomcamp2.yellow_tripdata_non_partitoned`
</code>

### Question 6:
### Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
### Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
### Choose the answer which most closely matches.
Ans: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

<code>
select distinct VendorID from  `marine-base-449315-s5.zoomcamp2.yellow_tripdata_non_partitoned`
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';

select distinct VendorID from  `marine-base-449315-s5.zoomcamp2.yellow_tripdata_partitoned_clustered`
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';
</code>

### Question 7: Where is the data stored in the External Table you created?
Ans: GCP Bucket

### Question 8: It is best practice in Big Query to always cluster your data:
Ans: False

### Question 9: No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
Ans: Estimates 0B on processing. This is becuase when using '*' BigQuery prevents unnecessary data scanning by scanning from metadata, hence the given query would scan all rows and aggregate only on execution 