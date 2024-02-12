
# Setup

## Step 1:
- Use terraform to create Cloud Storage and BigQuery dataset resource in GCP
```sh
# run terraform command in tf directory
terraform init
terraform plan
terraform apply
```

## Step 2:
- Download Green Taxi Trip Records Data for 2022 from [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and save to data folder
- Running `upload_to_gcs.py` to upload data to Google Cloud Storage

## Step 3: 
Create an external table using the Green Taxi Trip Records Data for 2022.
```sql
CREATE OR REPLACE EXTERNAL TABLE `maverick-413820.nyc_taxi_24.green_tripdata`
OPTIONS (
  format = 'Parquet',
  uris = ['gs://maverick-nyc-taxi-24/green/green_tripdata_2022-*.parquet']
);
```
## Step 4: 
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
```sql
CREATE OR REPLACE TABLE `maverick-413820.nyc_taxi_24.green_tripdata_nonpartitioned`
AS SELECT * FROM `maverick-413820.nyc_taxi_24.green_tripdata`;
```

# Quiz
## Question 1: 
- Question: What is count of records for the 2022 Green Taxi Data?
- Answer: 840,402
```sql
SELECT COUNT(1) FROM maverick-413820.nyc_taxi_24.green_tripdata
```

## Question 2:
- Question: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
- Answer: 0 MB for the External Table and 6.41MB for the Materialized Table

```sql
-- query on external table
SELECT COUNT(DISTINCT(PULocationID)) FROM `maverick-413820.nyc_taxi_24.green_tripdata`;
-- query on materialized atable
SELECT COUNT(DISTINCT(PULocationID)) FROM `maverick-413820.nyc_taxi_24.green_tripdata_nonpartitioned`;

```

## Question 3:
- Question: How many records have a fare_amount of 0?
- Answer: 1,622

```sql
SELECT COUNT(1) FROM `maverick-413820.nyc_taxi_24.green_tripdata`
  WHERE fare_amount = 0;
```

## Question 4:
- Question: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Answer: Partition by lpep_pickup_datetime and Cluster on PULocationID
- Explanation: 
	- Partition by lpep_pickup_datetime: effective for query that filter by date or date range as it allows BigQuery to scan only the relevant partitions
	- Cluster on PULocationID: further organize data within the parititon and effective for query that involves ordering rows by this column within each partition
```sql
CREATE OR REPLACE TABLE `maverick-413820.nyc_taxi_24.green_tripdata_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `maverick-413820.nyc_taxi_24.green_tripdata`
);
```

## Question 5:
- Question:  Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive).
Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
- Answer: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

```sql
-- query on materialized table
SELECT COUNT(DISTINCT(PULocationID)) 
  FROM `maverick-413820.nyc_taxi_24.green_tripdata_nonpartitioned`
  WHERE  DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'

-- query on partitioned and clustered table 
SELECT COUNT(DISTINCT(PULocationID)) 
  FROM `maverick-413820.nyc_taxi_24.green_tripdata_partitioned_clustered`
  WHERE  DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
```

## Question 6:
- Question: Where is the data stored in the External Table you created?
- Answer: GCP Bucket

## Question 7: 
- Question: It is best practice in Big Query to always cluster your data:
- Answer: False

## Question 8:
- Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
- Answer: 
	- bytes it estimate will be read: 0 B
	- why: it only accesses caches of precomputed results that were done in advance rather than performing live query
```sql
SELECT count(*) FROM `maverick-413820.nyc_taxi_24.green_tripdata_nonpartitioned`
```