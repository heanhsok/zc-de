## Q1: 
Answer: --rm
```sh
docker run --help
```


## 2: 
Answer: 0.42.0
```sh
docker run -it python:3.9 bash
pip list
```


## Q3: 
Answer: 15767

```sql
SELECT COUNT(1) FROM yellow_taxi_trips 
	WHERE DATE_PART('year', lpep_pickup_datetime) = 2019
		AND DATE_PART('month', lpep_pickup_datetime) = 9
		AND DATE_PART('day', lpep_pickup_datetime) = 18
```

## Q4: 
Answer: "2019-09-26"

```sql
SELECT * 
	FROM yellow_taxi_trips
	ORDER BY trip_distance DESC
```

## Q5:
Answer: "Brooklyn", "Queens", "Manhattan"

```sql
	SELECT "Borough", sum(total_amount)
	FROM yellow_taxi_trips AS t1
		LEFT JOIN zones AS t2
			ON t1."PULocationID" = t2."LocationID"
	GROUP BY "Borough"
	ORDER BY sum(total_amount) DESC
```

## Q6: 
Answer: JFK Airport

```sql
	SELECT 
			t1."PULocationID", t2."Zone" AS PUZone, t1."DOLocationID",  
			t3."Zone" AS DOZone,
			tip_amount
		FROM yellow_taxi_trips AS t1
			LEFT JOIN zones AS t2
				ON t1."PULocationID" = t2."LocationID"
			LEFT JOIN zones AS t3
				ON t1."DOLocationID" = t3."LocationID"
		WHERE t2."Zone" = 'Astoria' AND 
			DATE_PART('year', lpep_pickup_datetime) = 2019
			AND DATE_PART('month', lpep_pickup_datetime) = 9
		ORDER BY tip_amount DESC
```


## Q7:
Answer:
```bash
google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/shumd-404322/datasets/demo_dataset]
google_storage_bucket.demo-bucket: Creation complete after 2s [id=shumd-404322-terra-bucket]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```

```sh
terraform plan
terraform apply
terraform destroy
```