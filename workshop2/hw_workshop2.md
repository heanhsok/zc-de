# Question 0:
- Q: What are the dropoff taxi zones at the latest dropoff times?
- A: Midtown Center

```sql
WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;
```
```
   taxi_zone    | latest_dropoff_time
----------------+---------------------
 Midtown Center | 2022-01-03 17:24:54
```

# Question 1:
- Q: From this MV, find the pair of taxi zones with the highest average trip time.
- A: Yorkville East, Steinway

```sql
with cte as (
	select b."zone" pu_zone, c."zone" do_zone, 
		extract(epoch from a.tpep_dropoff_datetime - a.tpep_pickup_datetime) duration
		from trip_data a
		join taxi_zone b
	        on a.pulocationid = b.location_id
	    join taxi_zone c
	    	on a.dolocationid = c.location_id
) select pu_zone, do_zone, 
	avg(duration) avg_duration,
	min(duration) min_duration,
	max(duration) max_duration
	from cte
	group by pu_zone, do_zone
	order by avg_duration desc
	limit 1;
```

```
    pu_zone     | do_zone  | avg_duration | min_duration | max_duration
----------------+----------+--------------+--------------+--------------
 Yorkville East | Steinway | 86373.000000 | 86373.000000 | 86373.000000
```

# Question 2:
- Q: Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.
- A: 1

```sql
with cte as (
	select b."zone" pu_zone, c."zone" do_zone, 
		extract(epoch from a.tpep_dropoff_datetime - a.tpep_pickup_datetime) duration
		from trip_data a
		join taxi_zone b
	        on a.pulocationid = b.location_id
	    join taxi_zone c
	    	on a.dolocationid = c.location_id
) select pu_zone, do_zone, 
	avg(duration) avg_duration,
	count(1)
	from cte
	group by pu_zone, do_zone
	order by avg_duration desc
	limit 1;
```
```
    pu_zone     | do_zone  | avg_duration | count
----------------+----------+--------------+-------
 Yorkville East | Steinway | 86373.000000 |     1
```

# Question 3
- Q: From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups? For example if the latest pickup time is 2020-01-01 12:00:00, then the query should return the top 3 busiest zones from 2020-01-01 11:00:00 to 2020-01-01 12:00:00.
- A: LaGuardia Airport, Lincoln Square East, JFK Airport

```sql
select b."zone", count(1) cnt from trip_data a
	join taxi_zone b 
		on a.pulocationid = b.location_id 
	where tpep_pickup_datetime > (
		select max(tpep_pickup_datetime) - interval '17 hours' as time_17h_before
		from trip_data
	)
	group by b."zone"
	order by cnt desc
	limit 3;
```
```
        zone         | cnt
---------------------+-----
 LaGuardia Airport   |  19
 Lincoln Square East |  17
 JFK Airport         |  17
```