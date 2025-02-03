### Question 1: Understanding docker first run
Solution - Docker file

### Question 2: Understanding Docker networking and docker-compose
postgres:5433

### Question 3: Trip Segmentation Count
- Run postgres container, load data into postgres
```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v "$(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data" \
  -p 5433:5432 \
  --network=pg-network \
  --name=pgdatabase
  postgres:16
```
- Load taxi data into postgres
```
URL="wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
$ python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5440 \
    --db=ny_taxi \
    --table_name=green_2019 \
    --url=${URL}

URL2="wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
$ python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5440 \
    --db=ny_taxi \
    --table_name=zone_2019 \
    --url=${URL2}
  ```

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

Up to 1 mile
In between 1 (exclusive) and 3 miles (inclusive),
In between 3 (exclusive) and 7 miles (inclusive),
In between 7 (exclusive) and 10 miles (inclusive),
Over 10 miles
```
select 
	count(1)
from green_2019 g 
where lpep_pickup_datetime::date between '2019-10-01' and '2019-10-31'
and trip_distance between 0 and 1
;

select 
	count(1)
from green_2019 g 
where lpep_pickup_datetime::date between '2019-10-01' and '2019-10-31'
and trip_distance > 1
and trip_distance between 1 and 3
;

select 
	count(1)
from green_2019 g 
where lpep_pickup_datetime::date between '2019-10-01' and '2019-10-31'
and trip_distance > 3
and trip_distance between 3 and 7
;

select 
	count(1)
from green_2019 g 
where lpep_pickup_datetime::date between '2019-10-01' and '2019-10-31'
and trip_distance > 7
and trip_distance between 7 and 10
;

select 
	count(1)
from green_2019 g 
where lpep_pickup_datetime::date between '2019-10-01' and '2019-10-31'
and trip_distance > 10
;
```

### Question 4. Longest trip for each day
```
select lpep_pickup_datetime::date from green_2019 g 
where trip_distance =(select max(trip_distance) from green_2019 g2)
```

### Question 5. Three biggest pickup zones
```
select 
	z."Zone" 
from green_2019 g 
join zone_2019 z on g."PULocationID" = z."LocationID" 
where g.lpep_pickup_datetime::date = '2019-10-18'
group by z."Zone" 
having sum(total_amount) > 13000
```

### Question 6. Largest tip
```
select 
	z."Zone" "Drop Off Zone"
from green_2019 g 
join zone_2019 z on g."DOLocationID" = z."LocationID" 
and tip_amount = (
	select max(tip_amount) from green_2019 g 
	join zone_2019 z on g."PULocationID" = z."LocationID" 
	where lpep_dropoff_datetime::date between '2019-10-01'and '2019-10-31'
	and z."Zone" = 'East Harlem North'
)
```