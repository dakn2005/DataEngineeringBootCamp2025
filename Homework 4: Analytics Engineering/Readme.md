> Setup

<code>
CREATE OR REPLACE EXTERNAL TABLE `marine-base-449315-s5.zoomcamp_dbt.external_yellow_tripdata_2019_2020`
OPTIONS (
  format = 'csv',
  uris = ['gs://marine-base-449315-s5-kestra-bucket/yellow_tripdata_2019-*.csv', 
          'gs://marine-base-449315-s5-kestra-bucket/yellow_tripdata_2020-*.csv'
        ]
);


CREATE OR REPLACE EXTERNAL TABLE `marine-base-449315-s5.zoomcamp_dbt.external_green_tripdata_2019_2020`
OPTIONS (
  format = 'csv',
  uris = ['gs://marine-base-449315-s5-kestra-bucket/green_tripdata_2019-*.csv', 
          'gs://marine-base-449315-s5-kestra-bucket/green_tripdata_2020-*.csv'
        ]
);

select count(1) from `marine-base-449315-s5.zoomcamp_dbt.external_green_tripdata_2019_2020`;
select count(1) from `marine-base-449315-s5.zoomcamp_dbt.external_yellow_tripdata_2019_2020`;
select count(1) from `marine-base-449315-s5.zoomcamp_dbt.external_fhv_2019`;
</code>

### Question 1: Understanding dbt model resolution
Ans: select * from myproject.raw_nyc_tripdata.ext_green_taxi

### Question 2: dbt Variables & Dynamic Models
Ans: Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

### Question 3: dbt Data Lineage and Execution
Ans: dbt run --select +models/core/dim_taxi_trips.sql+ --target prod

### Question 4: dbt Macros and Jinja
Ans:
- When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET
- When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET
- When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults DBT_BIGQUERY_TARGET_DATASET

### Question 5: Question 5: Taxi Quarterly Revenue Growth
<code>
{% macro get_quarter(themonth) %}

    case 
        when {{themonth}} between 1 and 3 then 'Q1'
        when {{themonth}} between 4 and 6 then 'Q2'
        when {{themonth}} between 7 and 9 then 'Q3'
        when {{themonth}} between 10 and 12 then 'Q1'
    end

{%- endmacro %}

-- stg_quarterly_revenue

select 
    service_type,
    total_amount,
    {{ get_quarter(dbt_date.date_part('month', 'pickup_datetime')) }} ||
        '/' || {{ dbt_date.date_part('year', 'pickup_datetime') }} as qtrs
from {{ ref('fact_trips') }}

-- fct_taxi_trips_quarterly_revenue

select 
    service_type,
    qtrs quarter,
    sum(total_amount) revenue
from {{ ref('stg_quarterly_revenue') }}
group by service_type, qtrs
order by 1, 2
</code>

**Calculate YoY**

<code>
with splitted_dt as (
  select 
    *,
    split(quarter, '/')[offset(0)] as qtr,
    split(quarter, '/')[1] as yr,
  from `zoomcamp_dbt.fct_taxi_trips_quarterly_revenue`
) 
select 
  sdt.service_type,
  sdt.quarter,
  qtr ||'/'|| (cast(yr as int64) - 1) prevq,
  sdt.revenue curr_rev,
  fct2.revenue prev_rev,
  SAFE_DIVIDE(sdt.revenue - fct2.revenue, fct2.revenue) * 100 AS yoy_growth_percentage
from splitted_dt sdt
left join `zoomcamp_dbt.fct_taxi_trips_quarterly_revenue` fct2 
on fct2.quarter =  sdt.qtr ||'/'|| (cast(sdt.yr as int64) - 1)
and fct2.service_type = sdt.service_type
</code>

Ans: green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}

### Question 6: P97/P95/P90 Taxi Monthly Fare
Ans: green: {p97: 28.0, p95: 23.0, p90: 18.0}, yellow: {p97: 32, p95: 26, p90: 19.5}

<code>
-- stg_fct_taxi_trips_monthly_fare_p95

select 
    service_type,
 {{ dbt_date.date_part('year', 'pickup_datetime') }} year,
 {{ dbt_date.date_part('month', 'pickup_datetime') }} month,
 fare_amount
from {{ ref('fact_trips') }}
where fare_amount > 0 
and trip_distance > 0
and payment_type_description in ('Cash', 'Credit Card')

-- fct_taxi_trips_monthly_fare_p95

SELECT 
    service_type, 
    year, 
    month, 
    PERCENTILE_CONT(fare_amount, 0.97) OVER (
        PARTITION BY service_type, year, month 
    ) AS percentile_97,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (
        PARTITION BY service_type, year, month 
    ) AS percentile_95, -- Median
    PERCENTILE_CONT(fare_amount, 0.90) OVER (
        PARTITION BY service_type, year, month 
    ) AS percentile_90
FROM {{ ref('stg_fct_taxi_trips_monthly_fare_p95') }}

-- result query
select 
  service_type,
  year,
  month,
  avg(percentile_97),
  avg(percentile_95),
  avg(percentile_90)
from `zoomcamp_dbt.fct_taxi_trips_monthly_fare_p95`
where year = 2020 
and month = 4
group by service_type, year, month
</code>

### Question 7: Top #Nth longest P90 travel time Location for FHV
Ans: LaGuardia Airport, Chinatown, Garment District

<code>
-- dim_fhv_trips

{{
    config(
        materialized='table'
    )
}}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
),
staged_fhv as (
    select * from {{ ref('stg_fhv') }}
)
select 
    staged_fhv.*,
    {{ dbt_date.date_part('year', 'pickup_datetime') }} year,
    {{ dbt_date.date_part('month', 'pickup_datetime') }} month,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
from 
staged_fhv
inner join dim_zones as pickup_zone on staged_fhv.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone on staged_fhv.dolocationid = dropoff_zone.locationid

-- fct_fhv_monthly_zone_traveltime_p90

with
    t1 as (
        select
            *,
            timestamp_diff(
                timestamp(dropoff_datetime), timestamp(pickup_datetime), second
            ) trip_duration
        from {{ ref("dim_fhv_trips") }}
    )
select
    year,
    month, 
    pickup_zone, 
    dropoff_zone,  
    percentile_cont(trip_duration, 0.90) over (
        partition by year, month, pulocationid, dolocationid
    ) as percentile_90
from t1


-- result query

with calcs as (
  select 
    pickup_zone,
    dropoff_zone,
    avg(percentile_90) p90
  from `zoomcamp_dbt.fct_fhv_monthly_zone_traveltime_p90`
  where pickup_zone = 'Yorkville East'
  and month = 11
  and year = 2019
  group by pickup_zone, dropoff_zone
)
select * from calcs 
order by pickup_zone, p90 desc
;
</code>
