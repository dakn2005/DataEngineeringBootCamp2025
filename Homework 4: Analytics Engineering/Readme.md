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

<code>
{% macro get_quarter(themonth) -%}
    case 
        when {{themonth}} between 1 and 3 then 'Q1'
        when {{themonth}} between 4 and 6 then 'Q2'
        when {{themonth}} between 7 and 9 then 'Q3'
        when {{themonth}} between 10 and 12 then 'Q1'
    end

{%- endmacro %}

select 
    *,
    {{ get_quarter(dbt_date.date_part('month', 'revenue_month')) }} ||
        '/' || {{ dbt_date.date_part('year', 'revenue_month') }} as qtrs
from {{ ref('dm_monthly_zone_revenue') }}


</code>