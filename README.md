# Practice Case 6:
Download the February 2021 data from TLC Trip Record website (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and use Pyspark to analyze and answer these questions below. Upload your scriptinto Github or Gdrive.

## Question
+ How many taxi trips were there on February 15?
+ Find the longest trip for each day!
+ Find Top 5 Most frequent `dispatching_base_num`!
+ Find Top 5 Most common location pairs (PUlocationID and DOlocationID)!


## Answer

### How many taxi trips were there on February 15?
```sql
taxi_trips_15_feb = spark.sql("""
with trips_15_feb as 
(SELECT
    *
FROM 
    fhv_trip
WHERE
    to_date(pickup_datetime) = '2021-02-15'
)
SELECT
    COUNT(1) as taxi_trips_15_feb
FROM 
    trips_15_feb
WHERE
    to_date(pickup_datetime) = '2021-02-15'
""").show()
```
![NO 1](https://user-images.githubusercontent.com/85284506/206878865-e07c8c15-b843-4ca3-bb2e-b5a48ee77e0c.jpg)

## Find the longest trip for each day!
```sql
taxi_longest_trips = spark.sql("""
                              SELECT
                                  pickup_datetime, dropoff_datetime,
                                  ROUND(((unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime))/3600),2) AS duration_in_hours
                              FROM fhv_trip
                              SORT BY
                                  duration_in_hours DESC
                              """)
```
![NO 2](https://user-images.githubusercontent.com/85284506/206878904-3b901d9c-fb7c-4df8-8d85-a568606492d5.jpg)

### Find Top 5 Most frequent dispatching_base_num!

```sql
most_dispatching_base_num = spark.sql("""
    SELECT 
          dispatching_base_num,
          COUNT(1) as amount
    FROM 
          fhv_trip
    GROUP BY
          1
    ORDER BY
          2 DESC
    LIMIT 
          5
""")
```

![NO 3](https://user-images.githubusercontent.com/85284506/206878925-8584461a-c1ad-4c4c-9f2f-3553058c8d03.jpg)

### Find Top 5 Most common location pairs (PUlocationID and DOlocationID)!
```sql
location_pairs = spark.sql("""
SELECT
    CONCAT(coalesce(PickUp_Zone, 'Unknown'), '/', coalesce(DropOff_Zone, 'Unknown')) AS zone_pair,
    COUNT(1) as total_count
FROM
    location_pairs
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT
    5
;
""")
```
![NO 4](https://user-images.githubusercontent.com/85284506/206878973-b74181a2-50cc-48e3-aa36-b154de7c6ee2.jpg)
