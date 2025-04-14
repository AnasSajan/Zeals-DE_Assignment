SELECT
    trip_id,
    subscriber_type,
    bike_id,
    bike_type,
    DATETIME(start_time, 'Asia/Tokyo') as start_time,
    CAST(start_station_id AS STRING) AS start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    duration_minutes,
    DATETIME('#ingested_at#', 'Asia/Tokyo') as ingested_at
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
WHERE  start_time >= TIMESTAMP('#start_date#', 'Asia/Tokyo')
  and start_time < TIMESTAMP('#end_date#', 'Asia/Tokyo')