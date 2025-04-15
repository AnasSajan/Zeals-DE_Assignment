--Find the total number of trips for each day:
SELECT DATE(start_time) as date, COUNT(*) as total_trips
FROM bikeshare.trips_external
GROUP BY date
ORDER BY date;

--Calculate the average trip duration for each day:
SELECT DATE(start_time) as date, AVG(duration_minutes) as avg_duration
FROM bikeshare.trips_external
GROUP BY date
ORDER BY date;

--Identify the top 5 stations with the highest number of trip starts:
SELECT start_station_name, COUNT(*) as trip_starts
FROM bikeshare.trips_external
GROUP BY start_station_name
ORDER BY trip_starts DESC
LIMIT 5;

--Find the average number of trips per hour of the day:
SELECT EXTRACT(HOUR FROM start_time) as hour, COUNT(*)/COUNT(DISTINCT DATE(start_time)) as avg_trips
FROM bikeshare.trips_external
GROUP BY hour
ORDER BY hour;

--Determine the most common trip route (start station to end station):
SELECT start_station_name, end_station_name, COUNT(*) as trips
FROM bikeshare.trips_external
GROUP BY start_station_name, end_station_name
ORDER BY trips DESC
LIMIT 1;

--Calculate the number of trips each month:
SELECT EXTRACT(YEAR FROM start_time) as year, EXTRACT(MONTH FROM start_time) as month, COUNT(*) as total_trips
FROM bikeshare.trips_external
GROUP BY year, month
ORDER BY year, month;


--Find the station with the longest average trip duration:
SELECT start_station_name, AVG(duration_minutes) as avg_duration
FROM bikeshare.trips_external
GROUP BY start_station_name
ORDER BY avg_duration DESC
LIMIT 1;


--Find the busiest hour of the day (most trips started):
SELECT EXTRACT(HOUR FROM start_time) as hour, COUNT(*) as total_trips
FROM bikeshare.trips_external
GROUP BY hour
ORDER BY total_trips DESC
LIMIT 1;

--Identify the day with the highest number of trips:
SELECT DATE(start_time) as date, COUNT(*) as total_trips
FROM bikeshare.trips_external
GROUP BY date
ORDER BY total_trips DESC
LIMIT 1;


