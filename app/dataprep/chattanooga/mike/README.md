# APC Processing

## 1. Setup

* download data.zip and unzip in current directory (raw APC and GTFS data): https://vanderbilt365.sharepoint.com/:u:/r/sites/TransitHub/Shared%20Documents/General/datasets/carta-occupancy/app-data/data.zip?csf=1&web=1&e=qykqZ5
* download cartaapc_dashboard.csv.zip and place into output/ directory: https://vanderbilt365.sharepoint.com/:u:/r/sites/TransitHub/Shared%20Documents/General/datasets/carta-occupancy/app-data/cartaapc_dashboard.csv.zip?csf=1&web=1&e=MoYHmi

Note that cartaapc_dashboard.csv is the cleaned apc data for CARTA from Jan 1, 2019 to Jun 1, 2020

carta_apc_dataprep.ipynb is the notebook for cleaning the raw APC data and merging with GTFS. 

Notes:
the data/ directory holds all raw data (APC and GTFS) required by carta_apc_dataprep.ipynb. The
output/ directory is where carta_apc_dataprep.ipynb writes the processed files.

## 2. Data schema for processed dataset (cartaapc_dashboard.csv)

| Field | Source | Description |
|:-----:|:------:|:-----------:|
| trip_id  | APC, GTFS | unique trip identifier |
| arrival_time | APC | time when vehicle arrived at stop {hour}:{min}:{sec} |
| stop_id | APC, GTFS | unique stop identifier |
| stop_sequence | GTFS | each trip has an ordered sequence of stops visited, this is the sequence # of stop: stop_id in trip: trip_id |
| stop_lat | GTFS | latitude of this pickup location (stop) |
| stop_lon | GTFS | longitude of this pickup location (stop) |
| route_id | GTFS | unique route identifier |
| direction_id | GTFS | direction of travel along this route. 0 is outbound, 1 is inbound |
| board_count | APC | number of passengers boarding at this stop |
| alight_count | APC | number of passengers exiting vehicle at this stop |
| occupancy | APC | number of passengers on vehicle after vehicle leaves this stop ( occupancy = occupancy + board_count - alight_count) |
| direction_desc | APC | same as direction_id but in string format. Should be either OUTBOUND or INBOUND. |
| service_period | APC | either Weekday or Weekend |
| date | APC | date of vehicle arriving at this stop {year}-{month}-{day}|
| date_time | APC | date + time of vehicle arriving at this stop {year}-{month}-{day} {hour}:{min}:{sec} |
| trip_start_time | APC | represents the time at which this trip started. should be the same for all stops visited for a given trip |
| day_of_week | APC | the day of the week (0, 1, .... 6) |
| trip_date | APC | similar to trip_start_time, this is the date at which this trip started. Note that CARTA operates trips that cross over midnight, so trip_date can be different than 'date' field |
| hour | APC | hour of day (0, 1 ... 23) |


## 3. Reference documentation
Note that a good reference for GTFS is: https://developers.google.com/transit/gtfs/reference