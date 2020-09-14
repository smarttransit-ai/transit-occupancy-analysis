import dash
import flask
import os
from random import randint
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
import swifter
import plotly.express as px
import dataextract
from dash.dependencies import Input, Output
from plotly import graph_objs as go
from plotly.graph_objs import *
from datetime import datetime as dt
from datetime import time as tt
from datetime import timedelta 
import os,sys,resource
import dask.dataframe as dd
from fastparquet import ParquetFile
 
#resource.setrlimit(resource.RLIMIT_AS, (1e9, 1e9))  
# Data preparation Code. Uncomment as required

df = dataextract.decompress_pickle('nashville_bus_occupancy_dashboard-dubey.pbz2')
df['time_day_seconds']=df.swifter.apply(lambda row: row.timeofday.hour*60*60+row.timeofday.minute*60+row.timeofday.second, axis=1)
sdf = dd.from_pandas(df, npartitions=10)
custom= {'occupancy': 'max', 'board_count': 'sum', 'triptime': 'first','datetime':'first','time_day_seconds':'first','trip_start_time':'first','dayofweek':'first'}
max_occupancy_board_by_trip_day=sdf.groupby(['trip_id','route_id','direction_desc','month','year','date']).agg(custom).reset_index()
custom= {'occupancy': 'max', 'datetime':'first','board_count': 'sum','time_day_seconds':'first','trip_start_time':'first','dayofweek':'first'}
max_occupancy_board_by_stop_day=sdf.groupby(['route_id','trip_id','stop_id','direction_desc','stop_name','stop_lat','stop_lon','month','year','date']).agg(custom).reset_index()
max_occupancy_board_by_route_day=sdf.groupby(['route_id','direction_desc','month','year','date']).agg(custom).reset_index()
max_occupancy_board_by_trip_day.to_parquet('data/nashville/max_occupancy_board_by_trip_day.parquet', engine='fastparquet')
max_occupancy_board_by_stop_day.to_parquet('data/nashville/max_occupancy_board_by_stop_day.parquet', engine='fastparquet')
max_occupancy_board_by_route_day.to_parquet('data/nashville/max_occupancy_board_by_route_day.parquet', engine='fastparquet')
print(max_occupancy_board_by_trip_day.head())
print(max_occupancy_board_by_trip_day.dtypes)
print(max_occupancy_board_by_trip_day.datetime.min().compute())
sys.exit(0)