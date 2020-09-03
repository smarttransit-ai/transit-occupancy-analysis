import bz2
import pickle
import re

import pandas as pd
import zipfile
import os
from pathlib import Path


# These elements are best run from a Python console.

# Pickle a file and then compress it into a file with extension
def compress_pickle(title, data):
    with bz2.BZ2File(f'{title}.pbz2', 'w') as f:
        pickle.dump(data, f)


base_dir = 'C:\\Users\\fred\\PycharmProjects\\transit-occupancy-dashboard'
os.chdir(os.path.join(Path.home(), 'Data'))
os.getcwd()
os.chdir(base_dir)

# load data from a zip file

with zipfile.ZipFile('bus_occupancy_jan_through_jun.csv.zip', 'r') as zip_ref:
    zip_ref.extractall()

df = pd.read_csv('bus_occupancy_jan_through_jun.csv', index_col=0)
compress_pickle('bus_occupancy_jan_through_jun', df)

#
df = pd.read_csv('nashville_bus_occupancy_dashboard.csv', index_col=0)
compress_pickle('nashville_bus_occupancy_dashboard', df)

df = pd.read_csv('chattanooga_bus_occupancy_dashboard.csv', index_col=0)
compress_pickle('chattanooga_bus_occupancy_dashboard', df)


# Load any compressed pickle file
def decompress_pickle(file):
    data = bz2.BZ2File(f'{file}.pbz2', 'rb')
    data = pickle.load(data)
    return data

def compute_occupancy(df):

    dfs


df = decompress_pickle('chattanooga_bus_occupancy_dashboard')

# What does the data look like?
df.head()

# first let's check the column names. There are several ways.
df.keys()
sorted(df)

# We want to make sure the column names are reasonable.
# Let's look at the bus-line names.
df.line.unique()


# df = df.rename(columns={'line': 'raw_line'})

def repair_line(element):
    if isinstance(element, int):
        return str(element)
    if isinstance(element, str):
        match = re.match(r'^(\d+)(?:\.\d+)?$', element)
        if match is None:
            return element
        else:
            return match.group(1)
    if isinstance(element, float):
        return str(int(element))
    return 'Unknown'


df['line'] = df.apply(lambda row: repair_line(row['line']), axis=1)
df['trip_id'] = df.apply(lambda row: int(row['trip_id']), axis=1)
df['direction_id'] = df.apply(lambda row: int(row['direction_id']), axis=1)
df['stop_id'] = df.apply(lambda row: int(row['stop_id']), axis=1)
df['stop_sequence'] = df.apply(lambda row: int(row['stop_sequence']), axis=1)

df = df.dropna(axis='columns', subset=['date_time'])
df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')
df['arrival_date'] = df['date_time'].dt.date
df['arrival_time'] = df['date_time'].dt.time

df['net_count'] = df.apply(lambda row: row['board_count'] - row['alight_count'], axis='columns')
dfs = df.sort_values(by=['stop_sequence'])
dfs['raw_occupancy'] = dfs.groupby(by=['date', 'trip_id'])['net_count'].cumsum()

dfs[dfs['raw_occupancy'] < 0]
gdf = dfs.groupby(by=['date','trip_id'])
dfs['min_occupancy'] = gdf['raw_occupancy'].min()
dfs['occupancy'] = dfs.apply(lambda row: row['raw_occupancy'] - row['min_occupancy'], axis='columns')
gdf.get_group(('2020-04-16', 151780020))
gdf.get_group(('2020-03-23', 139653020))

compress_pickle('chattanooga_bus_occupancy_dashboard', df)
