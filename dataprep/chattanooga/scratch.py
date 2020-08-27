# https://github.com/hdemma/covid-19/blob/master/analysis/data_prep/nashville_bus_occupancy_data_processing.ipynb

import itertools as it
import multiprocessing as mp
import pandas as pd
import datetime as dt


def demo(a, b, c):
    print(f'a={a}, b={b}, c={c}')


def apply_parallel(func, df_apc, df_gtfs):
    df_grouped = df_apc.groupby(['apc_trip_id', 'ride_check_date', 'gtfs_start_date'])

    with mp.Pool(mp.cpu_count()) as p:
        ret_list = p.starmap(func, [(group, df_apc, df_gtfs) for name, group in df_grouped])
    return pd.concat(ret_list)


def main():
    df_apc = pd.read_csv('apc.csv', index_col=0)
    df_gtfs = pd.read_csv('gtfs.csv', index_col=0)
    start = dt.datetime.now()
    parallel_result = apply_parallel(demo, df_apc, df_gtfs)
    end = dt.datetime.now()
    print("time elapsed:", end - start)

    parallel_result.to_csv('test_apc_data_processing.csv')