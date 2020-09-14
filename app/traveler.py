import pandas as pd

statistic_fun = {
    'MEAN': pd.core.groupby.GroupBy.mean,
    'MINIMUM': pd.core.groupby.GroupBy.min,
    'MAXIMUM': pd.core.groupby.GroupBy.max,
    'STD_DEV': pd.core.groupby.GroupBy.std,
    'VARIANCE': pd.core.groupby.GroupBy.var,
}


# def load_bus_occupancy():
#     df = dataextract.decompress_pickle('bus_occupancy_jan_through_jun.pbz2')
#     return df


# def acquire_dataframe(session_id):
#     df = load_bus_occupancy()
#     df['date'] = df['date_time'].dt.date
#     df['arrival_time'] = df['date_time'].dt.time
#     return df


def statistic_opt(opt_key): return dict(label=opt_key, value=opt_key)


all_statistic_opts = list(map(lambda key: statistic_opt(key), statistic_fun.keys()))
