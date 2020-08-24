import bz2
import pickle
import pandas as pd
import zipfile


# Pickle a file and then compress it into a file with extension
def compressed_pickle(title, data):
    with bz2.BZ2File(title + '.pbz2', 'w') as f:
        pickle.dump(data, f)


# load data
# with zipfile.ZipFile('bus_occupancy_jan_through_jun.csv.zip', 'r') as zip_ref:
#     zip_ref.extractall()
#
# df = pd.read_csv('bus_occupancy_jan_through_jun.csv', index_col=0)
# compressed_pickle('bus_occupancy_jan_through_jun', df)
