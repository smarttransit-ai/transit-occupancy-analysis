import bz2
import pickle
import pandas as pd


# Load any compressed pickle file
def decompress_pickle(file):
    data = bz2.BZ2File(file, 'rb')
    data = pickle.load(data)
    return data

# Pickle a file and then compress it into a file with extension
def compress_pickle(title, data):
    with bz2.BZ2File(f'{title}.pbz2', 'w') as f:
        pickle.dump(data, f)