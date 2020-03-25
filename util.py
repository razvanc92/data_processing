import pickle

import numpy as np
import os
from DBConnection import db


def sensor_list(location='data/sensor_graph/adj_mx_bay_dcrnn.pkl'):
    return list(load_pickle(location)[0])


def load_pickle(pickle_file):
    try:
        with open(pickle_file, 'rb') as f:
            pickle_data = pickle.load(f)
    except UnicodeDecodeError as e:
        with open(pickle_file, 'rb') as f:
            pickle_data = pickle.load(f, encoding='latin1')
    except Exception as e:
        print('Unable to load data ', pickle_file, ':', e)
        raise
    return pickle_data


def load_dataset(dataset_dir):
    data = {}
    for category in ['val']:
        cat_data = np.load(os.path.join(dataset_dir, category + '.npz'))
        data['x_' + category] = cat_data['x']
        data['y_' + category] = cat_data['y']

    return data


def create_raw_table(name):
    query = "SELECT NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename='{0}')".format(name)
    create_table = db.execute_query(query)[0][0]

    if create_table:
        query = 'CREATE TABLE {0} (' \
                'id serial PRIMARY KEY, ' \
                'time VARCHAR (50), ' \
                'sensor_id numeric , '.format(name)
        for i in range(8):
            query += 'occupancy_lane_{0} VARCHAR(10), '.format(i)
            query += 'speed_lane_{0} VARCHAR(10), '.format(i)
            query += 'cars_lane_{0} VARCHAR(10), '.format(i)

        query = query[:-2] + ')'
        db.execute_command(query, 'Successfully created d4_raw_table')

def create_final_table(name):
    query = "SELECT NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename='{0}')".format(name)
    create_table = db.execute_query(query)[0][0]

    if create_table:
        query = 'CREATE TABLE {0} (' \
                'time VARCHAR (50), ' \
                'sensor_id numeric , '.format(name)

        for i in range(11):
            query+= 'bucket_{} numeric, '.format(i)

        query = query[:-2] + ')'
        db.execute_command(query, 'Successfully created d4_raw_table')
