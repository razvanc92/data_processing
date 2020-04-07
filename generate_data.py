from DBConnection import db
import numpy as np
from util import load_pickle
import os

output_dir = 'final_data/PEMS-BAY/medium/estimation/'
num_dates = 8404
no_features = 11
num_sensors = 87

sensor_ids = load_pickle('final_data/PEMS-BAY/medium/adj_mx_medium.pkl')[0]
values_sensor_ids = str(['(' + str(x) + ')' for x in sensor_ids]).replace("'", "")[1:-1]

select_query = "SELECT time, sensor_id, bucket_0::float, bucket_1::float, bucket_2::float, bucket_3::float," \
               " bucket_4::float, bucket_5::float, bucket_6::float, bucket_7::float, bucket_8::float, bucket_9::float," \
               " bucket_10::float from {0} where sensor_id = ANY(VALUES {1})" \
               "order by time, sensor_id asc"

data = db.execute_query(select_query.format('pems_final_normalized', values_sensor_ids))
data = np.array([x[2:] for x in data])
data = data.reshape([num_dates, num_sensors, no_features])
np.save(os.path.join(output_dir, "%s.npz" % 'data'), data)

# Compute the average distribution
data = db.execute_query(select_query.format('pems_final', values_sensor_ids))
data = np.array([x[2:] for x in data])
data = data.reshape([num_dates, num_sensors, no_features])

sensor_cars_counts = np.sum(data, axis=0)  # (sensor, buckets)
mean_sensor_cars_counts = sensor_cars_counts / data.shape[0]

total = np.sum(mean_sensor_cars_counts, keepdims=True, axis=1)
total[total == 0] = 1
average_histogram = mean_sensor_cars_counts / np.broadcast_to(total, mean_sensor_cars_counts.shape)
np.save(os.path.join(output_dir, "%s.npy" % 'average_histogram'), average_histogram)
