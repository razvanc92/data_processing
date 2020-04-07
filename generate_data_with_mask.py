from DBConnection import db
import numpy as np
from util import load_pickle, shuffle_along_axis
import os

remove_ratio = 0.4
output_dir = 'final_data/PEMS-BAY/medium/estimation/{0}'.format(remove_ratio)
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

# 1 create a mask of shape (ts, nodes)
mask = np.ones([num_dates, num_sensors])
mask[:, :int(num_sensors * remove_ratio)] = float('nan')
mask = shuffle_along_axis(mask, 1)

# 2. save data orig, and mask x
x_orig = data.copy()
x_masked = data * np.expand_dims(mask, -1).repeat(no_features, -1)

# 3 invert the mask so 1 means masked, and 0 not masked evey 1 in here the data is not present in x_masked
mask_to_save = mask * 0
mask_to_save = np.nan_to_num(mask_to_save, 1, 1.)

num_train = int(round(num_dates * 0.7))
num_val = int(round(num_dates * 0.8))

np.savez_compressed(os.path.join(output_dir, "%s.npz" % 'train'),
                    x=x_masked[:num_train], y=x_masked[:num_train], masks=mask_to_save[:num_train])

np.savez_compressed(os.path.join(output_dir, "%s.npz" % 'val'),
                    x=x_masked[num_train:num_val], y=x_orig[num_train:num_val], masks=mask_to_save[num_train:num_val])

np.savez_compressed(os.path.join(output_dir, "%s.npz" % 'test'),
                    x=x_masked[num_val:], y=x_orig[num_val:], masks=mask_to_save[num_val:])

# Compute the average distribution
data = db.execute_query(select_query.format('pems_final', values_sensor_ids))
data = np.array([x[2:] for x in data])
data = data.reshape([num_dates, num_sensors, no_features])
num_train = int(round(num_dates * 0.7))
train_samples = data[:num_train]

# Apply same masks
mask = np.nan_to_num(mask, 1, 0.)
masked_data = train_samples * np.expand_dims(mask[:num_train], -1).repeat(no_features, -1)
sensor_cars_counts = np.sum(masked_data, axis=0)  # (sensor, buckets)
mean_sensor_cars_counts = sensor_cars_counts / masked_data.shape[0]

total = np.sum(mean_sensor_cars_counts, keepdims=True, axis=1)
total[total == 0] = 1
average_histogram = mean_sensor_cars_counts / np.broadcast_to(total, mean_sensor_cars_counts.shape)
np.save(os.path.join(output_dir, "%s.npy" % 'average_histogram'), average_histogram)
