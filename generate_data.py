from DBConnection import db
import numpy as np
from util import load_pickle
import os

remove_ratio = 0.5
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

mask = np.ones(num_sensors)
mask[:int(num_sensors * remove_ratio)] = 0
num_train = int(round(num_dates * 0.8))
train_samples = data[:num_train]
test_samples = data[num_train:]

x_train = []
y_train = []
mask_train = []
for sample in train_samples:
    np.random.shuffle(mask)
    masked_sample = sample * np.expand_dims(mask, 1).repeat(no_features, 1)
    x_train.append(masked_sample)
    y_train.append(masked_sample)
    mask_train.append(mask)

x_test = []
y_test = []
mask_test = []
for sample in test_samples:
    np.random.shuffle(mask)
    masked_sample = sample * np.expand_dims(mask, 1).repeat(no_features, 1)
    x_test.append(masked_sample)
    # complement of the mask
    y_test.append(sample * np.expand_dims((mask == 0).astype('float32'), 1).repeat(no_features, 1))
    mask_test.append(mask)

np.savez_compressed(os.path.join(output_dir, "%s.npz" % 'train'),
                    x=np.stack(x_train), y=np.stack(y_train), mask=np.stack(mask_train))

np.savez_compressed(os.path.join(output_dir, "%s.npz" % 'test'),
                    x=np.stack(x_test), y=np.stack(y_test), mask=np.stack(mask_test))

# Compute the average distribution
data = db.execute_query(select_query.format('pems_final', values_sensor_ids))
data = np.array([x[2:] for x in data])
data = data.reshape([num_dates, num_sensors, no_features])
num_train = int(round(num_dates * 0.8))
train_samples = data[:num_train]

# Apply same masks
masked_data = []
for i in range(train_samples.shape[0]):
    masked_data.append(train_samples[i] * np.expand_dims(mask_train[i], 1).repeat(no_features, 1))

masked_data = np.stack(masked_data)
sensor_cars_counts = np.sum(masked_data, axis=0)  # (sensor, buckets)
mean_sensor_cars_counts = sensor_cars_counts / masked_data.shape[0]

total = np.sum(mean_sensor_cars_counts, keepdims=True, axis=1)
total[total == 0] = 1
average_histogram = mean_sensor_cars_counts / np.broadcast_to(total, mean_sensor_cars_counts.shape)
np.savez_compressed(os.path.join(output_dir, "%s.npz" % 'average_histogram'), average_histogram)