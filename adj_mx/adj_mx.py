from map.map_helper import MapHelper
import numpy as np
from util import load_pickle
import pickle

sensors_locations = np.loadtxt('./adj_mx/graph_sensor_locations_bay.csv', delimiter=',')
average_data = np.load('./adj_mx/average_histogram.npy')
sensors_with_data = np.sum(average_data, -1)
sensors_with_data_inds = np.nonzero(sensors_with_data)[0]
sensors_locations = sensors_locations[sensors_with_data_inds]

assign = []
draw_m = []
draw_l = []
data_m_ids = []
data_l_ids = []
for sensor in sensors_locations:
    if sensor[2] < -121.967:
        data_m_ids.append(int(sensor[0]))
        draw_m.append([sensor[0], sensor[1], sensor[2]])
    else:
        data_l_ids.append(int(sensor[0]))
        draw_l.append([sensor[0], sensor[1], sensor[2]])

print('data M no sensors: ', len(data_m_ids))
print('data L no sensors: ', len(data_l_ids))
data_m_ids = np.sort(data_m_ids)
data_l_ids = np.sort(data_l_ids)

mapHelper = MapHelper(sensors_locations)
mapHelper.draw_line(([37.418931, -121.967], [37.254601, -121.967]))
mapHelper.draw_sensor()

sensors_ids_dcrnn, sensors_mapping_dcrnn, adj_mx_dcrnn = load_pickle(
    'data/sensor_graph/adj_mx_bay_DCRNN.pkl')

# Medium DATA
sensor_mapping_m = {}
count = 0
sensor_map_to_dcrnn = []
for sensor_id in data_m_ids:
    sensor_mapping_m[sensor_id] = count
    count += 1
    sensor_map_to_dcrnn.append(sensors_mapping_dcrnn[str(sensor_id)])

sensor_map_to_dcrnn = np.array(sensor_map_to_dcrnn)
adj_mx_m = adj_mx_dcrnn[sensor_map_to_dcrnn[:, None], sensor_map_to_dcrnn]
data_m = [data_m_ids, sensor_mapping_m, adj_mx_m]

with open('./adj_mx/adj_mx_medium.pkl', 'wb') as f:
    pickle.dump(data_m, f, protocol=2)

# LARGE DATA
sensor_mapping_l = {}
count = 0
sensor_map_to_dcrnn = []
for sensor_id in data_l_ids:
    sensor_mapping_l[sensor_id] = count
    count += 1
    sensor_map_to_dcrnn.append(sensors_mapping_dcrnn[str(sensor_id)])

sensor_map_to_dcrnn = np.array(sensor_map_to_dcrnn)
adj_mx_l = adj_mx_dcrnn[sensor_map_to_dcrnn[:, None], sensor_map_to_dcrnn]
data_l = [data_l_ids, sensor_mapping_l, adj_mx_l]

with open('./adj_mx/adj_mx_large.pkl', 'wb') as f:
    pickle.dump(data_l, f, protocol=2)
