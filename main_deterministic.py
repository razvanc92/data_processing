import subprocess
from util import create_raw_table, create_final_table_deterministic, load_pickle
from DBConnection import db
import numpy as np
import time

db.execute_command('SET DATESTYLE TO MDY')
# create_raw_table('pems_raw_data')
# create_raw_table('pems_raw_sensor_data')
# create_final_table('pems_final')
create_final_table_deterministic('pems_final_deterministic')
# create_final_table('pems_final_normalized')
#
# print('Start insert data')
# rc = subprocess.call("./insert_data.sh", shell=True)
# print('Data successfully inserted to DB')

# Purge non used sensors
# stime = time.time()
# sensor_ids = np.loadtxt('data/sensor_graph/bay_sensor_location.csv', dtype='str', delimiter=',')[:, 0]  # PEMS
# values_sensor_ids = str(['(' + str(x) + ')' for x in sensor_ids]).replace("'", "")[1:-1]
# query = "insert into pems_raw_sensor_data select * FROM pems_raw_data WHERE sensor_id = ANY(VALUES {0})".format(
#     values_sensor_ids)
# db.execute_command(query)
# print('{}s elapsed to select and insert data in sensor table'.format(time.time() - stime))

# intervals
# query = "CREATE VIEW pems_intervals_speed AS SELECT pems_raw_sensor_data.sensor_id, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_0::text, ''::text)::double precision, 0) as cars_lane_0, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.speed_lane_0::text, ''::text)::double precision, 0) * COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_0::text, ''::text)::double precision, 0) as speed_lane_0, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_1::text, ''::text)::double precision, 0) as cars_lane_1, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.speed_lane_1::text, ''::text)::double precision, 0) * COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_1::text, ''::text)::double precision, 0) as speed_lane_1, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_2::text, ''::text)::double precision, 0) as cars_lane_2, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.speed_lane_2::text, ''::text)::double precision, 0) * COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_2::text, ''::text)::double precision, 0) as speed_lane_2, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_3::text, ''::text)::double precision, 0) as cars_lane_3, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.speed_lane_3::text, ''::text)::double precision, 0) * COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_3::text, ''::text)::double precision, 0) as speed_lane_3, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_4::text, ''::text)::double precision, 0) as cars_lane_4, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.speed_lane_4::text, ''::text)::double precision, 0) * COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_4::text, ''::text)::double precision, 0) as speed_lane_4, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_5::text, ''::text)::double precision, 0) as cars_lane_5, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.speed_lane_5::text, ''::text)::double precision, 0) * COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_5::text, ''::text)::double precision, 0) as speed_lane_5, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_6::text, ''::text)::double precision, 0) as cars_lane_6, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.speed_lane_6::text, ''::text)::double precision, 0) * COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_6::text, ''::text)::double precision, 0) as speed_lane_6, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_7::text, ''::text)::double precision, 0) as cars_lane_7, " \
#         "    COALESCE(NULLIF(pems_raw_sensor_data.speed_lane_7::text, ''::text)::double precision, 0) * COALESCE(NULLIF(pems_raw_sensor_data.cars_lane_7::text, ''::text)::double precision, 0) as speed_lane_7, " \
#         "    timezone('UTC'::text, to_timestamp(floor(date_part('epoch'::text, pems_raw_sensor_data.\"time\"::timestamp without time zone) / 900::double precision) * 900::double precision)) AS interval_alias " \
#         "   FROM pems_raw_sensor_data "
# db.execute_command(query)
#
# query = "INSERT INTO pems_final_deterministic select temp.interval_alias, temp.sensor_id, " \
#         " (temp.speed_lane_0 + temp.speed_lane_1 + temp.speed_lane_2 + temp.speed_lane_3 +  " \
#         "  temp.speed_lane_4 + temp.speed_lane_5 + temp.speed_lane_6 + temp.speed_lane_7)   /  " \
#         "  GREATEST((temp.cars_lane_0 + temp.cars_lane_1 + temp.cars_lane_2 + temp.cars_lane_3 +  " \
#         " 		   temp.cars_lane_4 + temp.cars_lane_5 + temp.cars_lane_6 + temp.cars_lane_7), 1::double precision) " \
#         "    " \
#         " from (  select pems_intervals_speed.interval_alias, sensor_id,  " \
#         " 	  sum(pems_intervals_speed.speed_lane_0 ) as speed_lane_0," \
#         " 	  sum(pems_intervals_speed.speed_lane_1 ) as speed_lane_1," \
#         " 	  sum(pems_intervals_speed.speed_lane_2 ) as speed_lane_2," \
#         " 	  sum(pems_intervals_speed.speed_lane_3 ) as speed_lane_3," \
#         " 	  sum(pems_intervals_speed.speed_lane_4 ) as speed_lane_4," \
#         " 	  sum(pems_intervals_speed.speed_lane_5 ) as speed_lane_5," \
#         " 	  sum(pems_intervals_speed.speed_lane_6 ) as speed_lane_6," \
#         " 	  sum(pems_intervals_speed.speed_lane_7 ) as speed_lane_7," \
#         " 	  sum(pems_intervals_speed.cars_lane_0 )  as cars_lane_0, " \
#         " 	  sum(pems_intervals_speed.cars_lane_1 )  as cars_lane_1," \
#         " 	  sum(pems_intervals_speed.cars_lane_2 )  as cars_lane_2, " \
#         " 	  sum(pems_intervals_speed.cars_lane_3 )  as cars_lane_3, " \
#         " 	  sum(pems_intervals_speed.cars_lane_4 )  as cars_lane_4, " \
#         " 	  sum(pems_intervals_speed.cars_lane_5 )  as cars_lane_5, " \
#         " 	  sum(pems_intervals_speed.cars_lane_6 )  as cars_lane_6, " \
#         " 	  sum(pems_intervals_speed.cars_lane_7 )  as cars_lane_7 " \
#         " 	  from pems_intervals_speed group by interval_alias, sensor_id) as temp"
#
# db.execute_command(query)
#
print('Starting to fill in missing data')

# populate with 0 if there are missing rows
select_dates = "SELECT distinct(time) from pems_final_deterministic;"
select_sensors = "SELECT distinct(sensor_id) from pems_final_deterministic;"
select_data = "SELECT time, sensor_id from pems_final_deterministic;"

dates = db.execute_query(select_dates)
sensors = db.execute_query(select_sensors)
data = db.execute_query(select_data)

start_time = time.time()
missing_rows = []

data_dict = {}
for row in data:
    date = row[0]
    sensor_id = row[1]

    if date not in data_dict:
        data_dict[date] = {}

    data_dict[date][sensor_id] = 1

print(time.time() - start_time)

for date in dates:
    date = date[0]
    for sensor in sensors:
        sensor = sensor[0]
        if sensor not in data_dict[date]:
            missing_rows.append((date, int(sensor), 0))

print(len(missing_rows))
missing_rows = str(missing_rows)[1:-1]
insert_query = "INSERT INTO pems_final_deterministic values {0}".format(missing_rows)
db.execute_command(insert_query)
