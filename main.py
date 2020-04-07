import subprocess
from util import create_raw_table, create_final_table, load_pickle
from DBConnection import db
import numpy as np
import time

db.execute_command('SET DATESTYLE TO MDY')
# create_raw_table('pems_raw_data')
# create_raw_table('pems_raw_sensor_data')
# create_final_table('pems_final')
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

# # intervals
# query = "CREATE VIEW pems_intervals AS SELECT pems_raw_sensor_data.sensor_id, " \
#         "    pems_raw_sensor_data.cars_lane_0, " \
#         "    pems_raw_sensor_data.speed_lane_0, " \
#         "    pems_raw_sensor_data.cars_lane_1, " \
#         "    pems_raw_sensor_data.speed_lane_1, " \
#         "    pems_raw_sensor_data.cars_lane_2, " \
#         "    pems_raw_sensor_data.speed_lane_2, " \
#         "    pems_raw_sensor_data.cars_lane_3, " \
#         "    pems_raw_sensor_data.speed_lane_3,  " \
#         "    pems_raw_sensor_data.cars_lane_4, " \
#         "    pems_raw_sensor_data.speed_lane_4, " \
#         "    pems_raw_sensor_data.cars_lane_5, " \
#         "    pems_raw_sensor_data.speed_lane_5, " \
#         "    pems_raw_sensor_data.cars_lane_6, " \
#         "    pems_raw_sensor_data.speed_lane_6, " \
#         "    pems_raw_sensor_data.cars_lane_7, " \
#         "    pems_raw_sensor_data.speed_lane_7, " \
#         "    timezone('UTC'::text, to_timestamp(floor(date_part('epoch'::text, pems_raw_sensor_data.\"time\"::timestamp without time zone) / 900::double precision) * 900::double precision)) AS interval_alias " \
#         "   FROM pems_raw_sensor_data "
# db.execute_command(query)

# query = "CREATE VIEW pems_buckets AS SELECT pems_intervals.interval_alias," \
#         " pems_intervals.sensor_id," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 0::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 10::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_0_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 10::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 20::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_1_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 20::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 30::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_2_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 30::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 40::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_3_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 40::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 50::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_4_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 50::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 60::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_5_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 60::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 70::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_6_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 70::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 80::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_7_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 80::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 90::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_8_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 90::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 100::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_9_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_0::text, ''::text)::double precision IS NOT NULL AND 100::double precision <= pems_intervals.speed_lane_0::double precision AND pems_intervals.speed_lane_0::double precision < 110::double precision THEN pems_intervals.cars_lane_0::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_10_lane_0," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 0::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 10::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_0_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 10::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 20::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_1_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 20::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 30::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_2_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 30::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 40::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_3_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 40::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 50::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_4_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 50::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 60::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_5_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 60::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 70::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_6_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 70::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 80::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_7_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 80::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 90::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_8_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 90::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 100::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_9_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_1::text, ''::text)::double precision IS NOT NULL AND 100::double precision <= pems_intervals.speed_lane_1::double precision AND pems_intervals.speed_lane_1::double precision < 110::double precision THEN pems_intervals.cars_lane_1::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_10_lane_1," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 0::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 10::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_0_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 10::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 20::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_1_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 20::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 30::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_2_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 30::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 40::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_3_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 40::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 50::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_4_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 50::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 60::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_5_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 60::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 70::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_6_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 70::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 80::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_7_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 80::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 90::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_8_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 90::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 100::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_9_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_2::text, ''::text)::double precision IS NOT NULL AND 100::double precision <= pems_intervals.speed_lane_2::double precision AND pems_intervals.speed_lane_2::double precision < 110::double precision THEN pems_intervals.cars_lane_2::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_10_lane_2," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 0::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 10::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_0_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 10::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 20::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_1_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 20::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 30::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_2_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 30::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 40::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_3_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 40::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 50::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_4_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 50::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 60::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_5_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 60::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 70::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_6_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 70::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 80::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_7_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 80::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 90::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_8_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 90::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 100::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_9_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_3::text, ''::text)::double precision IS NOT NULL AND 100::double precision <= pems_intervals.speed_lane_3::double precision AND pems_intervals.speed_lane_3::double precision < 110::double precision THEN pems_intervals.cars_lane_3::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_10_lane_3," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 0::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 10::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_0_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 10::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 20::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_1_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 20::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 30::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_2_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 30::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 40::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_3_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 40::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 50::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_4_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 50::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 60::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_5_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 60::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 70::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_6_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 70::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 80::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_7_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 80::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 90::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_8_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 90::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 100::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_9_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_4::text, ''::text)::double precision IS NOT NULL AND 100::double precision <= pems_intervals.speed_lane_4::double precision AND pems_intervals.speed_lane_4::double precision < 110::double precision THEN pems_intervals.cars_lane_4::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_10_lane_4," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 0::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 10::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_0_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 10::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 20::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_1_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 20::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 30::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_2_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 30::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 40::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_3_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 40::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 50::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_4_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 50::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 60::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_5_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 60::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 70::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_6_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 70::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 80::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_7_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 80::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 90::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_8_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 90::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 100::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_9_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_5::text, ''::text)::double precision IS NOT NULL AND 100::double precision <= pems_intervals.speed_lane_5::double precision AND pems_intervals.speed_lane_5::double precision < 110::double precision THEN pems_intervals.cars_lane_5::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_10_lane_5," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 0::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 10::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_0_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 10::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 20::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_1_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 20::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 30::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_2_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 30::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 40::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_3_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 40::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 50::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_4_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 50::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 60::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_5_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 60::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 70::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_6_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 70::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 80::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_7_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 80::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 90::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_8_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 90::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 100::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_9_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_6::text, ''::text)::double precision IS NOT NULL AND 100::double precision <= pems_intervals.speed_lane_6::double precision AND pems_intervals.speed_lane_6::double precision < 110::double precision THEN pems_intervals.cars_lane_6::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_10_lane_6," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 0::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 10::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_0_lane_7," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 10::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 20::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_1_lane_7," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 20::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 30::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_2_lane_7," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 30::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 40::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_3_lane_7," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 40::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 50::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_4_lane_7," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 50::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 60::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_5_lane_7," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 60::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 70::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_6_lane_7," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 70::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 80::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_7_lane_7," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 80::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 90::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_8_lane_7," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 90::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 100::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_9_lane_7," \
#         " sum(" \
#         " CASE" \
#         " WHEN NULLIF(pems_intervals.speed_lane_7::text, ''::text)::double precision IS NOT NULL AND 100::double precision <= pems_intervals.speed_lane_7::double precision AND pems_intervals.speed_lane_7::double precision < 110::double precision THEN pems_intervals.cars_lane_7::double precision" \
#         " ELSE 0::double precision" \
#         " END) AS bucket_10_lane_7" \
#         " FROM pems_intervals" \
#         " GROUP BY pems_intervals.interval_alias, pems_intervals.sensor_id;"
# db.execute_command(query)
#
# query = "INSERT INTO pems_final SELECT pems_buckets.interval_alias," \
#         " pems_buckets.sensor_id," \
#         " COALESCE(pems_buckets.bucket_0_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_0_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_0_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_0_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_0_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_0_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_0_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_0_lane_7, 0::double precision) AS bucket_0," \
#         " COALESCE(pems_buckets.bucket_1_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_1_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_1_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_1_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_1_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_1_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_1_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_1_lane_7, 0::double precision) AS bucket_1," \
#         " COALESCE(pems_buckets.bucket_2_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_2_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_2_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_2_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_2_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_2_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_2_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_2_lane_7, 0::double precision) AS bucket_2," \
#         " COALESCE(pems_buckets.bucket_3_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_3_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_3_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_3_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_3_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_3_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_3_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_3_lane_7, 0::double precision) AS bucket_3," \
#         " COALESCE(pems_buckets.bucket_4_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_4_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_4_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_4_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_4_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_4_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_4_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_4_lane_7, 0::double precision) AS bucket_4," \
#         " COALESCE(pems_buckets.bucket_5_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_5_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_5_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_5_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_5_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_5_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_5_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_5_lane_7, 0::double precision) AS bucket_5," \
#         " COALESCE(pems_buckets.bucket_6_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_6_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_6_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_6_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_6_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_6_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_6_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_6_lane_7, 0::double precision) AS bucket_6," \
#         " COALESCE(pems_buckets.bucket_7_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_7_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_7_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_7_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_7_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_7_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_7_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_7_lane_7, 0::double precision) AS bucket_7," \
#         " COALESCE(pems_buckets.bucket_8_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_8_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_8_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_8_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_8_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_8_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_8_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_8_lane_7, 0::double precision) AS bucket_8," \
#         " COALESCE(pems_buckets.bucket_9_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_9_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_9_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_9_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_9_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_9_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_9_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_9_lane_7, 0::double precision) AS bucket_9," \
#         " COALESCE(pems_buckets.bucket_10_lane_0, 0::double precision) + COALESCE(pems_buckets.bucket_10_lane_1, 0::double precision) + COALESCE(pems_buckets.bucket_10_lane_2, 0::double precision) + COALESCE(pems_buckets.bucket_10_lane_3, 0::double precision) + COALESCE(pems_buckets.bucket_10_lane_4, 0::double precision) + COALESCE(pems_buckets.bucket_10_lane_5, 0::double precision) + COALESCE(pems_buckets.bucket_10_lane_6, 0::double precision) + COALESCE(pems_buckets.bucket_10_lane_7, 0::double precision) AS bucket_10" \
#         " FROM pems_buckets;"
#
# db.execute_command(query)
#
# print('Starting to fill in missing data')

# populate with 0 if there are missing rows
select_dates = "SELECT distinct(time) from pems_final;"
select_sensors = "SELECT distinct(sensor_id) from pems_final;"
select_data = "SELECT time, sensor_id from pems_final;"

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
            missing_rows.append((date, int(sensor), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

print(len(missing_rows))
missing_rows = str(missing_rows)[1:-1]
insert_query = "INSERT INTO pems_final values {0}".format(missing_rows)
db.execute_command(insert_query)

# query = "INSERT into pems_final_normalized select time, sensor_id, "
# total_query = "case "
# select_query = "("
#
# for bucket in range(11):
#     select_query += 'COALESCE(bucket_{0}, 0) + '.format(bucket)
#
# select_query = select_query[:-2] + ')'
#
# total_query = "case {0} when 0 then 1 else {1} end".format(select_query, select_query)
#
# for bucket in range(11):
#     query += ' bucket_{0} / {1}, '.format(bucket, total_query)
#
# query = query[:-2] + ' from pems_final'
# db.execute_command(query)
