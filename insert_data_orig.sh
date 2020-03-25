#!/bin/bash

host=fl-cs-chenjuan1.srv.aau.dk
db_name=california_traffic
user=razvan
pw=12345678

header="(time, sensor_id, "
for i in {0..7}
do
  header+="cars_lane_$i, "
  header+="occupancy_lane_$i, "
  header+="speed_lane_$i, "
done

header="${header:0:-2})"
data_path=$PWD"/download_data/download/d04_text_station_raw_2017_"
curent_path=""
no_days=0

for month in {01..03}
do
  case "$month" in
    01) no_days=31;;
    02) no_days=28;;
    03) no_days=31 ;;
  esac

  for day in $(seq 01 $no_days)
    do
      curent_path=$data_path$month

      if (($day < 10)); then
        curent_path+=_0$day.txt
      else
        curent_path+=_$day.txt
      fi

      gunzip $curent_path
      PGPASSWORD=$pw psql -h $host -d $db_name -U $user -c "\copy pems_raw_data $header from '$curent_path' with delimiter as ','"
      echo Finished month $month day $day
    done
done