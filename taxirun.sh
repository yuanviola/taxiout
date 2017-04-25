#!/bin/bash
for i in `seq 10 12`;
do
  spark-submit why_yellow_taxi/Run/dumbo_run.py /user/ys2808/yellow_tripdata_2015-"$i".csv /user/ys2808/taxi_"$i"
done
