# taxiout
1.dumbo_run_ys.py

Run on Spark to filter out all the yellow cab trips along subway line.(origin and destination points are within 100 meters buffer of subway stations, and the trip could be reached by direct subway trip, with no transfer)

2.taxirun.sh

Bash code to run loop for 12 months

3.raw

raw txt data from Spark

4.clean_raw_data.ipynb

clean raw txt data into csv files, and saved in 5csv

5 csv

cleaned data

6.Analysis of yellow cab trips along subway line.ipynb

Data exploration and statistical test. 
1)find out anomaly day,
2)find out anomaly hour,
3)statistical test, which line got 'stolen' business by yellow cab
4)statistical test, which two lines are most similar

1. raw:
As data from 2016 Jul-Dec of yellow cab have no more lon-lat info any more, we used 2015 Jul-Dec yellow cab data. So 2015 Jul - 2016 Jun is one whole year. We can do seasonal comparison as well.<br>
Format of each row of each file: ((Line, DayofWeek, Hour, Day, Month) ,count)
