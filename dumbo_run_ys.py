import sys
import pyproj
import csv
import shapely.geometry as geom
import fiona
import fiona.crs
import shapely
import rtree
import geopandas as gpd
import numpy as np
import operator
import pandas as pd
import pyspark
from pyspark import SparkContext
from shapely.geometry import Point
from pyspark.sql import SQLContext
import datetime

def countLine(partID, records):
    import pyproj
    import csv
    import shapely.geometry as geom
    import fiona
    import fiona.crs
    import shapely
    import rtree
    import geopandas as gpd
    import numpy as np
    import operator
    import pandas as pd
    import pyspark
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    import datetime
    index = rtree.Rtree()
    for idx, geometry in enumerate(entr_buf.geometry):
        index.insert(idx, geometry.bounds)

    entr_lines = {}
    proj = pyproj.Proj(init='epsg:2263', preserve_units=True)

    if partID==0:
        records.next()
    reader = csv.reader(records)

    for row in reader:
        if ((float(row[5])!=0) and float(row[9]!=0)):
            if row[1]:
        	wd_h = datetime.datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
        	wd = wd_h.weekday()
        	hour = wd_h.hour
        	day = wd_h.day
        	month = wd_h.month
            else:
        	wd = None
        	hour = None
        	day = None
        	month = None

            p = geom.Point(proj(float(row[5]), float(row[6])))
            d = geom.Point(proj(float(row[9]), float(row[10])))
            p_potential = index.intersection((p.x,p.y,p.x,p.y))
            d_potential = index.intersection((d.x,d.y,d.x,d.y))
            p_match = None # The first one match, should be the closest one? No!
            d_match = None

            for p_idx in p_potential:
                if entr_buf.geometry[p_idx].contains(p):
                    p_match = p_idx # print 'p',p_idx
                    p_lines = set(entr_buf.lines[p_idx])
                    break

            for d_idx in d_potential:
                if entr_buf.geometry[d_idx].contains(d):
                    d_match = d_idx # print 'd',d_idx
                    d_lines = set(entr_buf.lines[d_idx])
                    break

            if ((p_match and d_match) and (p_match != d_match)):
                dirct_lines = tuple(p_lines.intersection(d_lines))
                dirct_lines_wd_h_d_m = (dirct_lines, wd, hour, day, month)
                if dirct_lines:
                    entr_lines[dirct_lines_wd_h_d_m] = entr_lines.get(dirct_lines_wd_h_d_m, 0)+1
    return entr_lines.items()

def mapper(record):
    for key in record[0][0]:
        yield (key, record[0][1], record[0][2], record[0][3], record[0][4]), record[1]


def service(record):
	if (record[0][0] == 'B' and (record[0][1] in [5, 6])):
		pass
	elif (record[0][0] == 'W' and (record[0][1] in [5, 6])):
		pass
	elif (record[0][0] == 'C' and (record[0][2] in range(0,6))):
		pass
	elif (record[0][0] == 'B' and (record[0][1] in range(0,6))):
		pass
	elif (record[0][0] == 'S' and (record[0][1] in range(0,6))):
		pass
	elif (record[0][0] == 'W' and (record[0][1] in range(0,6))):
		pass
	else:
		return record

def fetch_entr_geo(entr_points):
    import geopandas as gpd
    import pyproj
    routes = ['route_'+str(i) for i in range(1,12)]
    entr_geo = gpd.GeoDataFrame(columns=['geometry', 'lines'])
    proj = pyproj.Proj(init='epsg:2263', preserve_units=True)
    for i in range(len(entr_points)):
        entr_coor = entr_points[i].asDict()['geometry'].asDict()['coordinates']
        entr_buffer = Point(proj(float(entr_coor[0]), float(entr_coor[1]))).buffer(100)
        entr_prop = entr_points[i].asDict()['properties'].asDict()
        entr_lines = [entr_prop[r] for r in routes if entr_prop[r]]
        entr_geo = entr_geo.append({'geometry':entr_buffer, 'lines':entr_lines}, ignore_index=True)
    return entr_geo

if __name__ == '__main__':
    sc = SparkContext(appName="bigdata_project")   
    entr_json_path = '/user/ys2808/2016_(May)_New_York_City_Subway_Station_Entrances.json'
    #taxi_csv_path = '/user/df1676/yellow_tripdata_2016-01.csv'
    #taxi_csv_path = sys.argv[1]
    
    sqlContext = SQLContext(sc) 
    global entr_buf
    entr_points = sqlContext.read.load(entr_json_path, format='json', header=True, inferSchema=True).collect()[0].asDict()['features']
    entr_buf = fetch_entr_geo(entr_points)
    
    #sc = SparkContext(appName="bigdata")
    
    rdd = sc.textFile(sys.argv[1])
    counts = rdd.mapPartitionsWithIndex(countLine).flatMap(mapper).reduceByKey(lambda x,y: x+y).filter(service)
    #print counts.collect()[0]
    counts.saveAsTextFile(sys.argv[2])
