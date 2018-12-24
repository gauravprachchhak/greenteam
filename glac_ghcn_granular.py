# Command to run: spark-submit read_glacier.py 'path_to_the_RGI60_Glacier/01_rgi60_Alaska.csv'. Change the file after / for different glaciers.

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from math import radians, cos, sin, asin, sqrt
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType

spark = SparkSession.builder.appName('Load_Glacier_Data').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

glacier_schema = types.StructType([
    types.StructField('RGI_ID', types.StringType(), False),
    types.StructField('GLIMS_ID', types.StringType(), False),
    types.StructField('Begin_Date', types.StringType(), False),                 # DateType
    types.StructField('End_Date', types.StringType(), False),                   # The End Data is String type because the missing dates are represented by -9999999. Any of the date, month and year which is missing is denoted by '999*'.
    types.StructField('Longitude', types.FloatType(), False),
    types.StructField('Latitude', types.FloatType(), False),    
    types.StructField('O1_SubRegion', types.StringType(), False),               # IntegerType
    types.StructField('O2_SubRegion', types.StringType(), False),               # IntegerType
    types.StructField('Area', types.FloatType(), False),
    types.StructField('Minimum_Elevation', types.StringType(), False),          # IntegerType
    types.StructField('Maximum_Elevation', types.StringType(), False),          # IntegerType
    types.StructField('Median_Elevation', types.StringType(), False),           # IntegerType
    types.StructField('Slope', types.FloatType(), False),
    types.StructField('Aspect', types.StringType(), False),                     # IntegerType
    types.StructField('Longest_surface_flowline', types.StringType(), False),   # IntegerType
    types.StructField('Status', types.StringType(), False),                     # IntegerType
    types.StructField('Connectivity_level', types.StringType(), False),         # IntegerType
    types.StructField('Form', types.StringType(), False),                       # IntegerType
    types.StructField('Terminus_type', types.StringType(), False),              # IntegerType
    types.StructField('Surging', types.StringType(), False),                    # IntegerType
    types.StructField('Linkages', types.StringType(), False),                   # IntegerType
    types.StructField('Glacier_Name', types.StringType(), True)
    ])



@pandas_udf("double", PandasUDFType.SCALAR)
def haversine_distance(station_lon, station_lat, glacier_lon, glacier_lat):
	"""
	Calculate the great circle distance between two points 
	on the earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians 
	station_lon, station_lat, glacier_lon, glacier_lat = map(radians, [station_lon, station_lat, glacier_lon, glacier_lat])

	# haversine formula 
	dlon = glacier_lon - station_lon 
	dlat = glacier_lat - station_lat 
	a = sin(dlat/2)**2 + cos(station_lat) * cos(glacier_lat) * sin(dlon/2)**2
	c = 2 * asin(sqrt(a)) 
	r = 6371 # Radius of earth in kilometers. Use 3956 for miles
	return c * r


def main(glac_data, station_data):

	# create station data frame
	###################################################################################
	station = pd.read_fwf(station_data, names=["id","latitude","longitude","elevation","state","name","gsnflag","hcnflag","vmoid"])


	# station = station.select(
	#     station.value.slice(1,11).alias('id'),
	#     station.value.slice(13,8).cast('float').alias('latitude'),
	#     station.value.slice(22,9).cast('float').alias('longitude'),
	#     station.value.slice(32,6).cast('float').alias('elevation'),
	# 	  station.value.slice(39,2).alias('state'),
	#     station.value.slice(42,30).alias('name'),
	#     station.value.slice(73,3).alias('gsnflag'),
	#     station.value.slice(77,3).alias('hcnflag'),
	#     station.value.slice(81,5).cast('integer').alias('wmoid')
	# )       

	# print(station.head(4))

	# print(station.head(4))
	glac_data = pd.read_csv(glac_data)     # Reads the RGI60_Glacier data.
	# station_data_lat_lon = station_data.select(station_data['latitude'].alias('station_lat'), station_data['longitude'].alias('station_lon'))
	station_data_lat_lon = station[['latitude','longitude']]
	glac_data = glac_data[['CenLat','CenLon']]
	
	temp = glac_data.merge(station_data_lat_lon, how='outer')

	print(temp.head(5))
	# glac_station_lat_lon = glac_data.crossJoin(station_data_lat_lon)

	# glac_station_lat_lon.write.csv('glac_station_lat_lon')

	# glac_station_lat_lon.show()

	# glac_station_haversine_distance = glac_station_lat_lon.withColumn('Distance', haversine_distance('glacier_lon', 'glacier_lat', 'station_lon', 'station_lat'))

	# glac_nearest_station = glac_station_haversine_distance.groupby(['glacier_lon', 'glacier_lat']).agg(functions.min('Distance'))

	# glac_station_haversine_distance.show()

if __name__ == '__main__':
    glac_data = sys.argv[1]        # path to the RGI60_Glacier data.
    station_data = sys.argv[2]
    main(glac_data, station_data)                                                



'''
Questions to ask:
# create station data frame
	###################################################################################
	station = pd.read_csv(station_data,sep='\t',encoding = 'utf-8',header=None,names=["id","latitude","longitude","elevation","state","name","gsnflag","hcnflag","vmoid"])
	# station = pd.read_fwf(station_data, names=["id","latitude","longitude","elevation","state","name","gsnflag","hcnflag","vmoid"])
	print (station['id'])

	# station.columns = ["id","latitude","longitude","elevation","state","name","gsnflag","hcnflag","vmoid"]
	station.to_csv('abcd.csv')
1. Why using IntegerType, DateType gives error? The StringType, FloatType works though.
'''