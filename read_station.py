

	# Command to run: spark-submit read_glacier.py 'path_to_the_RGI60_Glacier/01_rgi60_Alaska.csv'. Change the file after / for different glaciers.

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Load_Glacier_Data').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# spark.sparkContext.setLogLevel('WARN')
# sc = spark.sparkContext


# This is kept for reference. Keep it, I will delete it when I solve the errors.
# # add more functions as necessary
# glacier_schema = types.StructType([
#     types.StructField('RGI_ID', types.StringType(), False),
#     types.StructField('GLIMS_ID', types.StringType(), False),
#     types.StructField('Begin_Date', types.DateType(), False),
#     types.StructField('End_Date', types.StringType(), False),		# The End Data is String type because the missing dates are represented by -9999999. Any of the date, month and year which is missing is denoted by '999*'.
#     types.StructField('Longitude', types.FloatType(), False),
#     types.StructField('Latitude', types.FloatType(), False),
#     types.StructField('O1_SubRegion', types.IntegerType(), False),
#     types.StructField('O2_SubRegion', types.IntegerType(), False),
#     types.StructField('Area', types.FloatType(), False),
#     types.StructField('Minimum_Elevation', types.IntegerType(), False),
#     types.StructField('Maximum_Elevation', types.IntegerType(), False),
#     types.StructField('Median_Elevation', types.IntegerType(), False),
#     types.StructField('Slope', types.FloatType(), False),
#     types.StructField('Aspect', types.IntegerType(), False),
#     types.StructField('Longest_surface_flowline', types.IntegerType(), False),
#     types.StructField('Status', types.IntegerType(), False),
#     types.StructField('Connectivity_level', types.IntegerType(), False),
#     types.StructField('Form', types.IntegerType(), False),
#     types.StructField('Terminus_type', types.IntegerType(), False),
#     types.StructField('Surging', types.IntegerType(), False),
#     types.StructField('Linkages', types.IntegerType(), False),
#     types.StructField('Glacier_Name', types.StringType(), True)
#     ])


def main(inputs):
	
	# create station data frame
	###################################################################################
	dfStation = spark.read.text(inputs)
	dfStation = dfStation.select(
	    dfStation.value.substr(1,11).alias('id'),
	    dfStation.value.substr(13,8).cast('float').alias('latitude'),
	    dfStation.value.substr(22,9).cast('float').alias('longitude'),
	    dfStation.value.substr(32,6).cast('float').alias('elevation'),
	    dfStation.value.substr(39,2).alias('state'),
	    dfStation.value.substr(42,30).alias('name'),
	    dfStation.value.substr(73,3).alias('gsnflag'),
	    dfStation.value.substr(77,3).alias('hcnflag'),
	    dfStation.value.substr(81,5).cast('integer').alias('wmoid')
	)       

	# dfStation = dfStation1.select('id','latitude', 'longitude', 'elevation', 'state', 'name', 'gsnflag', 'hcnflag', 'wmoid')

	dfStation.createOrReplaceTempView("stationTbl")

	dfStationQuery = spark.sql("""	
	SELECT id, latitude, longitude, elevation, state, name
	FROM stationTbl
	""")
                                                # Displays the dataframe.

if __name__ == '__main__':
    inputs = sys.argv[1]        # path to the RGI60_Glacier data.
    main(inputs)                                                



'''
Questions to ask:

1. Why using IntegerType, DateType gives error? The StringType, FloatType works though.
'''