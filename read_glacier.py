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

# add more functions as necessary
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

def main(inputs):
	test_tmax = spark.read.csv(inputs, schema = glacier_schema, header = True)     # Reads the RGI60_Glacier data.
	test_tmax.show()                                                               # Displays the dataframe.

if __name__ == '__main__':
    inputs = sys.argv[1]        # path to the RGI60_Glacier data.
    main(inputs)                                                



'''
Questions to ask:

1. Why using IntegerType, DateType gives error? The StringType, FloatType works though.
'''