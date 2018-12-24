# Command to run: spark-submit read_glacier.py 'path_to_the_RGI60_Glacier/01_rgi60_Alaska.csv'. Change the file after / for different glaciers.

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
import reverse_geocode # https://pypi.org/project/reverse_geocode/
import glob
import pandas as pd

spark = SparkSession.builder.appName('Transforming the Glacier Data').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# spark.sparkContext.setLogLevel('WARN')
# sc = spark.sparkContext

# add more functions as necessary
# glacier_schema = types.StructType([
#     types.StructField('RGI_ID', types.StringType(), False),
#     types.StructField('GLIMS_ID', types.StringType(), False),
#     types.StructField('Begin_Date', types.StringType(), False),                 # DateType
#     types.StructField('End_Date', types.StringType(), False),                   # The End Data is String type because the missing dates are represented by -9999999. Any of the date, month and year which is missing is denoted by '999*'.
#     types.StructField('Longitude', types.StringType(), False),
#     types.StructField('Latitude', types.StringType(), False),    
#     types.StructField('O1_SubRegion', types.StringType(), False),               # IntegerType
#     types.StructField('O2_SubRegion', types.StringType(), False),               # IntegerType
#     types.StructField('Area', types.FloatType(), False),
#     types.StructField('Minimum_Elevation', types.StringType(), False),          # IntegerType
#     types.StructField('Maximum_Elevation', types.StringType(), False),          # IntegerType
#     types.StructField('Median_Elevation', types.StringType(), False),           # IntegerType
#     types.StructField('Slope', types.FloatType(), False),
#     types.StructField('Aspect', types.StringType(), False),                     # IntegerType
#     types.StructField('Longest_surface_flowline', types.StringType(), False),   # IntegerType
#     types.StructField('Status', types.StringType(), False),                     # IntegerType
#     types.StructField('Connectivity_level', types.StringType(), False),         # IntegerType
#     types.StructField('Form', types.StringType(), False),                       # IntegerType
#     types.StructField('Terminus_type', types.StringType(), False),              # IntegerType
#     types.StructField('Surging', types.StringType(), False),                    # IntegerType
#     types.StructField('Linkages', types.StringType(), False),                   # IntegerType
#     types.StructField('Glacier_Name', types.StringType(), True)
#     ])

def main(inputs):
    # glac_data = spark.read.csv(inputs, schema = glacier_schema, header = True)
    # glac_data = glac_data.withColumn('Country', (geolocator.reverse(glac_data['Latitude'] + ',' + glac_data['Longitude']).raw['address']['country']))
    # glac_data.show()

    # Pandas
    # For a single file
    # glac_data = pd.read_csv(inputs)

    # For multiple lines.
    # https://medium.com/@kadek/elegantly-reading-multiple-csvs-into-pandas-e1a76843b688
    # glac_data = pd.concat([pd.read_csv(f) for f in glob.glob('/home/mihir/Desktop/Big_Data_Project/Big_Data_Project_Datasets/RGI60_Glacier/*.csv')])
    glac_data = pd.concat([pd.read_csv(f, sep = ',', encoding = 'utf-8') for f in glob.glob(inputs + '/*.csv')], ignore_index=True)
    glac_data['Year'] = glac_data['BgnDate'].astype(str).str[:4]	

    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    # 	print(glac_data)
    # https://www.digitalocean.com/community/tutorials/how-to-convert-data-types-in-python-3
    # https://stackoverflow.com/questions/46965999/reverse-geocoding-pandas-python
    glac_data['Country'] = glac_data.apply(lambda row: reverse_geocode.get(tuple([row['CenLat'], row['CenLon']]))['country'], axis = 1)

    # Now I will be performing the groupby based on country and the year and will aggregate Area Min, max, med Elevation, slope.
    # The area is added, min, max, med elevation is averaged and the slope is also averaged. (opinions may differ for the aggregation functions).

    # https://pandas.pydata.org/pandas-docs/version/0.22/generated/pandas.core.groupby.DataFrameGroupBy.agg.html
    glac_data = glac_data.groupby(['Country', 'Year']).agg({'Area' : 'mean', 'Zmin' : 'mean', 'Zmax' : 'mean', 'Zmed' : 'mean', 'Slope' : 'mean'})
    glac_data.to_csv('asdvdsa', sep='\t', encoding = 'utf-8')


    # glac_data.values
    # glac_data = glac_data.withColumn('Country', (geolocator.reverse(glac_data['Latitude'] + ',' + glac_data['Longitude']).raw['address']['country']))
    # glac_data.show()




if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)                                                



'''
Questions to ask:

1. Why using IntegerType, DateType gives error? The StringType, FloatType works though.
'''



# Change the name of the columns



# The values of the Area is not as expected because of the number of glaciers. We need to do someting for this issue.