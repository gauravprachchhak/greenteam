# Loads the data for GreenHouse Gas Emissions.
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Load GHG Data').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

ghg_schema = types.StructType([
    types.StructField('country', types.StringType(), False),
    types.StructField('year', types.StringType(), False),
    types.StructField('tot_ghg_ex_lucf', types.FloatType(), False),
    types.StructField('tot_ghg_in_lucf', types.FloatType(), False),
    types.StructField('tot_co2_ex_lucf', types.FloatType(), False),
    types.StructField('tot_ch4_ex_lucf', types.FloatType(), False),
    types.StructField('tot_n20_ex_lucf', types.FloatType(), False),
    types.StructField('tot_fgas_ex_lucf', types.FloatType(), False),
    types.StructField('tot_co2_in_lucf', types.FloatType(), False),
    types.StructField('tot_ch4_in_lucf', types.FloatType(), False),
    types.StructField('tot_n20_in_lucf', types.FloatType(), False),
    types.StructField('energy', types.FloatType(), False),
    types.StructField('industrial_process', types.FloatType(), False),
    types.StructField('agriculture', types.FloatType(), False),
    types.StructField('waste', types.FloatType(), False),
    types.StructField('lucf', types.FloatType(), False),
    types.StructField('bunker_fuel', types.FloatType(), False),
    types.StructField('energy_electricity_heat', types.FloatType(), False),
    types.StructField('energy_manufacturing_construction', types.FloatType(), False),
    types.StructField('energy_transportation', types.FloatType(), False),
    types.StructField('energy_ofc', types.FloatType(), False),
    types.StructField('energy_fugitive_emissions', types.FloatType(), False),
])

def main(inputs):
    ghg = spark.read.csv(inputs, schema=ghg_schema, header=True)
    # Removed the first two null values
    ghg = ghg.filter(ghg.country.isNotNull())

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs, output)
