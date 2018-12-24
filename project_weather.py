from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Big Data Project - GHCN-GHG').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

'''
Course:						Programming in Big Data 1 (CMPT 732)
Authors:					Veekesh Dhununjoy, Gaurav Prachchhak, Mihir Gajjar

Command to run:

spark-submit --jars azure.jar project_weather.py 
BigDataProject/GHCN_Yearly/ 
BigDataProject/GHCN_Country/ 
BigDataProject/GHCN_Stations/  
BigDataProject/CAIT_GHG_Emissions/ 
BigDataProject/Vehicle/

Functions:

main():						This is the main defenition where all the computations are done
							Input:(a,b,c,d,e)
							Output: Cosmos DB Records Insertion

Variable Naming Convention:

writeConfig:				Cosmos DB Configuration
weather_schema:				GHCN dataset schema
country_schema:				GHCN countries data for getting countries from country code
ghg_schema:					GHG dataset schema
vehicle_schema:				Vehicles_In_Use Dataset
vehicles:					DF containing Vehicles Data
dfVehiclesQuery:			DF containing selected data from vehicles DF
dfCars*:					DF containing Cars in use for specific year, specifically used for unpivoting
dfVehiclesUnpivot:			DF containing Unpivoted Vehicles DF
weather:					DF containing weather dataset
dfWeatherQuery:				DF containing selected weather data for effective joining DFs
country:					DF containing countries with their respective codes
dfCountryQuery:				DF containing selected countries with some string manipulation ops
GHG:						DF containing GHG data
dfGHGQuery:					DF containing selected GHG data with some string manipulation ops
dfMerge:					DF containing weather joined with country on country code to get country name
dfNormalize:				DF containing normalized data for one-to-one mapping
dfWeather_GHG:				DF containing weather joined with GHG with some string manipuation ops
dfWeather_GHG_Vehicle:		DF containing merged data from all tables

'''

# Base Configuration
###################################################################################
writeConfig = {
 "Endpoint" : "https://greenteam1.documents.azure.com:443/",
 "Masterkey" : "AZxVLVVVVTkioZDTMaeyLsmQb7LkpJLv6PHS3Qg8PI6yme5L7JrFfUbnb8PdePkdjKNkQbT6KriZwXZDWiL6IA==",
 "Database" : "weatherdb",
 "Collection" : "testforprofessor"
}

###################################################################################

# add more functions as necessary

def main(weather_inputs, country_inputs, station_inputs, ghg_inputs,vehicle_inputs):
	# main logic starts here

	# create weather schema
	############################################################################
	weather_schema = types.StructType([
	types.StructField('station', types.StringType(), False),
	types.StructField('date', types.StringType(), False),
	types.StructField('observation', types.StringType(), False),
	types.StructField('value', types.IntegerType(), False),
	types.StructField('mflag', types.StringType(), False),
	types.StructField('qflag', types.StringType(), False),
	types.StructField('sflag', types.StringType(), False),
	types.StructField('obstime', types.StringType(), False),])

	# create country schema
	############################################################################
	country_schema = types.StructType([
	types.StructField('code', types.StringType(), False),
	types.StructField('country', types.StringType(), False),])

	# create Greenhouse Gas Emission schema
	############################################################################
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
	types.StructField('energy_fugitive_emissions', types.FloatType(), False),])

	# Vehicle Schema
	###################################################################################
	vehicle_schema = types.StructType([
	types.StructField('COUNTRY', types.StringType(), False),
	types.StructField('Y2005', types.IntegerType(), False),
	types.StructField('Y2006', types.IntegerType(), False),
	types.StructField('Y2007', types.IntegerType(), False),
	types.StructField('Y2008', types.IntegerType(), False),
	types.StructField('Y2009', types.IntegerType(), False),
	types.StructField('Y2010', types.IntegerType(), False),
	types.StructField('Y2011', types.IntegerType(), False),
	types.StructField('Y2012', types.IntegerType(), False),
	types.StructField('Y2013', types.IntegerType(), False),
	types.StructField('Y2014', types.IntegerType(), False),
	types.StructField('Y2015', types.IntegerType(), False),
	types.StructField('MotorizationRate2014', types.StringType(), False),
	types.StructField('MotorizationRate2015', types.StringType(), False),])

	# Create Vehicles Data Frame
	###################################################################################
	vehicles = spark.read.csv(vehicle_inputs, schema=vehicle_schema, header = True)
	vehicles.createOrReplaceTempView("vehiclesTbl")

	dfVehiclesQuery = spark.sql("""	
	SELECT COUNTRY as country, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, MotorizationRate2014, MotorizationRate2015
	FROM vehiclesTbl
	""")

	dfVehiclesQuery.createOrReplaceTempView("vehiclesTbl1")

	# Unpivot 
	###################################################################################
	dfCars2005 = dfVehiclesQuery.select('country', lit(2005).alias('year'), (dfVehiclesQuery.Y2005).alias('CarsInUse'))
	dfCars2006 = dfVehiclesQuery.select('country', lit(2006).alias('year'), (dfVehiclesQuery.Y2006).alias('CarsInUse'))
	dfCars2007 = dfVehiclesQuery.select('country', lit(2007).alias('year'), (dfVehiclesQuery.Y2007).alias('CarsInUse'))
	dfCars2008 = dfVehiclesQuery.select('country', lit(2008).alias('year'), (dfVehiclesQuery.Y2008).alias('CarsInUse'))
	dfCars2009 = dfVehiclesQuery.select('country', lit(2009).alias('year'), (dfVehiclesQuery.Y2009).alias('CarsInUse'))
	dfCars2010 = dfVehiclesQuery.select('country', lit(2010).alias('year'), (dfVehiclesQuery.Y2010).alias('CarsInUse'))
	dfCars2011 = dfVehiclesQuery.select('country', lit(2011).alias('year'), (dfVehiclesQuery.Y2011).alias('CarsInUse'))
	dfCars2012 = dfVehiclesQuery.select('country', lit(2012).alias('year'), (dfVehiclesQuery.Y2012).alias('CarsInUse'))
	dfCars2013 = dfVehiclesQuery.select('country', lit(2013).alias('year'), (dfVehiclesQuery.Y2013).alias('CarsInUse'))
	dfCars2014 = dfVehiclesQuery.select('country', lit(2014).alias('year'), (dfVehiclesQuery.Y2014).alias('CarsInUse'))
	dfCars2015 = dfVehiclesQuery.select('country', lit(2015).alias('year'), (dfVehiclesQuery.Y2015).alias('CarsInUse'))

	dfVehiclesUnpivot = dfCars2005.unionAll(dfCars2006).unionAll(dfCars2007).unionAll(dfCars2008).unionAll(dfCars2009).unionAll(dfCars2010).unionAll(dfCars2011).unionAll(dfCars2012).unionAll(dfCars2013).unionAll(dfCars2014).unionAll(dfCars2015)
	dfVehiclesUnpivot = dfVehiclesUnpivot.select(dfVehiclesUnpivot.country, dfVehiclesUnpivot.year, dfVehiclesUnpivot.CarsInUse).orderBy(dfVehiclesUnpivot.country, dfVehiclesUnpivot.year)
	dfVehiclesUnpivot.createOrReplaceTempView("vehiclesTbl2")

	# select u.country, u.[Year], u.VehicleInUse
	# from dbo.Vehicle V
	# unpivot
	# (
	# 	VehicleInUse
	# 	for [Year] IN ([2005],[2006],[2007],[2008],[2009],[2010],[2011],[2012],[2013],[2014],[2015])
	# ) u

	# create weather data frame
	###################################################################################
	weather = spark.read.csv(weather_inputs, schema=weather_schema)
	weather.createOrReplaceTempView("weatherTbl")

	dfWeatherQuery = spark.sql("""	
	SELECT station, LEFT(station,2) as countrycode, LEFT(date,4) AS year, date, observation, CAST(value AS DECIMAL(10,2))/10 as value
	FROM weatherTbl
	WHERE qflag IS NULL
	AND observation IN ('TMAX', 'TMIN')
	""")

	dfWeatherQuery.createOrReplaceTempView("weatherTbl1")

	# create country data frame
	###################################################################################
	country = spark.read.csv(country_inputs, schema=country_schema)
	country.createOrReplaceTempView("countryTbl")

	dfCountryQuery = spark.sql("""	
	SELECT code, TRIM(country) AS country
	FROM countryTbl
	""")

	# create station data frame - not being used anymore 
	###################################################################################
	# dfStation = spark.read.text(station_inputs)
	# dfStation1 = dfStation.select(
	# 	dfStation.value.substr(1,11).alias('id'),
	# 	dfStation.value.substr(13,8).cast('float').alias('latitude'),
	# 	dfStation.value.substr(22,9).cast('float').alias('longitude'),
	# 	dfStation.value.substr(32,6).cast('float').alias('elevation'),
	# 	dfStation.value.substr(39,2).alias('state'),
	# 	dfStation.value.substr(42,30).alias('name'),
	# 	dfStation.value.substr(73,3).alias('gsnflag'),
	# 	dfStation.value.substr(77,3).alias('hcnflag'),
	# 	dfStation.value.substr(81,5).cast('integer').alias('wmoid')
	# )

	# dfStation1.createOrReplaceTempView("stationTbl")

	# dfStationQuery = spark.sql("""	
	# SELECT id, latitude, longitude, elevation, state, name
	# FROM stationTbl
	# """)

	# create Greenhouse data frame
	###################################################################################
	GHG = spark.read.csv(ghg_inputs, schema=ghg_schema, header = True)
	GHG.createOrReplaceTempView("GHGTbl")
    
    # Change data types to varchar to be able to save in Cosmos DB
	###################################################################################

	dfGHGQuery = spark.sql("""	
	SELECT TRIM(country) AS country, TRIM(year) AS year, 
	CAST(tot_ghg_ex_lucf as varchar(100)) as tot_ghg_ex_lucf, 
	CAST(tot_ghg_in_lucf as varchar(100)) as tot_ghg_in_lucf, 
	CAST(tot_co2_ex_lucf as varchar(100)) as tot_co2_ex_lucf,  
	CAST(tot_co2_in_lucf as varchar(100)) as tot_co2_in_lucf,  
	CAST(tot_n20_ex_lucf as varchar(100)) as tot_n20_ex_lucf,  
	CAST(tot_n20_in_lucf as varchar(100)) as tot_n20_in_lucf,  
	CAST(tot_ch4_ex_lucf as varchar(100)) as tot_ch4_ex_lucf,  
	CAST(tot_ch4_in_lucf as varchar(100)) as tot_ch4_in_lucf,  
	CAST(tot_fgas_ex_lucf as varchar(100)) as tot_fgas_ex_lucf,  
	CAST(energy as varchar(100)) as energy,  
	CAST(industrial_process as varchar(100)) as industrial_process,  
	CAST(agriculture as varchar(100)) as agriculture, 
	CAST(waste as varchar(100)) as waste,  
	CAST(lucf as varchar(100)) as lucf,  
	CAST(bunker_fuel as varchar(100)) as bunker_fuel,  
	CAST(energy_electricity_heat as varchar(100)) as energy_electricity_heat,  
	CAST(energy_manufacturing_construction as varchar(100)) as energy_manufacturing_construction,  
	CAST(energy_transportation as varchar(100)) as energy_transportation,  
	CAST(energy_ofc as varchar(100)) as energy_ofc, 
	CAST(energy_fugitive_emissions as varchar(100)) as energy_fugitive_emissions
	FROM GHGTbl
	""")

	dfGHGQuery.createOrReplaceTempView("tblGHGFinal")

	#Merging Weather and Country Info
	####################################################################################
	dfMerge = spark.sql("""
	SELECT B.country, A.year, A.observation, AVG(A.value) AS average
	FROM weatherTbl1 A
	INNER JOIN countryTbl B ON A.countrycode = B.code
	GROUP BY B.country, A.year, A.observation
	ORDER BY B.country, A.year, A.observation
		""")

	dfMerge.createOrReplaceTempView("tblAverage")

	#1-1 record
	#####################################################################################

	dfNormalize = spark.sql("""
	SELECT DISTINCT A.country, A.year, B.average AS TMAX_AVG, C.average AS TMIN_AVG
	FROM tblAverage A
	LEFT JOIN tblAverage B ON A.country = B.country AND A.year = B.year AND B.observation = 'TMAX'
	LEFT JOIN tblAverage C ON A.country = C.country AND A.year = C.year AND C.observation = 'TMIN'
	ORDER BY A.country, A.year
		""")

	dfNormalize.createOrReplaceTempView("tblWeatherFinal")

	# JOIN Weather Data to GHG
	#####################################################################################

	dfWeather_GHG = spark.sql("""
	SELECT UPPER(A.country) as country, A.year, A.TMAX_AVG, A.TMIN_AVG, B.tot_ghg_ex_lucf, B.tot_ghg_in_lucf, B.tot_co2_ex_lucf, B.tot_co2_in_lucf,
	B.tot_ch4_ex_lucf, B.tot_ch4_in_lucf, B.tot_n20_ex_lucf, B.tot_n20_in_lucf, B.tot_fgas_ex_lucf, 
	B.energy, B.industrial_process, B.agriculture, B.waste, B.lucf, B.bunker_fuel, B.energy_electricity_heat, B.energy_manufacturing_construction, 
	B.energy_transportation, B.energy_ofc, B.energy_fugitive_emissions
	FROM tblWeatherFinal A
	INNER JOIN tblGHGFinal B ON TRIM(A.country) = TRIM(B.country) AND TRIM(A.year) = TRIM(B.year)
		""")

	dfWeather_GHG.createOrReplaceTempView("tblWeather_GHG")

	#Merge All Data Sets (Weather-GHG-Vehicles)
	######################################################################################
	
	dfWeather_GHG_Vehicle = spark.sql("""
	SELECT A.country, A.year, CAST(A.TMAX_AVG AS varchar(100)) as tmax_avg, CAST(A.TMIN_AVG AS varchar(100)) as tmin_avg, 
	A.tot_ghg_ex_lucf, A.tot_ghg_in_lucf, A.tot_co2_ex_lucf, A.tot_co2_in_lucf,
	A.tot_ch4_ex_lucf, A.tot_ch4_in_lucf, A.tot_n20_ex_lucf, A.tot_n20_in_lucf, A.tot_fgas_ex_lucf, 
	A.energy, A.industrial_process, A.agriculture, A.waste, A.lucf, A.bunker_fuel, A.energy_electricity_heat, A.energy_manufacturing_construction, 
	A.energy_transportation, A.energy_ofc, A.energy_fugitive_emissions, B.CarsInUse as cars_in_use
	FROM tblWeather_GHG A
	INNER JOIN vehiclesTbl2 B ON TRIM(A.country) = TRIM(B.country) AND TRIM(A.year) = TRIM(B.year)
	""")

	#dfWeather_GHG_Vehicle.show(truncate = False)
	#dfWeather_GHG_Vehicle.write.csv(output_file, mode='overwrite', header = 'True')
	#return

	######################################################################################
	#dfWeather_GHG_Vehicle.write.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=keyspace).save(mode ="append")
	dfWeather_GHG_Vehicle.write.format("com.microsoft.azure.cosmosdb.spark").options(**writeConfig).save()
	return
	
if __name__ == '__main__':
	weather_inputs = sys.argv[1]
	country_inputs = sys.argv[2]
	station_inputs = sys.argv[3]
	ghg_inputs = sys.argv[4]
	vehicle_inputs = sys.argv[5]
	main(weather_inputs, country_inputs, station_inputs, ghg_inputs, vehicle_inputs)