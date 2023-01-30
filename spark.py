from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from hdfs import InsecureClient

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
schema = StructType([\
	StructField('_c0', LongType(), True),\
	StructField('Unnamed: 0', StringType(), True),\
	StructField('Unnamed: 0.1', StringType(), True),\
	StructField('Latitude', StringType(), True),\
	StructField('Longitude', StringType(), True),\
	StructField('Altitude', StringType(), True),\
	StructField('battery_state_of_charge_pct', DoubleType(), True),\
	StructField('ambient_air_temperature_degc', DoubleType(), True),\
	StructField('odometer_distance_m', StringType(), True),\
	StructField('unitID', LongType(), True),\
	StructField('RptTimestamp', LongType(), True),\
	StructField('m_vs', DoubleType(), True),\
	StructField('hvbatt_max_cell_temp_c', StringType(), True),\
	StructField('hvbatt_min_cell_temp_c', StringType(), True),\
	StructField('battery_available_energy_wh', LongType(), True),\
	StructField('fleet', StringType(), True),\
	StructField('mil_V', StringType(), True),\
	StructField('battery_regen_lifetime_wh', StringType(), True),\
	StructField('hvac_cycle_wh', StringType(), True),\
	StructField('vehicle_speed_kph', StringType(), True),\
	])
# df = spark.read.options(header='True', delimiter=',', schema=schema).csv("hdfs://localhost:9000/LE/Data/*.csv")

c = InsecureClient(url="http://localhost:9870", root='/')
files = c.list('LE/Data')
df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
columns_to_drop = ["_c0", "Unnamed: 0.1", "Latitude", "Longitude", "Altitude", "odometer_distance_m", "hvbatt_max_cell_temp_c", "hvbatt_min_cell_temp_c", "fleet", "mil_V", "battery_regen_lifetime_wh", "hvac_cycle_wh", "vehicle_speed_kph", "Unnamed: 0"]
df = df.drop(*columns_to_drop)
rdd = df.rdd
for f in files:
	if f[-4:] == ".csv":
		try:
			curr = spark.read.options(header='True', InferSchema='True', delimiter=',').csv("hdfs://localhost:9000/LE/Data/" + f)
			curr = curr.drop(*columns_to_drop).na.drop()
			curr.withColumn("battery_state_of_charge_pct", curr.battery_state_of_charge_pct.astype("double"))
			curr.withColumn("ambient_air_temperature_degc", curr.ambient_air_temperature_degc.astype("double"))
			curr.withColumn("unitID", curr.unitID.astype("long"))
			curr.withColumn("RptTimestamp", curr.RptTimestamp.astype("long"))
			curr.withColumn("m_vs", curr.m_vs.astype("double"))
			curr.withColumn("battery_available_energy_wh", curr.battery_available_energy_wh.astype("long"))
			currRdd = curr.rdd.map(lambda x: (x['battery_state_of_charge_pct'], x['ambient_air_temperature_degc'], \
				x['unitID'], x['RptTimestamp'], x['m_vs'], x['battery_available_energy_wh']))
			rdd = rdd.union(currRdd)
		except:
			print("Error happens while unioning file" + f)


# columns = ["m_vs", "ambient_air_temperature_degc", "unitID", "battery_available_energy_wh", "battery_state_of_charge_pct", "RptTimestamp"]


df = spark.createDataFrame(rdd, df.schema)
for col in df:
	df = df.filter(col != 0)

my_window = Window.partitionBy("unitID").orderBy("RptTimestamp")
df = df.withColumn("prev_RptTimestamp", F.lag(df.RptTimestamp).over(my_window))
df = df.withColumn("capacity", df.battery_available_energy_wh / df.battery_state_of_charge_pct)
df = df.withColumn("prev_Capacity", F.lag(df.capacity).over(my_window))
df = df.withColumn("velocity", F.when(F.isnull(df.prev_RptTimestamp), 0).otherwise((df.prev_Capacity - df.capacity) / (df.RptTimestamp - df.prev_RptTimestamp)))
velocity_df = df.filter("velocity != 0").na.drop()
mvs_df = velocity_df.groupBy("m_vs").mean('velocity').orderBy("m_vs")
temparature_df = velocity_df.groupBy("ambient_air_temperature_degc").mean('velocity').orderBy("ambient_air_temperature_degc")
mvs_df.rdd.saveAsTextFile("hdfs://localhost:9000/LE/Result/m_vs")
temparature_df.rdd.saveAsTextFile("hdfs://localhost:9000/LE/Result/temparature")
