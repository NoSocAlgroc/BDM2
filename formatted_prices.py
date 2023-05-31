from pyspark.sql import SparkSession

# Crear una instancia de SparkSession
spark = SparkSession.builder.appName("ReadFiles").getOrCreate()

# Leer archivos JSON y crear otro DataFrame de Spark                       #PRICE_OPENDATA --> precio casas
json_df4 = spark.read.json("/Users/Marta1/PycharmProjects/BDM2/price_opendata/price_opendata_neighborhood.json")
print("json_df4")
print(json_df4)


# convert the DataFrame to an RDD
json_rdd4 = json_df4.rdd

###3#### MAKE TRANSFORMATIONS on the json and parquet files

##########price_opendata_neighborhood
print("price_opendata_neighborhood")
json_df4.show(n=2)

print("price_opendata_neighborhood rdd")
# Take the first two lines of the RDD
first_two_lines = json_rdd4.take(2)

# Print the first two lines
for line in first_two_lines:
    print(line)

json_rdd4 = json_rdd4.flatMap(lambda row: [
    (row._id, row.district_id, row.district_name, info.Amount, info.diffAmount, info.diffPerMeter, info.usedAmount, info.usedPerMeter, info.year, row["neigh_name "])
    for info in row.info
])

print("flattened_json_rdd4 rdd")
# Take the first two lines of the RDD
first_two_lines = json_rdd4.take(2)

# Print the first two lines
for line in first_two_lines:
    print(line)