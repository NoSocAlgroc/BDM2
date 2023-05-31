from pyspark.sql import SparkSession

# Crear una instancia de SparkSession
spark = SparkSession.builder.appName("ReadFiles").getOrCreate()

# Leer archivos JSON en el primer directorio y crear un DataFrame de Spark
json_df1 = spark.read.json("./P2_data/income_opendata/income_opendata_neighborhood.json") #INCOME_DATA --> rentas
print("json_df1")
print(json_df1)

# convert the DataFrame to an RDD
json_rdd1 = json_df1.rdd

###3#### MAKE TRANSFORMATIONS on the json and parquet files

##########Income data
print("income_data")
json_df1.show(n=2)

print("income_data rdd")
# Take the first two lines of the RDD
first_two_lines = json_rdd1.take(2)

# Print the first two lines
for line in first_two_lines:
    print(line)

# Flatten the rows to extract desired columns
json_rdd1 = json_rdd1.flatMap(lambda row: [
    (row._id, row.district_id, row.district_name, info.RFD, info.pop, info.year, row["neigh_name "])
    for info in row.info
])

print(" ")
print("flattened_json_rdd1")
print(json_rdd1)

# Take the first two lines of the RDD
first_two_lines = json_rdd1.take(2)

# Print the first two lines
for line in first_two_lines:
    print(line)
