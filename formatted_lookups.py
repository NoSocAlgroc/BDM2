from pyspark.sql import SparkSession

# Crear una instancia de SparkSession
spark = SparkSession.builder \
    .config("spark.jars", "./postgresql-42.6.0.jar") \
    .appName("BDM2") \
    .getOrCreate()

# Leer archivos JSON en el segundo directorio y crear otro DataFrame de Spark      #LOOKUP
json_df2 = spark.read.json("/Users/Marta1/PycharmProjects/BDM2/data/lookup_tables/income_lookup_district.json")
json_df3 = spark.read.json("/Users/Marta1/PycharmProjects/BDM2/data/lookup_tables/income_lookup_neighborhood.json")
print("json_df2")
print(json_df2)
print("json_df3")
print(json_df3)

# convert the DataFrame to an RDD
json_rdd2 = json_df2.rdd
json_rdd3 = json_df3.rdd

###3#### MAKE TRANSFORMATIONS on the json and parquet files

##########Lookup data
print("lookup_district")
json_df2.show(n=2)

print("lookup_district rdd")
# Take the first two lines of the RDD
first_two_lines = json_rdd2.take(2)

# Print the first two lines
for line in first_two_lines:
    print(line)

from pyspark.sql import Row
def transform_row(row):
        # Extracting _id, district, district_name, district_reconciled
        _id = row._id
        district = row.district
        district_name = row.district_name
        district_reconciled = row.district_reconciled

        # Creating a new row for each neighborhood_id value
        transformed_rows = []
        for neighborhood_id in row.neighborhood_id:
                transformed_rows.append(Row(_id=_id, district=district, district_name=district_name,
                                            district_reconciled=district_reconciled, neighborhood_id=neighborhood_id))

        return transformed_rows


# Applying the transformation to each row
json_rdd2 = json_rdd2.flatMap(transform_row)

print("lookup_district rdd")
# Take the first two lines of the RDD
first_two_lines = json_rdd2.take(2)

# Print the first two lines
for line in first_two_lines:
    print(line)


##########Lookup data
print("lookup_neighborhood")
json_df3.show(n=2)

print("lookup_neighborhood rdd")
# Take the first two lines of the RDD
first_two_lines = json_rdd3.take(2)

# Print the first two lines
for line in first_two_lines:
    print(line)