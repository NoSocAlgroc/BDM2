from pyspark.sql import SparkSession

# Crear una instancia de SparkSession
spark = SparkSession.builder.appName("ReadFiles").getOrCreate()

# Leer archivos Parquet y crear un DataFrame de Spark
parquet_df = spark.read.parquet("/Users/Marta1/PycharmProjects/BDM2/data/idealista/2020_01_02_idealista/") #IDEALISTA --> alquileres
print("parquet_df")
print(parquet_df)
parquet_df.show(n=2)

# convert the DataFrame to an RDD
parquet_rdd = parquet_df.rdd

###3#### MAKE TRANSFORMATIONS on the json and parquet files
###### IDEALISTA
# idealista → dia i barri  → agrupar per any

# How many columns has the RDD
sample_rows = parquet_rdd.take(1)
num_columns = len(sample_rows[0])
print("num_columns")
print(num_columns)

# Distinct values from x[30]
column_rdd = parquet_rdd.map(lambda x: x[30])
distinct_values = column_rdd.distinct().collect()
print("distinct_values")
for value in distinct_values:
    print(value)

# Change type of variables and remove some of them
def idealistaChangeType(x):
    return (x[1],
            x[2], #country
            int(x[4]) if isinstance(x[4], str) else x[4],  #distance
            x[5],  #distric
            x[6],
            int(x[8]) if isinstance(x[8], str) else x[8],  #floor
            x[9],
            x[10],
            x[11],
            x[12],
            x[13],
            x[14],
            x[15],
            x[16],
            x[17],  # municipality
            x[18],  # neighborhood
            x[19],
            x[20],
            x[22],
            x[23],
            x[26], #province
            x[27],
            x[28],
            x[29],
            x[30]=="good", # status
            x[33],
            )

parquet_rdd = parquet_rdd.map(idealistaChangeType)
print("parquet rdd")

# Take the first two lines of the RDD
first_two_lines = parquet_rdd.take(2)

# Print the first two lines
for line in first_two_lines:
    print(line)

# group by barri
parquet_rdd = parquet_rdd.groupBy(lambda x: (x[1], x[3], x[14], x[15], x[20])) #country, district, municipality, neighborhood, province

# Calculate the mean of integer columns and percentage of true values for boolean columns
#def process_row(rows):
  #  index_num_cols = [1, 2]
    #for i in index_num_cols:
     #   total = 0.0
      #  for row in rows:
       #     total += row[i]
#
 #       mean = total/len(rows)
#
 #   return mean

#processed_rdd = parquet_rdd.map(process_row)
