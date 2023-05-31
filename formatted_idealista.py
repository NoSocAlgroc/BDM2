from pyspark.sql import SparkSession
import os

dir="data/idealista"
spark = SparkSession.builder.appName("ReadFiles").getOrCreate()
files=[f for f in os.scandir(dir) if f.is_dir()]

def reduce(a,b):
    return a[:4]+tuple(map(sum,zip(a[4:],b[4:])))

joined=None

for file in files:
    rdd = spark.read.parquet(file.path).rdd
    year=file.name.split("_")[0]

    def idealistaChangeType(x):
        strCols=["country","province","municipality","neighborhood"]
        numCols=["bathrooms","numPhotos","price","priceByArea","latitude","longitude","distance"]
        boolCols=["exterior","has360","has3DTour","hasLift","hasPlan","hasStaging","hasVideo","newDevelopment","showAddress","topNewDevelopment"]
        return ((x["district"],
                 year), 
                tuple((x[c] if c in x else None for c in strCols))+
                tuple((float(x[c]) for c in numCols))+
                tuple((x[c]==True for c in boolCols))+
                tuple((1,))
                )
    rdd=rdd.map(idealistaChangeType)

    rdd=rdd.reduceByKey(reduce)

    if joined is None:
        joined=rdd
    else:
        joined=joined.union(rdd)
joined=joined.reduceByKey(reduce)
joined=joined.mapValues(lambda x: x[:4]+tuple((v/x[-1] for v in x[4:-1]))+x[-1:])