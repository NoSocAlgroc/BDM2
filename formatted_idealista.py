from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

dir="data/idealista"
spark = SparkSession.builder.appName("ReadFiles").getOrCreate()
files=[f for f in os.scandir(dir) if f.is_dir()]

def reduce(a,b):
    return a[:4]+tuple(map(sum,zip(a[4:],b[4:])))

joinedRDD=None

for file in files:
    incomeRDD = spark.read.parquet(file.path).rdd
    year=file.name.split("_")[0]

    def idealistaChangeType(x):
        keyCols=["district","neighborhood"]
        numCols=["bathrooms","numPhotos","price","priceByArea","latitude","longitude","distance"]
        boolCols=["exterior","has360","has3DTour","hasLift","hasPlan","hasStaging","hasVideo","newDevelopment","showAddress","topNewDevelopment"]
        return (tuple((x[c] if c in x else "None" for c in keyCols))+(int(year),),
                tuple((float(x[c]) for c in numCols))+
                tuple((x[c]==True for c in boolCols))+
                tuple((1,))
                )
    incomeRDD=incomeRDD.map(idealistaChangeType)

    incomeRDD=incomeRDD.reduceByKey(reduce)

    if joinedRDD is None:
        joinedRDD=incomeRDD
    else:
        joinedRDD=joinedRDD.union(incomeRDD)
joinedRDD=joinedRDD.reduceByKey(reduce)
joinedRDD=joinedRDD.mapValues(lambda x: tuple((v/x[-1] for v in x[:-1]))+x[-1:])

#Lookups

lookupDir="data/lookup_tables"
distRDD = spark.read.json(os.path.join(lookupDir,"income_lookup_district.json")).rdd
neigRDD = spark.read.json(os.path.join(lookupDir,"income_lookup_neighborhood.json")).rdd


distRDD=distRDD.map(lambda x:((x['district'],),(x['_id'],x['district_name'])))
neigRDD=neigRDD.map(lambda x:((x['neighborhood'],),(x['_id'],x['neighborhood_name'])))

def flattenValues(x):
    k,(v1,v2)=x
    return (k,v1+v2) if v2 is not None else (k,(v1+(None,None)))
def neighKey(x):#(d,n,y),(v)
    k,v=x
    return ((k[1],),(k[0],k[2])+v)#(n),(d,y,v)

def distKey(x):#(n),(d,y,v,nID,nName)
    k,v=x
    return ((v[0],),(k[0],)+v[1:])#(d),(n,y,v,nID,nName)

def fullKey(x):#(d),(n,y,v,nID,nName,dID,dName)
    k,v=x
    return (k+v)#(d,n,y,v,nID,nName,dID,dName)
    #return ((v[-2],v[-4],v[1],v[2:-4],v[0],v[-3],k[0],v[-1]))#(dID,nID,y,v,n,nName,d,dName)


joinedRDD=joinedRDD.map(neighKey).leftOuterJoin(neigRDD).map(flattenValues)
joinedRDD=joinedRDD.map(distKey).leftOuterJoin(distRDD).map(flattenValues)
joinedRDD=joinedRDD.map(fullKey)

keyCols=["district","neighborhood"]
numCols=["bathrooms","numPhotos","price","priceByArea","latitude","longitude","distance"]
boolCols=["exterior","has360","has3DTour","hasLift","hasPlan","hasStaging","hasVideo","newDevelopment","showAddress","topNewDevelopment"]

# Define the schema for your DataFrame
schema = StructType([
    StructField(col, StringType(), True) for col in keyCols
] + [
    StructField('year', IntegerType(), True)
] + [
    StructField(col, FloatType(), True) for col in numCols
] + [
    StructField(col, FloatType(), True) for col in boolCols
] + [
    StructField('count', IntegerType(), True),
    StructField('neighborhoodID', StringType(), True),
    StructField('neighborhood_name', StringType(), True),
    StructField('districtID', StringType(), True),
    StructField('district_name', StringType(), True)
])

# Create DataFrame with the specified schema
df = spark.createDataFrame(joinedRDD, schema)
df.write.mode("overwrite").parquet("hdfs://10.4.41.64:27000/user/bdm/formatted/idealista/idealista_year_district")