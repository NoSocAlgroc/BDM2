from pyspark.sql import SparkSession
import os

# Crear una instancia de SparkSession
spark = SparkSession.builder \
    .config("spark.jars", "./postgresql-42.6.0.jar") \
    .appName("BDM2") \
    .getOrCreate()

jsonrdd= spark.read.json("data/income_opendata/income_opendata_neighborhood.json").rdd

def extractYears(x):
    res=[]
    if x[3] is None:
        return []
    for r in x[3]:
        tr=tuple(r)
        res+=[(x[2:3]+x[4:]+tr[2:],r[:2])]#(district_name,RFD,pop,year,neigh_name)
    return res
jsonrdd=jsonrdd.flatMap(extractYears)




lookupDir="data/lookup_tables"
distRDD = spark.read.json(os.path.join(lookupDir,"income_lookup_district.json")).rdd
neigRDD = spark.read.json(os.path.join(lookupDir,"income_lookup_neighborhood.json")).rdd


distRDD=distRDD.map(lambda x:((x['district_reconciled'],),(x['_id'],x['district_name'])))
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

jsonrdd=jsonrdd.map(neighKey).leftOuterJoin(neigRDD).map(flattenValues)
jsonrdd=jsonrdd.map(distKey).leftOuterJoin(distRDD).map(flattenValues)
jsonrdd=jsonrdd.map(fullKey)

df=spark.createDataFrame(jsonrdd, ['district', 'neighborhood', 'year', 'rfd', 'population', 'neighborhoodID','neighborhood_name','districtID','district_name'])
df.write.mode("overwrite").parquet("hdfs://10.4.41.64:27000/user/bdm/formatted/income/income_year_district")