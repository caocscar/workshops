# CSCAR Workshop on PySpark and SparkSQL
```
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0
export PYSPARK_PYTHON=/sw/lsa/centos7/python-anaconda3/created-20170424/bin/python
```

## Documentation
The latest Spark documentation can be found at https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html

### Introduction: Spark Overview
Let's look at the *Overview* section. You should take away a couple of things from the Spark Overview:
1. RDD (Resilient Distributed Dataset). The *resilient* part alludes to the fact that it can automatically recover from node failures. The *distributed* part refers to the fact that your data is partitioned across nodes in the cluster and will be operated on in parallel.
2. Spark performs in-memory computation. It does not write/read intermediate results to disk.

### APIs
Spark has API bindings to Scala, Java, Python and R. The official documentation page shows code snippets for the first three languages (sorry , R users).

The Spark Python API documentation can be found at https://spark.apache.org/docs/2.2.0/api/python/index.html

## Pros/Cons
Advantages: Fast and can work with TB of data
Disadvantages: Readability and Debugging Spark Messages is a pain

# PySpark Interactive Shell
The interactive shell is analogous to a Jupyter Notebook. This command starts up the interactive shell for PySpark.   
`pyspark --master yarn-client --queue default`

The interactive shell does not start with a clean slate. It already has a couple of objects defined for you.  
`sc` is a SparkContext and `sqlContext` is a HiveContext.

You can check this by looking at the variable type.  
```python
type(sc)
type(sqlContext)
```

## Initializing Spark
The first thing you must do is create a `SparkContext` object. This tells Spark how to access a cluster.

`SparkConf` is where you can set the configuration for your Spark application.

If you were writing a script, this would be the equivalent lines to get you to the same starting point.
```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)
```
**Note:** You can only have ONE SparkContext running at once

## Set up SQLContext
Let's create a SQLContext
`sqlc = SQLContext(sc)`

## Data
As with all data analysis, you can either:
1. Create data from scratch
2. Read it in from an external data source
The goal is to get it into a RDD and eventually a DataFrame

## Parallelized Collections
```python
data = range(1000000)
RDDdata = sc.parallelize(data)
total = RDDdata.reduce(lambda a,b: a+b)
```

## Persistence
We could also save RDDdata in memory by using the `persist` method. This saves it from being recomputed each time.
```
RDDdata_persistent = RDDdata.persist()
total = RDDdata_persistent.reduce(lambda a,b: a=b)
```
You can use the `unpersist` method to manually remove a RDD or wait for Spark to automatically drop out older data partitions.

## Broadcast Variables
Broadcast variables are read-only variables that are cached on each machine (instead of passing a copy with every task).
Below is an example of how to create one:
```
v = range(100)
bv = sc.broadcast(v)
bv.value
```
> After the broadcast variable is created, it should be used instead of the value v in any functions run on the cluster so that v is not shipped to the nodes more than once. In addition, the object v should not be modified after it is broadcast in order to ensure that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped to a new node later).

## Accumulators
> Accumulators are variables that are only “added” to through an associative and commutative operation and can therefore be efficiently supported in parallel. They can be used to implement counters (as in MapReduce) or sums.
```
counter = 0
acounter = sc.accumulator(counter)
sc.parallelize(range(100)).foreach(lambda x: counter.add(x))
counter.value
```

## Read in Different Filetypes
PySpark can create RDDs from any storage source supported by Hadoop. This includes text files. We'll work with text files and another format called parquet.

## Text Files
Read in text file into a RDD
```
filename = 'TripStart_41300.txt'
lines = sc.textFile(filename)
```
Parse each row specifying delimiter

`columns = lines.map(lambda x: x.split(','))`

Create a RDD of `Row` objects
```
from pyspark.sql import Row
table = columns.map(lambda x: Row(RxDevice=int(x[0]), FileId=int(x[1]), TxDevice=int(x[2]), Gentime=int(x[3]), Latitude=float(x[4]),      Longitude=float(x[5]), Elevation=float(x[6]), Speed=float(x[7]), Heading=float(x[8]), Yawrate=float(x[9])) )
```
Create a DataFrame from RDD of `Row` objects and view
```
bsm = sqlContext.createDataFrame(table)
bsm
bsm.show(5)
```
**Note:** Columns are now in alphabetical order and not in order constructed. Technically, column order makes no difference. Visually, sometimes it does.

### Parquet Files
Parquet is a column-store data format in Hadoop. They consist of a set of files in a folder. That's all I'm going to say about that.
```
foldername = '41300'
df = sqlContext.read.parquet(foldername)
```

# Spark SQL
Spark SQL is a Spark module for structured data processing.

The latest SQL programming guide can be found at https://spark.apache.org/docs/latest/sql-programming-guide.html

You can perform SQL queries on Spark DataFrames after you register them as a table

### Set up a Temp Table
```
df.registerTempTable('Bsm')`
sqlContext.registerDataFrameAsTable(df, "myTable")
```
### SQL Queries
Then you can start querying the table like a regular database.
```
records = sqlContext.sql('SELECT COUNT(*) as Rows FROM Bsm')
trips = sqlContext.sql('SELECT DISTINCT RxDevice, FileId FROM Bsm ORDER BY RxDevice DESC, FileId')
driver_trips = sqlContext.sql('SELECT RxDevice, COUNT(DISTINCT FileId) as Trips FROM Bsm GROUP BY RxDevice HAVING Trips > 10')
area = sqlContext.sql('SELECT * FROM Bsm WHERE Latitude BETWEEN 42.0 and 42.5 AND Longitude BETWEEN -84.0 and -83.5')
```
The result is always a DataFrame.  
**Note:** No computation has been evaluated. Spark commands are evaluated lazily (i.e. when they are needed).

To view the results, use the `show` method.
```
records.show()
trips.show()
driver_trips.show()
area.show()
```
NOTE: Maybe Move Persistence HERE

To get the number of rows in the resulting DataFrame, use the `count` method.
`records.count()`

You can also query parquet files directly using SQL bypassing the need for a DataFrame.
```
ct = sqlContext.sql('SELECT COUNT(*) as Rows FROM parquet.`{}`'.format(foldername) )
```

## Spark DataFrames
If you are familiar with pandas or R DataFrames, you can forget about SQL and just use the DataFrame equivalent methods.

## Selecting Data
```
A = df.select('longitude','latitude','elevation')
```
**Note:** This is probably the easiest way to reorder existing columns

## Renaming Columns
There are multiple ways to rename columns. Here are three ways using the `alias`, `selectExpr`, `withColumnRenamed` methods.
```
from pyspark.sql.functions import *
A1 = A.select(col('longitude').alias('lon'), col('latitude').alias('lat'), 'elevation' )
A2 = A.selectExpr("longitude as lon", "latitude as lat", "elevation")
A3 = A.withColumnRenamed('longitude','lon') # one column at a time
```

## Filtering Rows
To filter rows based on a criteria use the `filter` method. `where` can also be used as it is an alias for `filter`.  
`df_filter = df.filter('Longitude < -84').where('Latitude > 43')`

To check if the DataFrame is correct, we can use the `agg` method along with the `min`,`max` functions.
```
df_filter.describe(['Longitude','Latitude']).show()
lonlat = df_filter.agg(max('Longitude'), min('Latitude') )
lonlat.show()
```

## Binning Data
`splits` input datatype should match the `inputCol` datatype
```
from pyspark.ml.feature import Bucketizer
buck = Bucketizer(splits=[-90.0, 40.0, 42.0, 44.0, 46.0, 90.0], inputCol='Latitude', outputCol='bins')
binCol = buck.transform(df)
binCol.show()
buck.getSplits()
```
**Note**: It doesn't assign the bin label but rather the bin index

## Miscellaneous Methods
There are a lot of methods available. A list of them are here http://spark.apache.org/docs/latest/api/python/pyspark.sql.html

## Merging Data

## Replacing Values
```
newdf = df.replace(10, 3, ['RxDevice','TxDevice'])
newdf.show()
```

## Group By Method

## Adding and Deleting Columns

### Adding
To initialize with a constant
```
from pyspark.sql import functions as fct
newdf = df.withColumn('colname', fct.lit(7) )
newdf.show()
```

To calculate a new column based on another one
```
newdf = df.withColumn('colname', df['Latitude'] - 42)
newdf.show()
```

### Deleting
To drop a column, use the `drop` method.  
```
samedf = newdf.drop('colname').show()
samedf = newdf.drop(['colname','elevation']).show()
```


## Applying A Function to a Dataframe

## Duplicates
```
dedupe = df.drop_duplicates(['RxDevice','FileId'])
```
## Reshaping Data
No built-in method like `pd.melt`
Check this stackoverflow answer for a homebrew solution https://stackoverflow.com/questions/41670103/pandas-melt-function-in-apache-spark

## Converting to DateTime Format


## Crosstabs
```
df.crosstab('RxDevice','FileId').show()
```
OutOfMemoryError: Java heap space

## Comparison
SQL|DataFrame
---|---
`SELECT COUNT(*) as Rows FROM Bsm`|`Rows = Bsm.count()`
`SELECT DISTINCT RxDevice, FileId FROM Bsm ORDER BY RxDevice DESC, FileId`|
`SELECT RxDevice, COUNT(DISTINCT FileId) as Trips FROM Bsm GROUP BY RxDevice HAVING Trips > 10`|
`SELECT * FROM Bsm WHERE Latitude BETWEEN 42.0 and 42.5 AND Longitude BETWEEN -84.0 and -83.5`|


----------------------------------------

## Save DataFrame to File
```
savefile = 'trips'
trips.write.format('json').save(savefile)
```
**Note:** Saving to text requires the DataFrame only have one column that is of string type

## Exit PySpark Interactive Shell
Type `exit()` or press Ctrl-D




