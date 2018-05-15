# PySpark and SparkSQL

## Apache Spark Ecosystem
- **SparkSQL + DataFrames**
- Spark Streaming
- MLlib (Machine Learning)
- GraphX (Network Analysis)  

https://databricks.com/spark/about

## Setting Python Version 
Change Python version for PySpark to Python 3.X (instead of default Python 2.7)  
For Python 3.5 `export PYSPARK_PYTHON=/sw/lsa/centos7/python-anaconda3/created-20170424/bin/python`  
For Python 3.6 `export PYSPARK_PYTHON=/sw/dsi/centos7/x86-64/Anaconda3-5.0.1/bin/python`

## Documentation
The latest Spark documentation can be found at https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html

### Introduction Spark Overview
Let's look at the *Overview* section. You should take away a couple of things from the Spark Overview:
1. RDD (Resilient Distributed Dataset). The *resilient* part alludes to the fact that it can automatically recover from node failures. The *distributed* part refers to the fact that your data is partitioned across nodes in the cluster and will be operated on in parallel.
2. Spark performs in-memory computation. It does not write/read intermediate results to disk.

### APIs
Spark has API bindings to Scala, Java, Python and R. The official documentation page shows code snippets for the first three languages (sorry , R users).

The Spark Python API documentation can be found at https://spark.apache.org/docs/2.2.0/api/python/index.html

## Pros/Cons
Advantages: Relatively fast and can work with TB of data  
Disadvantages: Readability and debugging spark messages is a pain

# PySpark Interactive Shell
The interactive shell is analogous to a Jupyter Notebook. This command starts up the interactive shell for PySpark.   
`pyspark --master yarn --queue default`

The interactive shell does not start with a clean slate. It already has a couple of objects defined for you.  
`sc` is a SparkContext and `sqlContext` is as self-described. Making your own SparkContext will not work. 

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
from pyspark.sql import SQLContext, SparkSession

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

spark = SparkSession.builder \
     .master("local") \
     .appName("Word Count") \
     .config("spark.some.config.option", "some-value") \
     .getOrCreate()
```
**Note:** You can only have ONE SparkContext running at once

## Data
As with all data analysis, you can either:
1. Create data from scratch
2. Read it in from an external data source

Our goal is to get it into a RDD and eventually a DataFrame

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
# PySpark Cheat Sheets
DataCamp has created a cheat sheet for PySpark DataFrames
https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf

They also have one for PySpark RDDs
https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_Cheat_Sheet_Python.pdf

# File I/O

## Reading Files
PySpark can create RDDs from any storage source supported by Hadoop. We'll work with text files and another format called parquet.

## Text Files
Read in text file into a RDD
```
filename = 'bsm.txt'
lines = sc.textFile(filename)
```
This method is more powerful than that though. You can pass also pass it a:
- directory `/alex/foldername`
- use wildcards `/alex/data[0-7]*`
- compressed files `alex.txt.gz`
- comma-separated list of any of the above `/another/folder, /a/specific/file`

They will automatically be stored into a single RDD 

Parse each row specifying delimiter

`columns = lines.map(lambda x: x.split(','))`

Create a RDD of `Row` objects
```
from pyspark.sql import Row
table = columns.map(lambda x: Row(RxDevice=int(x[0]), FileId=int(x[1]), Gentime=int(x[3]), Latitude=float(x[7]),      Longitude=float(x[8]), Elevation=float(x[9]), Speed=float(x[10]), Heading=float(x[11]), Yawrate=float(x[15])) )
```
Create a DataFrame from RDD of `Row` objects and view
```
bsm = sqlContext.createDataFrame(table)
bsm
bsm.show(5)
```
**Note:** Columns are now in alphabetical order and not in order constructed. Theoretically, column order makes no difference. Practically and visually, sometimes it does.

To reorder columns, you actually have to create a new dataframe using the `select` method.
```
columns = ['RxDevice','FileId','Gentime','Longitude','Latitude','Elevation','Speed','Heading','Yawrate']
BSM = bsm.select(columns)
BSM.show(5)
```

### Parquet Files
Parquet is a column-store data format in Hadoop. They consist of a set of files in a folder.
```
foldername = '41300'
df = sqlContext.read.parquet(foldername)
```

## Writing Files
Documentation for the `df.write` method is located at http://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.csv

File formats available for saving the DataFrame are:
1. csv (really any delimiter)
2. json
3. parquet w/ snappy
4. ORC w/ snappy

### CSV
```
BSM.write.csv('alexander', sep=',', header=True)
```
The result is a folder called `alexander` that has multiple csv files within it using the comma delimiter (which is the default). The number of files should be the same as the number of partitions. You can check this number by using the method `rdd.getNumPartitions()`.

The other file formats have similar notation. I've added the `mode` method to `overwrite` the folder. You can also `append` the DataFrame to existing data. These formats will also have multiple files within it.

```
BSM.write.mode('overwrite').json('alexander')
BSM.write.mode('overwrite').parquet('alexander')
BSM.write.mode('overwrite').orc('alexander')
```

**Tip:** There is a `text` method also but I do NOT recommend using it. It can only handle a one column DataFrame of type string. Use the `csv` method instead.

### Reducing Partitions
Recall that you can retrieve the number of partitions with the method  
`BSM.rdd.getNumPartitions()`

You can use the `coalesce` method to return a new DataFrame that has exactly *N* partitions.
```
N = 20
BSM.coalesce(N).write.mode('overwrite').parquet('alexander')
```
The result is still a folder called *alexander* but this time with *N* files.

### `coalesce` vs `repartition`
There is a also a `repartition` method to do something similiar. One difference is that with `repartition`, the number of partitions can be increased/decreased, but with `coalesce` the number of partitions can only be decreased. `coalesce` is better than `repartition` since it avoids a **full shuffle** of the data. `coalesce` moves data off the extra nodes onto the kept nodes.
```
df = BSM.repartition(3)
```

# Spark SQL
Spark SQL is a Spark module for structured data processing.

The latest SQL programming guide can be found at https://spark.apache.org/docs/latest/sql-programming-guide.html

You can perform SQL queries on Spark DataFrames after you register them as a table

### Set up a Temp Table
`df.registerTempTable('Bsm')`
OR
`sqlContext.registerDataFrameAsTable(df, "Bsm")`

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

To get the number of rows in the resulting DataFrame, use the `count` method.  
`records.count()`

# Spark DataFrames
If you are familiar with pandas or R DataFrames, you can forget about SQL and just use the DataFrame equivalent methods.
A DataFrame is equivalent to a relational table in Spark SQL.

## Selecting Data
`A = df.select('longitude','latitude','elevation')`  
**Note:** This is probably the easiest way to reorder existing columns but it creates a new DataFrame

## Renaming Columns
There are multiple ways to rename columns. Here are three ways using the `alias`, `selectExpr`, `withColumnRenamed` methods.
```
from pyspark.sql.functions import *
A1 = A.select(col('longitude').alias('lon'), col('latitude').alias('lat'), 'elevation' )
A2 = A.selectExpr("longitude as lon", "latitude as lat", "elevation")
A3 = A.withColumnRenamed('longitude','lon') # one column at a time
```

**Tip:** Parquet does not like column names to have any of the following characters `,;{}()=` in addition to spaces, tab \t, and newline \n characters. You might still get same error after renaming it. Not sure why. Better to take care of it before uploading data file.

## Filtering Rows
To filter rows based on a criteria use the `filter` method. `where` can also be used as it is an alias for `filter`.  
`df_filter = df.filter('Longitude < -84').where('Latitude > 43')`

To check if the DataFrame is correct, we can use the `agg` method along with the `min`,`max` functions.
```
df_filter.describe(['Longitude','Latitude']).show()
lonlat = df_filter.agg(max('Longitude'), min('Latitude') )
lonlat.show()
```

## Column Info
To get a list of column names use `df.columns` (same as pandas).  
To get info about the schema of the DataFrame, `df.printSchema()` of `df.dtypes` like in pandas

## Merging Data
A = BSM.select('RxDevice','FileId','Gentime','Longitude','Latitude','Elevation').persist()  
B = BSM.select('RxDevice','FileId','Gentime','Heading','Yawrate','Speed').persist()  
C = A.join(B, on=['RxDevice','FileId','Gentime'], how='inner')

## Replacing Values
Suppose you want to replace the number 10 with the value 3.
```
newdf = df.replace(10, 3, ['RxDevice'])
newdf.show()
```

## Group By Method
`df.groupBy(['RxDevice','FileId']).count()`

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

## Converting to DateTime Format
```
from pyspark.sql.functions import from_unixtime
df.select('Gentime', (from_unixtime(df["Gentime"] / 1000000).alias('Newtime')) )
```

## Applying A Function to a Dataframe
```
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

f1 = udf(lambda x: len(str(x)), IntegerType()) # if the function returns an int
# Using the knowledge we've gained so far, 
newdf = df.withColumn("newColumn", f1("Speed"))
newdf.show()
# Alternatively
S = df.select('Speed', f1("Speed").alias("newCol"))
S.show()
```
`udf` stands for user defined function.

## Duplicates
`dedupe = df.drop_duplicates(['RxDevice','FileId'])`

## Reshaping Data
No built-in method like `pd.melt`
Check this stackoverflow answer for a homebrew solution https://stackoverflow.com/questions/41670103/pandas-melt-function-in-apache-spark

## Crosstabs
`df.crosstab('RxDevice','FileId').show()`
OutOfMemoryError: Java heap space

## Comparison
SQL|DataFrame
---|---
`SELECT COUNT(*) as Rows FROM Bsm`|`Rows = Bsm.count()`
`SELECT DISTINCT RxDevice, FileId FROM Bsm ORDER BY RxDevice DESC, FileId`| 
`SELECT RxDevice, COUNT(DISTINCT FileId) as Trips FROM Bsm GROUP BY RxDevice HAVING Trips > 10`|
`SELECT * FROM Bsm WHERE Latitude BETWEEN 42.0 and 42.5 AND Longitude BETWEEN -84.0 and -83.5`|

## Physical Plan
You can use the `explain` method to look at the plan PySpark has made. Two different set of codes can result in the same plan.
For example, we want to do something to every column in the DataFrame
https://medium.com/@mrpowers/performing-operations-on-multiple-columns-in-a-pyspark-dataframe-36e97896c378
```
code1
```
is equivalent to
```
code2
```
in terms of its plan.

So the takeaway sometime, is to write the code version that is easier to read.

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

## Exit PySpark Interactive Shell
Type `exit()` or press Ctrl-D

