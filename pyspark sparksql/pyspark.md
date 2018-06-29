# PySpark and SparkSQL

## Table of Contents
- [Apache Spark Ecosystem](#apache-spark-ecosystem)
- [Setting Python Version](#setting-python-version)
- [PySpark Interactive Shell](#pyspark-interactive-shell)
     - [Exit Shell](#exit-interactive-shell)
- [PySpark Cheat Sheets](#pyspark-cheat-sheets)
- [File I/O](#file-io)
     - [Reading Files](#reading-files)
     - [Writing Files](#writing-files)
- [Spark SQL](#spark-sql)
     - [Set up a Temp Table](#set-up-a-temp-table)
     - [SQL Queries](#sql-queries)
 - [Spark DataFrames](#spark-dataframes)   
     - [Selecting Data](#selecting-data)
     - [Renaming Columns](#renaming-columns)
     - [Filtering Rows](#filtering-rows)
     - [Descriptive Statistics](#descriptive-statistics)
     - [Column Info](#column-info)
     - [Merging Data](#merging-data)
     - [Replacing Values](#replacing-values)
     - [Group By Method](#group-by-method)
     - [Adding Columns](#adding-columns)
     - [Deleting Columns](#deleting-columns)
     - [Converting to Datetime Format](#converting-to-datetime-format)
     - [Applying a Function to a DataFrame](#applying-a-function-to-a-dataframe)
     - [Dropping Duplicates](#dropping-duplicates)
     - [Binning Data](#binning-data)
- [SQL vs DataFrame Comparison](#comparison)
- [Physical Plan](#physical-plan)
- [Miscellaneous Methods](#miscellaneous-methods)
    

## Apache Spark Ecosystem
- **SparkSQL + DataFrames**
- Spark Streaming
- MLlib (Machine Learning)
- GraphX (Network Analysis)  

https://databricks.com/spark/about

## Setting Python Version 
Change Python version for PySpark to Python 3.6 (instead of default Python 2.7)  
`export PYSPARK_PYTHON=/sw/dsi/centos7/x86-64/Anaconda3-5.0.1/bin/python`

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
The interactive shell is analogous to a Jupyter Notebook. This command starts up the interactive shell for PySpark. The first example line of code starts the shell with the default settings. The second example line starts the shell with custom settings.
```
pyspark --master yarn --queue workshop    
pyspark --master yarn --queue workshop --num-executors 20 --executor-memory 5g --executor-cores 4
```
The interactive shell does not start with a clean slate. It already has a couple of objects defined for you.  
`sc` is a SparkContext and `sqlContext` is as self-described. Making your own SparkContext will not work. 

You can check this by looking at the variable type.  
```python
type(sc)
type(sqlContext)
```

## Exit Interactive Shell
Type `exit()` or press Ctrl-D

## Data
As with all data analysis, you can either:
1. Create data from scratch (Practically, you should never have to do this on Spark. If you do, you're probably doing it wrong.)
2. Read it in from an external data source

Our goal is to get data into a RDD and eventually a DataFrame

# PySpark Cheat Sheets
DataCamp has created a cheat sheet for PySpark DataFrames
https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf

They also have one for PySpark RDDs
https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_Cheat_Sheet_Python.pdf

# File IO

## Reading Files
PySpark can create RDDs from any storage source supported by Hadoop. We'll work with text files and another format called parquet.

## Text Files
Read in text file into a RDD
```
filename = 'TripStart_41300_sm.txt'
lines = sc.textFile(filename)
```
This method is more powerful than that though. You can also use:
- a directory `/alex/foldername`
- wildcards `/alex/data[0-7]*`
- compressed files `alex.txt.gz`
- comma-separated list of any of the above `/another/folder, /a/specific/file`

They will automatically be stored into a single RDD.

Next, parse each row specifying delimiter

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
**Note:** Columns are now in alphabetical order and not in order constructed.

To reorder columns, you actually have to create a new dataframe using the `select` method.
```
columns = ['RxDevice','FileId','Gentime','Longitude','Latitude','Elevation','Speed','Heading','Yawrate']
df = bsm.select(columns)
df.show(5)
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
folder = 'alexander'
df.write.csv(folder, sep=',', header=True)
```
The result is a folder called `alexander` that has multiple csv files within it using the comma delimiter (which is the default). The number of files should be the same as the number of partitions. You can check this number by using the method `rdd.getNumPartitions()`.

The other file formats have similar notation. I've added the `mode` method to `overwrite` the folder. You can also `append` the DataFrame to existing data. These formats will also have multiple files within it.
```
df.write.mode('overwrite').json(folder)
df.write.mode('overwrite').parquet(folder)
df.write.mode('overwrite').orc(folder)
```
**Tip:** There is a `text` method also but I do NOT recommend using it. It can only handle a one column DataFrame of type string. Use the `csv` method instead.

### Reducing Partitions
Recall that you can retrieve the number of partitions with the method  
`df.rdd.getNumPartitions()`

You can use the `coalesce` method to return a new DataFrame that has exactly *N* partitions.
```
N = 5
df.coalesce(N).write.mode('overwrite').parquet(folder)
```
The result is still a folder called *alexander* but this time with *N* files.

### `coalesce` vs `repartition`
There is a also a `repartition` method to do something similiar. One difference is that with `repartition`, the number of partitions can be increased/decreased, but with `coalesce` the number of partitions can only be decreased. `coalesce` is better than `repartition` since it avoids a **full shuffle** of the data. `coalesce` moves data off the extra nodes onto the kept nodes.
```
df_repartition = df.repartition(4)
```

# Spark SQL
Spark SQL is a Spark module for structured data processing.

The latest SQL programming guide can be found at https://spark.apache.org/docs/latest/sql-programming-guide.html.

You can perform SQL queries on Spark DataFrames after you register them as a table.

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
area = sqlContext.sql('SELECT * FROM Bsm WHERE Latitude BETWEEN 42.4 and 42.5 AND Longitude BETWEEN -83.6 and -83.5')
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

To get the number of rows in a DataFrame, use the `count` method.  
`trips.count()`

# Spark DataFrames
If you are familiar with pandas or R DataFrames, you can alternatively forget about SQL and just use the DataFrame equivalent methods.
A DataFrame is equivalent to a relational table in Spark SQL.

## Selecting Data
`subset = df.select('longitude','latitude','elevation')`  
**Note:** For some reason, column names are not case sensitive

## Renaming Columns
There are multiple ways to rename columns. Here are three ways using the `alias`, `selectExpr`, `withColumnRenamed` methods.
```
from pyspark.sql.functions import *
rename1 = subset.select(col('Longitude').alias('lon'), col('Latitude').alias('lat'), 'elevation' )
rename2 = subset.selectExpr('longitude as lon', 'latitude as lat', 'elevation')
rename3 = subset.withColumnRenamed('Longitude','lon').withColumnRenamed('latitude','lat')
```

**Tip:** Parquet does not like column names to have any of the following characters `,;{}()=` in addition to spaces, tab \t, and newline \n characters. You might still get same error after renaming it. Not sure why. Better to take care of it before uploading data file.

## Filtering Rows
To filter rows based on a criteria use the `filter` method. `where` can also be used as it is an alias for `filter`.  
`filter = df.filter('Longitude < -84').where('Latitude > 43')`

## Descriptive Statistics
To check if the DataFrame is correct, we can use the `agg` method along with the `min`,`max` functions.
```
summary = df_filter.describe(['Longitude','Latitude'])
summary.show()
lonlat = df_filter.agg(max('Longitude'), min('Latitude') )
lonlat.show()
```

## Column Info
To get a list of column names use `df.columns` (same as pandas).  
To get info about the schema of the DataFrame, `df.printSchema()` or `df.dtypes` like in pandas or just `df`

## Merging Data
```
Left = df.select('RxDevice','FileId','Gentime','Heading','Yawrate','Speed')
Right = df.select('RxDevice','FileId','Gentime','Longitude','Latitude','Elevation')
Merge = Left.join(Right, on=['RxDevice','FileId','Gentime'], how='inner')
```
## Replacing Values
Suppose you want to replace the number 10 with the value 3.
```
newvalues = df.replace(10, 3, ['RxDevice']).replace(922233, 99, 'FileId')
newvalues.show()
```
## Group By Method
Similar to `pandas` group by method.
```
counts = df.groupBy(['RxDevice','FileId']).count()
counts.show()
```
**Note**: If we try to do another `show` command, it will recompute the counts dataframe. This is where the `persist` method would come in handy.
```
ct = df.groupBy(['RxDevice','FileId']).count().persist()
ct.show()
ct.show(100)
```
## Adding ColumnsR.
To initialize with a constant
```
from pyspark.sql import functions as fct
newdf = df.withColumn('newcol', fct.lit(7) )
newdf.show()
```

To calculate a new column based on another one
```
newdf = newdf.withColumn('colname', df['Latitude'] - 42)
newdf.show()
```

## Deleting Columns
To drop a column, use the `drop` method.  
```
smalldf = newdf.drop('colname')
smalldf.show()
```
To drop multiple columns, you can use the `*iterator` idiom with an iterator (e.g. list). 
```
columns2delete = ['newcol','colname','elevation']
smallerdf = newdf.drop(*columns2delete)
smallerdf.show()
```

## Converting to DateTime Format
`Gentime` is in units of microseconds so we divide by a million to convert to seconds. The epoch for `Gentime` is in 2004 instead of 1970 so we add the necessary seconds to account for this.
```
from pyspark.sql.functions import from_unixtime
timedf = df.select('Gentime', (from_unixtime(df['Gentime'] / 1000000 + 1072929024).alias('DateTime')) )
timedf.show()
```

## Applying A Function to a Dataframe
Define a function that returns the length of the column value. 
```
from pyspark.sql.functions import udf

f1 = udf(lambda x: len(str(x)), 'int') # if the function returns an int
# Using the knowledge we've gained so far, 
newdf = df.withColumn('lengthYawrate', f1('Yawrate') )
newdf.show()
# Alternatively
newdf = df.select('Yawrate', f1("Yawrate").alias("lengthYaw"))
newdf.show()
```
**Note**: `udf` stands for user defined function.

## Dropping Duplicates
The syntax is the same as for `pandas`.
```
dedupe = df.drop_duplicates(['RxDevice','FileId'])
dedupe.show()
```

## Binning Data
The Bucketizer function will bin your continuous data into ordinal data.
```
from pyspark.ml.feature import Bucketizer
buck = Bucketizer(inputCol='Heading', splits=[-1, 45, 135, 225, 315, 360], outputCol='bins')
dfbins = buck.transform(df)
dfbins.show()
```
You can check the result with SparkSQL
```
dfbins.registerTempTable('table')
records = sqlContext.sql('SELECT bins, COUNT(*) as ct FROM table GROUP BY bins ORDER BY bins')
records.show()
```
Alternatively, you can create a contingency table using the `crosstab` method against a constant column.
```
dfbins = dfbins.withColumn('constant', fct.lit(77) )
contingency = dfbins.crosstab('bins','constant')
contingency.show()
```
## Comparison
SparkSQL|Spark DataFrame
---|---
`SELECT COUNT(*) as Rows FROM Bsm`|`Rows = df.count()`
`SELECT DISTINCT RxDevice, FileId FROM Bsm ORDER BY RxDevice, FileId DESC`|`df.drop_duplicates(['RxDevice','FileId']).orderBy(['RxDevice','FileId'], ascending=[True,False])`
`SELECT RxDevice, COUNT(DISTINCT FileId) as Trips FROM Bsm GROUP BY RxDevice HAVING Trips > 10`|`df.groupby('RxDevice').where('Trips > 10')`
`SELECT * FROM Bsm WHERE Speed BETWEEN 30 and 50 and Yawrate > 10`|`df.filter('Speed >= 30').filter('Speed <= 50').filter('Yawrate > 10')`
## Physical Plan
You can use the `explain` method to look at the plan PySpark has made. Different sets of code can result in the same plan.
Suppose we want to round all applicable columns to 1 decimal place.  
**Note:** DataFrame `int` columns are not affected by rounding). 

Here are several ways we can do it.
1. `for` loop
2. `list` comprehension
3. `reduce` function

This Python code
```
df1 = df
for column in df1.columns:
	df1 = df1.withColumn(column, fct.round(df[column],1))
```
is equivalent to
```
df2 = df.select(*[fct.round(df[column],1) for column in df.columns])
```
is equivalent to
```
from functools import reduce
df3 = reduce(lambda df,column: df.withColumn(column, fct.round(df[column],1)), df.columns, df)
```
in terms of its output and execution plan.
```
df1.explain()
df2.explain()
df3.explain()
```
So the takeaway is, sometime you don't have to worry about optimizing Python code because PySpark rewrites it in the background. So it good programming practice would be to write code that is easy to read.

## Miscellaneous Methods
There are a lot of methods available. A list of them are here http://spark.apache.org/docs/latest/api/python/pyspark.sql.html

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

# Prototyping with Parallelize and Collect

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
