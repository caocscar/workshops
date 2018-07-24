# PySpark and SparkSQL

## Table of Contents
- [Apache Spark Ecosystem](#apache-spark-ecosystem)
- [Flux Hadoop Cluster](#flux-hadoop-cluster)
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
     - [Row Count](#row-count)
     - [Column Info](#column-info)
     - [Selecting Rows](#selecting-rows)
     - [Selecting Columns](#selecting-columns)
     - [Descriptive Statistics](#descriptive-statistics)
     - [Renaming Columns](#renaming-columns)
     - [Adding Columns](#adding-columns)
     - [Deleting Columns](#deleting-columns)
     - [Replacing Values](#replacing-values)
     - [Applying a Function to a DataFrame](#applying-a-function-to-a-dataframe)
     - [Dropping Duplicates](#dropping-duplicates)
     - [Merging Data](#merging-data)
     - [Group By Method](#group-by-method)
     - [Persistence](#persistence)
     - [Converting to Datetime Format](#converting-to-datetime-format)
     - [Binning Data](#binning-data)
- [SQL vs DataFrame Comparison](#comparison)
- [Physical Plan](#physical-plan)
- [Miscellaneous Methods](#miscellaneous-methods)   
- [Running PySpark as a Script](#running-pyspark-as-a-script)

## Apache Spark Ecosystem
- **SparkSQL + DataFrames**
- Spark Streaming
- MLlib (Machine Learning)
- GraphX (Network Analysis)  

https://databricks.com/spark/about

## Documentation
The relevant Spark documentation can be found at https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html

### Introduction Spark Overview
Let's look at the *Overview* section. You should take away a couple of things from the Spark Overview:
1. RDD (Resilient Distributed Dataset). The *resilient* part alludes to the fact that it can automatically recover from node failures. The *distributed* part refers to the fact that your data is partitioned across nodes in the cluster and will be operated on in parallel.
2. Spark performs in-memory computation. It does not write/read intermediate results to disk.

### APIs
Spark has API bindings to **Scala, Java, Python and R**. The official documentation page shows code snippets for the first three languages (sorry, **R** users).

The Spark Python API documentation can be found at https://spark.apache.org/docs/2.2.0/api/python/index.html.  We will mostly deal with the `pyspark.sql` module https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html.

## Pros/Cons
Advantages: Relatively fast and can work with TB of data  
Disadvantages: Readability and debugging Spark messages is a pain

## Flux Hadoop Cluster
SSH to `flux-hadoop-login.arc-ts.umich.edu` `Port 22` using a SSH client (e.g. PuTTY on Windows) and login using your Flux account and 2FA.

**Note:** ARC-TS has a [Getting Started with Hadoop User Guide](http://arc-ts.umich.edu/new-hadoop-user-guide/)

### Setting Python Version 
Change Python version for PySpark to Python 3.6 (instead of default Python 2.7)  
`export PYSPARK_PYTHON=/sw/dsi/centos7/x86-64/Anaconda3-5.0.1/bin/python`

# PySpark Interactive Shell
The interactive shell is analogous to a python console. The following command starts up the interactive shell for PySpark with default settings in the `workshop` queue. 
`pyspark --master yarn --queue workshop`

The following line adds some custom settings.  
`pyspark --master yarn --queue workshop --num-executors 20 --executor-memory 5g --executor-cores 4`

**Note:** You might get a warning message that looks like `WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.` This usually resolves itself after a few seconds. If not, try again at a later time.

The interactive shell does not start with a clean slate. It already has several objects defined for you. 
- `sc` is a SparkContext
- `sqlContext` is a SQLContext object
- `spark` is a SparkSession object

You can check this by typing the variable names.  
```
sc
sqlContext
spark
```

## Exit Interactive Shell
Type `exit()` or press Ctrl-D

## Data
As with all data analysis, you can either:
1. Read it in from an external data source.
2. Create data from scratch. (Practically, you should never have to do this on Spark. If you do, you're probably doing it wrong).

Our goal is to get data into a RDD and eventually a DataFrame.

# PySpark Cheat Sheets
DataCamp has created a cheat sheet for PySpark DataFrames
https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf

They also have one for PySpark RDDs
https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_Cheat_Sheet_Python.pdf

We'll cover some basic stuff on RDD at the end.

# File IO

## Reading Files
PySpark can create RDDs from any storage source supported by Hadoop. We'll work with text files and another format called *parquet*.

## Text Files
Use the `textFile` method to read in a text file into a RDD. The dataset we'll be using is from connected vehicles transmitting their information.
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
table = columns.map(lambda x: Row(RxDevice=int(x[0]), FileId=int(x[1]), Gentime=int(x[3]), Latitude=float(x[7]), Longitude=float(x[8]), Elevation=float(x[9]), Speed=float(x[10]), Heading=float(x[11]), Yawrate=float(x[15])) )
```
Create a DataFrame from RDD of `Row` objects and view using the `show` method.
```
bsm = sqlContext.createDataFrame(table)
bsm
bsm.show(5)
```
OR

`bsm = table.toDF()`

**Note:** Columns are now in alphabetical order and not in order constructed.

To restore columns to the original order, you actually have to create a new dataframe using the `select` method.
```
columns = ['RxDevice','FileId','Gentime','Longitude','Latitude','Elevation','Speed','Heading','Yawrate']
df = bsm.select(columns)
df.show(5)
```

### Parquet Files
Parquet is a column-store data format in Hadoop. They consist of a set of files in a folder.
```
folder = 'uniqname'
df = sqlContext.read.parquet(folder)
```

## Writing Files
Documentation for the `df.write` method is located at http://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.csv

File formats available for saving the DataFrame are:
1. csv (really any delimiter)
2. json
3. parquet w/ snappy compression
4. ORC w/ snappy compression

### CSV
```
folder = 'uniqname'
df.write.csv(folder, sep=',', header=True)
```
The result is a folder called `uniqname` that has multiple csv files within it using the comma delimiter (which is the default). The number of files should be the same as the number of partitions. You can check this number by using the method `bsm.rdd.getNumPartitions()`.

### Parquet, JSON, ORC
The other file formats have similar notation. I've added the `mode` method to `overwrite` the folder. You can also `append` the DataFrame to existing data. These formats will also have multiple files within it.
```
df.write.mode('overwrite').parquet(folder)
df.write.mode('overwrite').json(folder)
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
The result is still the same folder but this time with *N* files.

### `coalesce` vs `repartition`
There is a also a `repartition` method to do something similiar. One difference is that with `repartition`, the number of partitions can  increase/decrease, but with `coalesce` the number of partitions can only decrease. `coalesce` is better than `repartition` in this sense since it avoids a **full shuffle** of the data. `coalesce` just moves data off the extra nodes onto the kept nodes.
```
df_repartition = df.repartition(4)
df_repartition.rdd.getNumPartitions()
df_repartition.show(5)
```
**Note**: If we try to do another `show` command, it will recompute the *df_repartition* dataframe. 

## Persistence
Use the `persist` method to save the computation you performed to prevent it from being re-computed.
```
df_repartition = df.repartition(4).persist()
df_repartition.show(7)
df_repartition.show(2)
```
Now there should be no waiting.

# Spark SQL
Spark SQL is a Spark module for structured data processing.  
The latest SQL programming guide can be found at https://spark.apache.org/docs/latest/sql-programming-guide.html.  
You can perform SQL queries on Spark DataFrames after you register them as a table.

### Set up a Temp Table
To create a temporary table for SQL queries, you can use either the `registerTempTable` or `sqlContext.registerDataFrameAsTable` method.

`df.registerTempTable('Bsm')` OR `sqlContext.registerDataFrameAsTable(df, 'Bsm')`

### SQL Queries
Then you can start querying the table like a regular database using SQL. BTW, I also run a Intro SQL workshop for CSCAR.
```
records = sqlContext.sql('SELECT COUNT(*) as Rows FROM Bsm')
trips = sqlContext.sql('SELECT DISTINCT RxDevice, FileId FROM Bsm ORDER BY RxDevice DESC, FileId')
driver_trips = sqlContext.sql('SELECT RxDevice, COUNT(DISTINCT FileId) as Trips FROM Bsm GROUP BY RxDevice HAVING Trips > 10')
area = sqlContext.sql('SELECT * FROM Bsm WHERE Latitude BETWEEN 42.4 and 42.5 AND Longitude BETWEEN -83.6 and -83.5')
```
The result is always a DataFrame.  
**Note:** No computation has been evaluated. Spark commands are evaluated lazily (i.e. when they are needed).
```
records.show() # or records.persist().show()
trips.show() # or trips.persist().show()
driver_trips.show() # or driver_trips.persist().show()
area.show() # or area.persist().show()
```

# Spark DataFrames
If you are familiar with **pandas** or **R** DataFrames, you can alternatively forget about SQL and just use the DataFrame equivalent methods. A DataFrame is equivalent to a relational table in Spark SQL.

## Row Count
To get the number of rows in a DataFrame, use the `count` method.  
`trips.count()`

## Column Info
To get a list of column names use `df.columns` (same as pandas).  
To get info about the schema of the DataFrame, `df.printSchema()` or `df.dtypes` like in pandas or just `df`

## Selecting Rows
To select rows based on a criteria use the `filter` method. `where` can also be used as it is an alias for `filter`.  
```
df_filter = df.filter('Longitude < -84').where('Latitude > 43')
df_filter.show()
```
## Selecting Columns
To select a subset of columns, use the `select` method with the order of column names that you want.  
`subset = df.select('longitude','latitude','elevation')`  

You can also pass it a list of column names as we did previously.
```python
cols = ['longitude','latitude','elevation']
subset = df.select(cols)
```

**Note:** For some reason, column names are not case sensitive

## Descriptive Statistics
The `describe` method will return the following values for you for each numeric column: count, mean, standard deviation, minimum, and maximum.
```
summary = df_filter.describe(['Longitude','Latitude'])
summary.show()
```
## Renaming Columns
There are multiple ways to rename columns. Here are three ways using the `withColumnRenamed`, `alias`, `selectExpr`  methods.
```
from pyspark.sql.functions import col
rename1 = subset.withColumnRenamed('Longitude','lon').withColumnRenamed('latitude','lat')
rename2 = subset.select(col('Longitude').alias('lon'), col('Latitude').alias('lat'), 'elevation' )
rename3 = subset.selectExpr('longitude as lon', 'latitude as lat', 'elevation')
rename1.show()
```
**Tip:** I recommend using `withColumnRenamed` since the other methods will only return the columns selected.

**Tip:** Parquet does not like column names to have any of the following characters `,;{}()=` in addition to spaces, tab `\t`, and newline `\n` characters. You might still get same error after renaming it. Not sure why. Better to take care of it before uploading data file.

## Adding Columns
To initialize with a constant
```python
from pyspark.sql.functions import lit

newdf = df.withColumn('newcol', lit(7) )
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
## Applying A Function to a Dataframe
Define a function that returns the length of the column value. 
```python
from pyspark.sql.functions import udf

f1 = udf(lambda x: len(str(x)), 'int') # if the function returns an int
newdf = df.withColumn('lengthYawrate', f1('Yawrate') )
newdf.show()
```
OR
```
newdf = df.select('Yawrate', f1("Yawrate").alias("lengthYaw"))
newdf.show()
```
**Note**: `udf` stands for user defined function.

## Replacing Values
Suppose you want to replace the RxDevice and FileId with other arbitrary values.
```
newvalues = df.replace(10, 3, ['RxDevice']).replace(922233, 99, 'FileId')
newvalues.show()
```
## Dropping Duplicates
The syntax is the same as for `pandas`.
```
dedupe = df.drop_duplicates(['RxDevice','FileId'])
dedupe.show()
```
## Merging Data
You can merge two DataFrames using the `join` method. The `join` method works similar to the `merge` method in `pandas`. You specify your left and right DataFrames with the `on` argument and `how` argument specifying which columns to merge on and what kind of join operation you want to perform, respectively.

If you want to merge on two columns with different names, you would use the following syntax.  
`[Left.first_name == Right.given_name, Left.last_name == Right.family_name]`  
and pass the list to the `on` argument.

The different how (join) options are: `inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti.`
The default join is `inner` as with most programs.
```
Left = df.select('RxDevice','FileId','Gentime','Heading','Yawrate','Speed')
Right = df.select('RxDevice','FileId','Gentime','Longitude','Latitude','Elevation')
Merge = Left.join(Right, on=['RxDevice','FileId','Gentime'], how='inner')
Merge.show(5)
```
Now, here is a merge example where the column names are different and the same. You should get a warning about merging identically named columns this way.  `WARN Column: Constructing trivially true equals predicate, 'FileId#1L = FileId#1L'. Perhaps you need to use aliases.` We'll see the issues with it shortly.
```
R = Right.withColumnRenamed('Gentime','T')
merge_expr = [Left.Gentime == R.T, Left.FileId == Right.FileId]
M = Left.join(R, on=merge_expr)
M.show(5)
```
We can see there are some duplicate columns in the merged DataFrame. This is a result of columns have the same name in each of the DataFrame. This will cause trouble down the road if you try to select a duplicate column.  
`M.select('FileId')`

You will get an error that looks something like this.  
`Reference 'FileId' is ambiguous, could be: FileId#1L, FileId#159L.`

You can't drop the duplicate columns or rename them because they have the same name and you can't reference them by index like in `pandas`.

So when you are merging on columns that have some matching and non-matching names, the best solution I can find is to rename the columns so that they are either all matching or all non-matching. You should also rename any column names that are the same in the Left and Right DataFrame that are not part of the merge condition otherwise you will run into the same issue.

## Group By Method
Similar to `pandas` group by method.
```
counts = df.groupBy(['RxDevice','FileId']).count()
counts.show()
```
## Converting to DateTime Format
`Gentime` is in units of microseconds so we divide by a million to convert to seconds. The epoch for `Gentime` is in 2004 instead of 1970 so we add the necessary seconds to account for this.
```python
from pyspark.sql.functions import from_unixtime

timedf = df.select('Gentime', (from_unixtime(df['Gentime'] / 1000000 + 1072929024).alias('DateTime')) )
timedf.show()
```
## Binning Data
The Bucketizer function will bin your continuous data into ordinal data.
```python
from pyspark.ml.feature import Bucketizer

buck = Bucketizer(inputCol='Heading', splits=[-1, 45, 135, 225, 315, 361], outputCol='bins')
dfbins = buck.transform(df)
dfbins.show()
```
You can check the result with SparkSQL
```
dfbins.registerTempTable('table')
records = sqlContext.sql('SELECT bins, COUNT(*) as ct FROM table GROUP BY bins ORDER BY bins')
records.show()
```
Alternatively, you check the result with DataFrames by creating a contingency table using the `crosstab` method against a constant column.
```
dfbins = dfbins.withColumn('constant', lit(77) )
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

This PySpark code
```
from pyspark.sql.functions import round
df1 = df
for column in df1.columns:
	df1 = df1.withColumn(column, round(df[column],1))
```
is equivalent to
```
df2 = df.select([round(df[column],1) for column in df.columns])
```
is equivalent to
``` python
from functools import reduce

df3 = reduce(lambda df,column: df.withColumn(column, round(df[column],1)), df.columns, df)
```
Looking at the plan Spark has laid out for each of the variables, it is equivalent in terms of its output (except column name) and execution plan.
```
df1.explain()
df2.explain()
df3.explain()
```
So the takeaway is, sometime you don't have to worry about optimizing code because PySpark re-interprets it in the background. So good programming practice would dictate making the code as easy to read as possible.

## Miscellaneous Methods
There are a lot of methods available. A list of them are here http://spark.apache.org/docs/latest/api/python/pyspark.sql.html

**Tip:** The Spark version on Flux Hadoop is updated every time maintenance is performed. When you look at the Spark documentation, make sure you are looking up docs for the same version (and not necessarily the latest version). 

You can check the current version of Spark using `SparkContext.version` or `sc.version` OR if you are outside of the PySpark interactive shell `spark-shell --version`.

## Running PySpark as a Script
If you don't run PySpark through the interactive shell but rather as a Python script. You will need some additional code at the top of your script. 

The first thing you must do is create a `SparkContext()` object. This tells Spark how to access a cluster. `SparkConf()` is where you can set the configuration for your Spark application.

To get to the same starting point as the interactive shell, you need to start with these additional lines.
```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
```
**Note:** You can only have ONE SparkContext running at once. Making your own SparkContext will not work in the interactive shell since one already exists at startup.

So an example script would look like this.
```
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

conf = SparkConf().setAppName('Workshop Ex')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

filename = 'TripStart_41300_sm.txt'
lines = sc.textFile(filename)
columns = lines.map(lambda x: x.split(','))
table = columns.map(lambda x: Row(RxDevice=int(x[0]), FileId=int(x[1]), Gentime=int(x[3]), Latitude=float(x[7]), Longitude=float(x[8]), Elevation=float(x[9]), Speed=float(x[10]), Heading=float(x[11]), Yawrate=float(x[15])) )
bsm = sqlContext.createDataFrame(table)
columns = ['RxDevice','FileId','Gentime','Longitude','Latitude','Elevation','Speed','Heading','Yawrate']
df = bsm.select(columns)
folder = 'alexander'
df.write.mode('overwrite').orc(folder)
```

Submit the Spark job through the command line like this.  
`spark-submit --master yarn --num-executors 20 --executor-memory 5g --executor-cores 4 job.py`

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
