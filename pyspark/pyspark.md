# PySpark and SparkSQL

## Table of Contents
- [PySpark and SparkSQL](#pyspark-and-sparksql)
  - [Table of Contents](#table-of-contents)
  - [Apache Spark Ecosystem](#apache-spark-ecosystem)
  - [Documentation](#documentation)
    - [Introduction Spark Overview](#introduction-spark-overview)
    - [APIs](#apis)
  - [UM Hadoop Cluster](#um-hadoop-cluster)
  - [Using Python 3](#using-python-3)
- [PySpark Interactive Shell](#pyspark-interactive-shell)
  - [Exit Interactive Shell](#exit-interactive-shell)
  - [Data](#data)
- [PySpark Cheat Sheets](#pyspark-cheat-sheets)
- [File IO](#file-io)
  - [Reading Files](#reading-files)
  - [Text Files](#text-files)
    - [Parquet Files](#parquet-files)
    - [ORC Files](#orc-files)
  - [Writing Files](#writing-files)
    - [Parquet, ORC, JSON, CSV](#parquet-orc-json-csv)
  - [Setting Number of Partitions](#setting-number-of-partitions)
    - [Reducing Partitions](#reducing-partitions)
    - [`coalesce` vs `repartition`](#coalesce-vs-repartition)
  - [Persistence](#persistence)
- [Spark SQL](#spark-sql)
    - [Set up a Temp Table](#set-up-a-temp-table)
    - [SQL Queries](#sql-queries)
    - [Drop a Temp Table](#drop-a-temp-table)
- [Spark DataFrames](#spark-dataframes)
  - [Row Count](#row-count)
  - [Column Info](#column-info)
  - [Selecting Rows](#selecting-rows)
  - [Selecting Columns](#selecting-columns)
  - [Descriptive Statistics](#descriptive-statistics)
  - [Count Distinct Rows](#count-distinct-rows)
  - [Renaming Columns](#renaming-columns)
  - [Adding Columns](#adding-columns)
  - [Deleting Columns](#deleting-columns)
  - [Applying A Function to a Dataframe](#applying-a-function-to-a-dataframe)
  - [Replacing Values](#replacing-values)
  - [Dropping Duplicates](#dropping-duplicates)
  - [Merging Data](#merging-data)
  - [Grouping Data](#grouping-data)
  - [Sorting Data](#sorting-data)
  - [Converting to DateTime Format](#converting-to-datetime-format)
  - [Binning Data](#binning-data)
  - [Adding File Source Column](#adding-file-source-column)
  - [Physical Plan](#physical-plan)
  - [Miscellaneous Methods](#miscellaneous-methods)
  - [Listing files in HDFS to iterate](#listing-files-in-hdfs-to-iterate)
  - [Running PySpark as a Script](#running-pyspark-as-a-script)
- [Exercises](#exercises)
- [Spark UI](#spark-ui)
- [Spark Version](#spark-version)
- [Using Jupyter Notebook with PySpark](#using-jupyter-notebook-with-pyspark)

## Apache Spark Ecosystem
- **SparkSQL + DataFrames**
- Spark Streaming
- MLlib (Machine Learning)
- GraphX (Network Analysis)  

https://databricks.com/spark/about

## Documentation
The relevant Spark documentation can be found at https://spark.apache.org/docs/2.2.1/rdd-programming-guide.html

### Introduction Spark Overview
Let's look at the *Overview* section. You should take away a couple of things from the Spark Overview:
1. RDD (Resilient Distributed Dataset). The *resilient* part alludes to the fact that it can automatically recover from node failures. The *distributed* part refers to the fact that your data is partitioned across nodes in the cluster and will be operated on in parallel.
2. Spark performs in-memory computation. It does not write/read intermediate results to disk.

### APIs
Spark has API bindings to **Scala, Java, Python and R**. The official documentation page shows code snippets for the first three languages (sorry, **R** users).

The Spark Python API documentation can be found at https://spark.apache.org/docs/2.2.1/api/python/index.html.  We will mostly deal with the `pyspark.sql` module https://spark.apache.org/docs/2.2.1/api/python/pyspark.sql.html.

## UM Hadoop Cluster
For Cavium:
SSH to `cavium-thunderx.arc-ts.umich.edu` `Port 22` using a SSH client (e.g. PuTTY on Windows) and login using your Cavium account and two-factor authentication.

**Note:** ARC-TS has a [Getting Started with Hadoop User Guide](http://arc-ts.umich.edu/new-hadoop-user-guide/)

## Using Python 3
The current default python version for pyspark is 2.7. To set it up for Python 3, type the following in the terminal before launching the shell.

`export PYSPARK_PYTHON=/sw/dsi/aarch64/centos7/python/3.7.4/bin/python3`

# PySpark Interactive Shell
The interactive shell is analogous to a python console. The following command starts up the interactive shell for PySpark with default settings in the `workshop` queue.  
`pyspark --master yarn --queue default`

The following line adds some custom settings.  The 'XXXX' should be a number between 4040 and 4150.  
`pyspark --master yarn --queue default --num-executors 5 --executor-cores 5 --executor-memory 1g --conf spark.ui.port=XXXX`

**Note:** You might get a warning message that looks like `WARN Utils: Service 'SparkUI' could not bind on port 4XXX. Attempting port 4YYY.` This usually resolves itself after a few seconds. If not, try again at a later time.

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

You can go to http://cavium-rm01.arc-ts.umich.edu:8088/cluster/scheduler for a look at the status of the Cavium cluster including your spark jobs.

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
Use the `textFile` method to read in a text file into a RDD. The dataset we'll be using is from connected vehicles transmitting their information. A sample of the data can be seen [here](sample.csv).
```
filename = '/var/cscar-spark-workshop/bsm_sm.txt' # workshop directory
lines = sc.textFile(filename, sc.defaultParallelism)
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

### ORC Files
ORC (Optimized Row Columnar) is a columnar data format in Hadoop. They consist of a set of files in a folder.
```
folder = 'uniqname'
df = sqlContext.read.orc(folder)
```
**Tip:** Take a moment to compare how much faster the computation for `df.count()` is on the same dataframe if you read it in from a parquet/orc file format instead of a csv file.

## Writing Files
Documentation for the `df.write` method is located at http://spark.apache.org/docs/2.2.1/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.csv

File formats available for saving the DataFrame are:
1. parquet w/ snappy compression
2. ORC w/ snappy compression
3. json
4. csv (really any delimiter)

### Parquet, ORC, JSON, CSV
The file formats all have similar notation. I've added the `mode` method to `overwrite` the folder. You can also `append` the DataFrame to existing data. These formats will also have multiple files within it.
```
folder = 'uniqname'
df.write.mode('overwrite').parquet(folder)
df.write.mode('overwrite').orc(folder)
df.write.mode('overwrite').json(folder)
df.write.mode('overwrite').csv(folder, sep=',', header=True)
```
The result is a folder called `uniqname` that has multiple files within it. The number of files should be the same as the number of partitions. You can check this number by using the method `df.rdd.getNumPartitions()`.

**Tip:** There is a `text` method also but I do NOT recommend using it. It can only handle a one column DataFrame of type string. Use the `csv` method instead or better yet `parquet`.

**Tip:** Spark does NOT like it when you try to overwrite a folder that you read your data from. For example
```
alex = sqlContext.read.parquet('originalfolder')
alex.write.mode('overwrite').parquet('originalfolder')
```
You will get a `java.io.FileNotFoundException: File does not exist:` error message when you try to do this.

## Setting Number of Partitions
You can set your number of partitions during file input with the `textFile` method by providing an optional second argument (`minSplits`). We did this earlier by specifying `sc.defaultParallelism` as our argument. By default, Spark creates one partition for each block of the file (blocks being 128MB by default in HDFS), but you can also ask for a higher number of partitions by passing a larger value. Note that you can not have fewer partitions than blocks. If you specify fewer partitions than blocks, it will default to the number of blocks.

### Reducing Partitions
Recall that you can retrieve the number of partitions with the method  
`df.rdd.getNumPartitions()`

You can use the `coalesce` method to return a new DataFrame that has exactly *N* partitions.
```
N = 5
df.coalesce(N).write.mode('overwrite').parquet('coalescent')
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
Now there should be no waiting. You can use the `unpersist` method to manually remove a RDD or wait for Spark to automatically drop out older data partitions.

# Spark SQL
Spark SQL is a Spark module for structured data processing.  
The latest SQL programming guide can be found at https://spark.apache.org/docs/latest/sql-programming-guide.html.  
You can perform SQL queries on Spark DataFrames after you register them as a table.

Before we move forward, let's make sure we are working with the large version of the file
```
folder = '/var/cscar-spark-workshop/large'
df = sqlContext.read.parquet(folder)
df.count() # 155785661
```

### Set up a Temp Table
To create a temporary table for SQL queries, you can use either the `registerTempTable` or `sqlContext.registerDataFrameAsTable` method.

`df.registerTempTable('Bsm')` OR `sqlContext.registerDataFrameAsTable(df, 'Bsm')`

### SQL Queries
Then you can start querying the table like a regular database using SQL. BTW, I also run a Intro SQL workshop for CSCAR.
```
records = sqlContext.sql('SELECT COUNT(*) as Rows FROM Bsm')
trips = sqlContext.sql('SELECT DISTINCT RxDevice, FileId FROM Bsm ORDER BY RxDevice, FileId DESC')
driver_trips = sqlContext.sql('SELECT RxDevice, COUNT(DISTINCT FileId) as Trips FROM Bsm GROUP BY RxDevice HAVING Trips > 60')
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
To extract the values from the resulting dataframe, use the `collect` method to convert to a list of Rows like so:
```
rowlist = trips.collect()
value = rowlist[1]['RxDevice'] # specify row, column
```

### Drop a Temp Table
To drop a temporary table after creation, use the `dropTempView` method and specify the name of the temp table `spark.catalog.dropTempView('Bsm')`

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
```python
summary = subset.describe(['Longitude','Latitude'])
summary.show()
```

## Count Distinct Rows
The `countDistinct` method will return the number of distinct values in the set of columns. Similar to SQL syntax `COUNT DISTINCT(column)`.
```python
from pyspark.sql.functions import countDistinct

unique_rows1 = subset.agg(countDistinct('Longitude').alias('unique_longitude')).persist()
unique_rows1.show()
unique_rows2 = subset.agg(countDistinct("Longitude","Latitude").alias('unique_gps'))
unique_rows2.show()
# you can also use countDistinct(df.Longitude, df.Latitude)
```

## Renaming Columns
There are multiple ways to rename columns. Here are three ways using the `withColumnRenamed`, `alias`, `selectExpr`  methods.
```python
from pyspark.sql.functions import col

rename1 = subset.withColumnRenamed('Longitude','lon').withColumnRenamed('latitude','lat')
rename2 = subset.select(col('Longitude').alias('lon'), col('Latitude').alias('lat'))
rename3 = subset.selectExpr('longitude as lon', 'latitude as lat',)
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

string_length = udf(lambda x: len(str(x)), 'int') # if the function returns an int
newdf = df.withColumn('lengthYawrate', string_length('Yawrate') )
newdf.show()
```

OR
```
newdf = df.select('Yawrate', string_length("Yawrate").alias("lengthYaw"))
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
The syntax is the same as for `pandas` with the exception that the argument must be a list except when considering all the columns.
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

## Grouping Data
To group by column values, use the `groupBy` method.
```python
counts = df.groupBy(['RxDevice','FileId']).count()
counts.show()
```

## Sorting Data
To sort by columns, use the `orderBy` method or its alias `sort`.
```python
counts_sorted = counts.orderBy(["count", "RxDevice"], ascending=[True, False])
counts_sorted.show()
```

## Converting to DateTime Format
`Gentime` is in units of microseconds so we divide by a million to convert to seconds. The epoch for `Gentime` is in 2004 instead of 1970 so we add the necessary seconds to account for this.

`from_unixtime` will return a date as a string type and in the current system time zone
```python
from pyspark.sql.functions import from_unixtime

timedf = df.select('Gentime', (from_unixtime(df['Gentime'] / 1000000 + 1072929024).alias('ts')) )
timedf.show()
```

`to_timestamp` will return a date as a timestamp type
```python
from pyspark.sql.functions import to_timestamp

timedf = df.select('Gentime', to_timestamp(from_unixtime(df['Gentime'] / 1000000 + 1072929024),'yyyy-MM-dd HH:mm:ss').alias('ts'))
timedf.show()
```

`year`, `month`, `dayofmonth` are some of the functions available to extract specific time attributes.
```python
from pyspark.sql.functions import year, month, dayofmonth

timedf = timedf.withColumn('year', year('ts')).withColumn('month', month('ts')).withColumn('day', dayofmonth('ts'))
```

`from_utc_timestamp` can be used to convert to a specific timezone besides UTC. 
```python
from pyspark.sql.functions import from_utc_timestamp

timedf = timedf.withColumn('GentimeEST', from_utc_timestamp('ts', 'America/New_York'))
```
Reference for datetime format: https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html

## Binning Data
The Bucketizer function will bin your continuous data into ordinal data. 
```python
from pyspark.ml.feature import Bucketizer

buck = Bucketizer(inputCol='Heading', splits=[-1, 45, 135, 225, 315, 361], outputCol='bins')
dfbins = buck.transform(df)
dfbins.show()
```
**Note:** The imperfection of the bin size is due to data quality.

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

## Adding File Source Column
If you want to add the file source for each row when you read in several files at once, you can use the `input_file_name` function
```python
from pyspark.sql.functions import input_file_name

df.withColumn('filename', input_file_name())
```

## Physical Plan
You can use the `explain` method to look at the plan PySpark has made. Different sets of code can result in the same plan.
Suppose we want to round all applicable columns to 1 decimal place.  
**Note:** DataFrame `int` columns are not affected by rounding. 

Here are several ways we can do it.
1. `for` loop with `withColumn`
2. `list` comprehension with `select`
3. `reduce` function with `withColumn`

This PySpark code
```python
from pyspark.sql.functions import round

df1 = df
for column in df1.columns:
	df1 = df1.withColumn(column, round(df[column],1))
```
is equivalent to
```python
df2 = df.select([round(df[column],1) for column in df.columns])
```
is equivalent to
```python
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

## Listing files in HDFS to iterate
If you want to get a list of files on HDFS, here's one example to list all the files from Jan 2020.
```python
import subprocess
import re

cmd = 'hdfs dfs -ls /data/twitter/decahose/2020/decahose.2020-01*'
files = subprocess.check_output(cmd, shell=True)
files = files.decode('utf-8')
flist = files.split('\n')
regex = re.compile('decahose\.2020.+\.bz2')
filelist = []
for x in flist:
    if 'decahose' not in x: continue
    match = re.search(regex,x)
    filelist.append(match.group(0) if match else print(x))

print('files: {}'.format(len(filelist)))
```
We expect there to be 62 files (two per day) but we see 60 files. Closer inspection reveals Jan 28th is missing data.

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

filename = '/var/cscar-spark-workshop/bsm_sm.txt' # workshop directory
lines = sc.textFile(filename)
columns = lines.map(lambda x: x.split(','))
table = columns.map(lambda x: Row(RxDevice=int(x[0]), FileId=int(x[1]), Gentime=int(x[3]), Latitude=float(x[7]), Longitude=float(x[8]), Elevation=float(x[9]), Speed=float(x[10]), Heading=float(x[11]), Yawrate=float(x[15])) )
bsm = sqlContext.createDataFrame(table)
columns = ['RxDevice','FileId','Gentime','Longitude','Latitude','Elevation','Speed','Heading','Yawrate']
df = bsm.select(columns)
folder = 'uniqname'
df.write.mode('overwrite').orc(folder)
```
Submit the Spark job through the command line like this.  
`spark-submit --master yarn --queue workshop --num-executors 5 --executor-memory 5g --executor-cores 4 job.py`

# Exercises
1. Return the number of points in the area with latitude in [43,44] and longitude in [-84,-83].
2. Create a two column DataFrame that returns a unique set of device-trip ids (RxDevice, FileId) sorted by RxDevice in ascending order and then FileId in descending order.
3. Create a two column DataFrame that returns two columns (RxDevice, Trips) for RxDevices with more than 60 trips.

# Spark UI
This is a GUI to see active and completed Spark jobs.

On campus Ports 4040-4150 open for spark UI
http://cavium-thunderx.arc-ts.umich.edu:4050

# Spark Version
You can check the current version of Spark using `sc.version` OR if you are outside of the PySpark interactive shell `spark-shell --version`.

# Using Jupyter Notebook with PySpark
**Note**: Jupyter notebooks do not run on the Cavium cluster. They currently only run on the login node. This means only the login node's resources (i.e. less resources than the cluster) are available to the Jupyter notebook.

1. Open a command prompt/terminal in Windows/Mac. You should have PuTTY in your PATH (for Windows).  Port 8889 is arbitrarily chosen.  The first localhost port is for your local machine. The second localhost port is for Cavium. They do not necessarily have to be the same.  
`putty.exe -ssh -L localhost:8889:localhost:8889 cavium-thunderx.arc-ts.umich.edu` (Windows)  
`ssh -L localhost:8889:localhost:8889 cavium-thunderx.arc-ts.umich.edu` (Mac/Linux)
2. This should open a ssh client for Cavium. Log in as usual.
3. From the Cavium terminal, type the following (replace XXXX with number between 4040 and 4150):
```bash
export PYSPARK_PYTHON=/sw/dsi/aarch64/centos7/python/3.7.4/bin/python3  # set Python version to 3.X ; default is 2.7
export PYSPARK_DRIVER_PYTHON=jupyter  
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port=8889'  # same as second port listed above
pyspark --master yarn --queue default --num-executors 5 --executor-memory 1g --conf spark.ui.port=XXXX
```
4. Copy/paste the URL (from your terminal where you launched jupyter notebook) into your browser. The URL should look something like this but with a different token.
http://localhost:8889/?token=745f8234f6d0cf3b362404ba32ec7026cb6e5ea7cc960856  
If the first localhost port is different from the second, then change the url to match the first port number in order for Jupyter notebook to show up.

5. You should be connected.
