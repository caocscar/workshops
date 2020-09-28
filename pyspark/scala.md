# Scala and SparkSQL
Scala stands for SCAlable LAnguage. Scale is considered to have a more steep learning curve than Python and PySpark. Its easier to learn the syntax if you are familiar with JavaScript.

## Table of Contents
- [Scala and SparkSQL](#scala-and-sparksql)
  - [Table of Contents](#table-of-contents)
  - [Apache Spark Ecosystem](#apache-spark-ecosystem)
  - [Documentation](#documentation)
    - [Introduction Spark Overview](#introduction-spark-overview)
    - [APIs](#apis)
  - [UM Hadoop Cluster](#um-hadoop-cluster)
- [Scala Interactive Shell](#scala-interactive-shell)
  - [Shell Commands](#shell-commands)
  - [Exit Interactive Shell](#exit-interactive-shell)
  - [Data](#data)
- [Scala Cheat Sheets](#scala-cheat-sheets)
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
  - [NULL Values](#null-values)
  - [Selecting Columns](#selecting-columns)
  - [Descriptive Statistics](#descriptive-statistics)
  - [Count Distinct Rows](#count-distinct-rows)
  - [Grabbing Values from DataFrame](#grabbing-values-from-dataframe)
  - [Renaming Columns](#renaming-columns)
  - [Adding Columns](#adding-columns)
  - [Deleting Columns](#deleting-columns)
  - [Applying A Function to a Dataframe](#applying-a-function-to-a-dataframe)
  - [Replacing Values](#replacing-values)
  - [Dropping Duplicates](#dropping-duplicates)
  - [Merging Data](#merging-data)
  - [Appending Data](#appending-data)
  - [Grouping Data](#grouping-data)
  - [Sorting Data](#sorting-data)
  - [Converting to DateTime Format](#converting-to-datetime-format)
  - [Binning Data](#binning-data)
  - [Adding File Source Column](#adding-file-source-column)
  - [Physical Plan](#physical-plan)
  - [Miscellaneous Methods](#miscellaneous-methods)
  - [Official Guide to Spark SQL, DataFrames, and Datasets](#official-guide-to-spark-sql-dataframes-and-datasets)
  - [Listing files in HDFS to iterate](#listing-files-in-hdfs-to-iterate)
- [Exercises](#exercises)
- [Running a Scala Script](#running-a-scala-script)
- [Spark UI](#spark-ui)
- [Spark Version](#spark-version)

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

The Spark Scala API documentation can be found at https://spark.apache.org/docs/latest/api/java/index.html?overview-summary.html.  We will mostly deal with the `org.apache.spark.sql` package https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/package-summary.html.

## UM Hadoop Cluster
For Cavium  
SSH to `cavium-thunderx.arc-ts.umich.edu` `Port 22` using a SSH client (e.g. PuTTY on Windows) and login using your Cavium account and two-factor authentication.

**Note:** ARC-TS has a [Getting Started with Hadoop User Guide](http://arc-ts.umich.edu/new-hadoop-user-guide/)

# Scala Interactive Shell
The interactive shell is analogous to a python console. The following command starts up the interactive shell for PySpark with default settings (`--num-exeuctors 1 --executor-cores 2 --exeuctor-memory 1g`) in the `default` queue.  You can type `spark-shell --help` in the terminal to see other options but also see the default settings.
`spark-shell --master yarn --queue default`

The following line adds some custom settings.  The 'XXXX' should be a number between 4040 and 4150.  
`spark-shell --master yarn --queue default --num-executors 5 --executor-cores 5 --executor-memory 1g --conf spark.ui.port=XXXX`

**Note:** You might get a warning message that looks like `WARN Utils: Service 'SparkUI' could not bind on port 4XXX. Attempting port 4YYY.` This usually resolves itself after a few seconds. If not, try again at a later time.

The interactive shell does not start with a clean slate. It already has several objects defined for you.
- `sc` is a SparkContext
- `spark` is a SparkSession object

You can check this by typing the variable names.
```scala
sc
spark
```

We can also create something called a `sqlContext` variable. You can use it in lieu of the `spark` variable.
```scala
import org.apache.spark.sql._
val sqlContext = new SQLContext(sc)
```

You will also get a message that says something like:
```
Spark context Web UI available at http://141.211.40.214:<port number>
```
Put this url in your browser to see the spark job.

You can go to http://cavium-rm01.arc-ts.umich.edu:8088/cluster/scheduler for a look at the status of the Cavium cluster including your spark jobs.

## Shell Commands
Use `:sh` preceding the shell command of interest like `:sh mkdir testfolder`

## Exit Interactive Shell
Type `:q` or `:quit`

## Data
As with all data analysis, you can either:
1. Read it in from an external data source.
2. Create data from scratch. (Practically, you should never have to do this on Spark. If you do, you're probably doing it wrong).

Our goal is to get data into a RDD and eventually a DataFrame.

# Scala Cheat Sheets
Here is a cheat sheet for Scala
https://jaxenter.com/cheat-sheet-complete-guide-scala-136558.html

And a pdf version
https://github.com/markus1189/scala-cheat-sheet/raw/f5ce79d/scala-cheat-sheet.pdf

~~We'll cover some basic stuff on RDD at the end.~~

# File IO

## Reading Files
Scala can create RDDs from any storage source supported by Hadoop. We'll work with text files and another format called *parquet*.

## Text Files
Use the `textFile` method to read in a text file into a RDD. The dataset we'll be using is from connected vehicles transmitting their information. A sample of the data can be seen [here](sample.csv).
```scala
val filename = "/var/cscar-spark-workshop/bsm_sm.txt" // workshop directory
val lines = sc.textFile(filename, sc.defaultParallelism)
```
**Note**: In Scala, the single quote character `'` is NOT equivalent to the double quote character `"` and should NOT be used for strings.

This method is more powerful than that though. You can also use:
- a directory `/alex/foldername`
- wildcards `/alex/data[0-7]*`
- compressed files `alex.txt.gz`
- comma-separated list of any of the above `/another/folder, /a/specific/file`

They will automatically be stored into a single RDD.

Next, parse each row specifying delimiter

```scala
val columns = lines.map(line => line.split(","))
```

Create a RDD of `Row` objects
```scala
import org.apache.spark.sql.Row

val table = columns.map(cols => Row(cols(0).toInt, cols(1).toInt, cols(3).toLong, cols(7).toFloat,
    cols(8).toFloat, cols(9).toFloat, cols(10).toFloat, cols(11).toFloat, cols(15).toFloat
))
```

Create a DataFrame from RDD of `Row` objects and the schema with it. View results using the `show` method.
```scala
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, FloatType}

val schema = StructType(Seq(
    StructField(name="RxDevice", dataType=IntegerType, nullable=false),
    StructField(name="FileId", dataType=IntegerType, nullable=false),
    StructField(name="Gentime", dataType=LongType, nullable=false),
    StructField(name="Latitude", dataType=FloatType, nullable=false),
    StructField(name="Longitude", dataType=FloatType, nullable=false),
    StructField(name="Elevation", dataType=FloatType, nullable=false),
    StructField(name="Speed", dataType=FloatType, nullable=false),
    StructField(name="Heading", dataType=FloatType, nullable=false),
    StructField(name="Yawrate", dataType=FloatType, nullable=false)
))
val bsm = spark.createDataFrame(table, schema)
bsm.show(5)
```

We can also skip the RDD stage and go from the file to the dataframe directly.
```scala
import org.apache.spark.sql._
val df0: DataFrame = spark.read.csv(filename)

// import spark.implicits._
val df1 = df0.map(d => {
      val cols = d.getString(0).split(",")
      (cols(0).toInt, cols(1).toInt, cols(3).toLong, cols(7).toFloat, cols(8).toFloat,
      cols(9).toFloat, cols(10).toFloat, cols(11).toFloat, cols(15).toFloat)
    })
```

The file and dataframe doesn't have any headers. If we want to add them, we need to create a new dataframe (since spark objects are immutable).
```scala
val headers = Seq("RxDevice","FileId","Gentime","Latitude","Longitude","Elevation","Speed","Heading","Yawrate")
val df = df1.toDF(headers: _*)
```

### Parquet Files
Parquet is a column-store data format in Hadoop. They consist of a set of files in a folder.
```scala
val folder = "parquetfolder"
val df = spark.read.parquet(folder)
```

### ORC Files
ORC (Optimized Row Columnar) is a columnar data format in Hadoop. They consist of a set of files in a folder.
```scala
val folder = "orcfolder"
df = spark.read.orc(folder)
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
```scala
val folder = "uniqname"
df.write.mode("overwrite").parquet(folder)
df.write.mode("overwrite").orc(folder)
df.write.mode("overwrite").json(folder)
df.write.mode("overwrite").option("header","true").option("sep",",").csv(folder)
```
The result is a folder called `uniqname` that has multiple files within it. The number of files should be the same as the number of partitions. You can check this number by using the command `df.rdd.getNumPartitions`.

**Tip:** There is a `text` method also but I do NOT recommend using it. It can only handle a one column DataFrame of type string. Use the `csv` method instead or better yet `parquet`.

**Tip:** Spark does NOT like it when you try to overwrite a folder that you read your data from. For example
```scala
val alex = sqlContext.read.parquet("originalfolder")
alex.write.mode("overwrite").parquet("originalfolder")
```
You will get a `java.io.FileNotFoundException: File does not exist:` error message when you try to do this.

## Setting Number of Partitions
By default, Spark creates one partition for each block of the file (blocks being 128MB by default in HDFS), but you can also set a higher number of partitions. Note that you can not have fewer partitions than blocks. If you specify fewer partitions than blocks, it will default to the number of blocks.

### Reducing Partitions
Recall that you can retrieve the number of partitions with the command  
`df.rdd.getNumPartitions`

You can use the `coalesce` method to return a new DataFrame that has exactly *N* partitions.
```scala
val N = 5
df.coalesce(N).write.mode("overwrite").parquet("coalescent")
```
The result is still the same folder but this time with *N* files.

### `coalesce` vs `repartition`
There is a also a `repartition` method to do something similiar. One difference is that with `repartition`, the number of partitions can  increase/decrease, but with `coalesce` the number of partitions can only decrease. `coalesce` is better than `repartition` in this sense since it avoids a **full shuffle** of the data. `coalesce` just moves data off the extra nodes onto the kept nodes.
```scala
val df_repartition = df.repartition(4)
df_repartition.rdd.getNumPartitions
df_repartition.show(5)
```
**Note**: If we try to do another `show` command, it will recompute the *df_repartition* dataframe. 

## Persistence
Use the `persist` method to save the computation you performed to prevent it from being re-computed.
```scala
val df_repartition = df.repartition(4).persist()
df_repartition.show(7)
df_repartition.show(2)
```
Now there should be no waiting. You can use the `unpersist` method to manually remove a RDD or wait for Spark to automatically drop out older data partitions.

# Spark SQL
Spark SQL is a Spark module for structured data processing.  
The latest SQL programming guide can be found at https://spark.apache.org/docs/latest/sql-programming-guide.html.  
You can perform SQL queries on Spark DataFrames after you register them as a table.

Before we move forward, let's make sure we are working with the large version of the file
```scala
val folder = "/var/cscar-spark-workshop/large"
val df = spark.read.parquet(folder)
df.count() // 155785661
```

### Set up a Temp Table
To create a temporary table for SQL queries, you can use either the `registerTempTable` or `createOrReplaceTempView` method.

`df.registerTempTable("Bsm")` OR `df.createOrReplaceTempView("Bsm")`

### SQL Queries
Then you can start querying the table like a regular database using SQL. BTW, I also run a Intro SQL workshop for CSCAR.
```scala
val records = spark.sql("SELECT COUNT(*) as Rows FROM Bsm")
val trips = spark.sql("SELECT DISTINCT RxDevice, FileId FROM Bsm ORDER BY RxDevice, FileId DESC")
val driver_trips = spark.sql("SELECT RxDevice, COUNT(DISTINCT FileId) as Trips FROM Bsm GROUP BY RxDevice HAVING Trips > 60")
val area = spark.sql("SELECT * FROM Bsm WHERE Latitude BETWEEN 42.4 and 42.5 AND Longitude BETWEEN -83.6 and -83.5")
```
The result is always a DataFrame.  
**Note:** No computation has been evaluated. Spark commands are evaluated lazily (i.e. when they are needed).
```scala
records.show() # or records.persist().show()
trips.show() # or trips.persist().show()
driver_trips.show() # or driver_trips.persist().show()
area.show() # or area.persist().show()
```
To extract the values from the resulting dataframe, use the `collect` method to convert to a list of Rows like so:
```scala
val rowlist = trips.collect()
val value = rowlist(0)(1) # specify row, column
```

### Drop a Temp Table
To drop a temporary table after creation, use the `dropTempView` method and specify the name of the temp table `spark.catalog.dropTempView("Bsm")`

# Spark DataFrames
If you are familiar with **pandas** or **R** DataFrames, you can alternatively forget about SQL and just use the DataFrame equivalent methods. A DataFrame is equivalent to a relational table in Spark SQL.

In Spark 2.0, DataFrames are just Dataset of Rows in Scala. DataFrame operations are referred to as "untyped dataset operations".

## Row Count
To get the number of rows in a DataFrame, use the `count` method.  
`trips.count()`

## Column Info
To get a list of column names use `df.columns` (same as pandas).  
To get info about the schema of the DataFrame, `df.printSchema()` or `df.dtypes` like in pandas or just `df`

## Selecting Rows
To select rows based on a criteria use the `filter` method. `where` can also be used as it is an alias for `filter`.  
```scala
val df_filter = df.filter("Longitude < -84").where("Latitude > 43")
df_filter.show()
```
This above uses two SQL expressions. Similarly, you could combine the two expressions into one.
```scala
val df_filter = df.filter("Longitude < -84 AND Latitude > 43")
df_filter.show()
```
Alternatively, you an also use the `$` (Column) notation:
```scala
val df_filter = df.filter($"Longitude" < -84).where($"Latitude" > 43)
df_filter.show()
```

## NULL Values
Use the `isNull` and `isNotNull` method to include or exclude null values.
```scala
val df_notnull = df.filter($"Longitude".isNotNull)
val df_null = df.filter($"Latitude".isNull)
```

## Selecting Columns
To select a subset of columns, use the `select` method with the order of column names that you want.  
```scala
val subset = df.select("longitude","latitude","elevation")
```
**Note:** For some reason, column names are not case sensitive

## Descriptive Statistics
The `describe` method will return the following values for you for each numeric column: count, mean, standard deviation, minimum, and maximum.
```scala
val summary = subset.describe("Longitude","Latitude")
summary.show()
```

## Count Distinct Rows
The `countDistinct` method will return the number of distinct values in the set of columns. Similar to SQL syntax `COUNT DISTINCT(column)`.
```scala
import org.apache.spark.sql.functions.countDistinct

val unique_rows1 = subset.agg(countDistinct("Longitude") as "unique_longitude").persist()
unique_rows1.show()
val unique_rows2 = subset.agg(countDistinct("Longitude","Latitude") as "unique_gps")
unique_rows2.show()
```

## Grabbing Values from DataFrame
http://spark.apache.org/docs/1.4.1/api/scala/#org.apache.spark.sql.DataFrame
To grab individual elements from a DataFrame
```scala
val result = unique_rows1.head().getLong(0)
```

## Renaming Columns
There are multiple ways to rename columns. Here are three ways using the `withColumnRenamed`, `as` methods.
```scala
val rename1 = subset.withColumnRenamed("Longitude","lon").withColumnRenamed("latitude","lat")
val rename2 = subset.select(col("Longitude").as("lon"), col("Latitude").as("lat"))
rename1.show()
```
**Tip:** I recommend using `withColumnRenamed` since the other methods will only return the columns selected.

**Tip:** Parquet does not like column names to have any of the following characters `,;{}()=` in addition to spaces, tab `\t`, and newline `\n` characters. You might still get same error after renaming it. Not sure why. Better to take care of it before uploading data file.

## Adding Columns
To initialize with a constant
```scala
import org.apache.spark.sql.functions.{lit,typedLit} 
// you can use either but typedLit is more robust and can handle List, Seq, and Map objects
val newdf = df.withColumn("newcol", typedLit(7) )
newdf.show()
```
To calculate a new column based on another one
```scala
val newcol = newdf.withColumn("colname", $"Latitude" - 42)
newcol.show()
```
**Note**: `$` is equivalent to using `col()` Ref: https://spark.apache.org/docs/2.2.0/api/java/org/apache/spark/sql/Column.html

## Deleting Columns
To drop a column, use the `drop` method.  
```scala
val smalldf = newcol.drop("colname")
smalldf.show()
```
To drop multiple columns, you can use the `_*` idiom with a sequence (e.g. `Array,List,Seq,Vector`). This is analogous to the `*` operator for unpacking in python.
```scala
val columns2delete = Seq("newcol","colname","elevation")
val smallerdf = newdf.drop(columns2delete: _*)
smallerdf.printSchema()
```

## Applying A Function to a Dataframe
Define a function that returns the length of the column value. 
```scala
import org.apache.spark.sql.functions.udf

val string_length = udf((x:Double) => x.toString.length)
val newdf = df.withColumn("lengthYawrate", string_length($"Yawrate"))
newdf.show()
```
OR
```scala
val newdf = df.select($"Yawrate", string_length($"Yawrate").alias("lengthYaw"))
newdf.show()
```
**Note**: `udf` stands for user defined function.

## Replacing Values
Suppose you want to replace the RxDevice with other arbitrary values.
```scala
val newvalues = df.withColumn("RxDevice", when(col("RxDevice") === 2767, 77).otherwise(col("RxDevice")) )
newvalues.show()
```

## Dropping Duplicates
The syntax is the same as for `pandas` with the exception that the argument must be a list except when considering all the columns.
```scala
val dropColumns = Seq("RxDevice","FileId")
val dedupe = df.dropDuplicates(dropColumns)
dedupe.show()
```

## Merging Data
You can merge two DataFrames using the `join` method. The `join` method works similar to the `merge` method in `pandas`. You specify your left and right DataFrames with the `on` argument and `how` argument specifying which columns to merge on and what kind of join operation you want to perform, respectively.

If you want to merge on two columns with different names, you would use the following syntax.  
`[Left.first_name == Right.given_name, Left.last_name == Right.family_name]`  
and pass the list to the `on` argument.

The different how (join) options are: `inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti.`
The default join is `inner` as with most programs.
```scala
val Left = df.select("RxDevice","FileId","Gentime","Heading","Yawrate","Speed")
val Right = df.select("RxDevice","FileId","Gentime","Longitude","Latitude","Elevation")
val Merge = Left.join(Right, Seq("RxDevice","FileId","Gentime"), "inner")
Merge.show(5)
```
Now, here is a merge example where the column names are different and the same. You should get a warning about merging identically named columns this way.  `WARN sql.Column: Constructing trivially true equals predicate, 'FileId#1L = FileId#1L'. Perhaps you need to use aliases.` We'll see the issues with it shortly.
```scala
val R = Right.withColumnRenamed("Gentime","T")
val M = Left.join(R, Left("Gentime") <=> R("T") && Left("FileId") <=> R("FileId"))
M.show(5)
```
We can see there are some duplicate columns in the merged DataFrame. This is a result of columns have the same name in each of the DataFrame. This will cause trouble down the road if you try to select a duplicate column.  
`M.select("FileId")`

You will get an error that looks something like this.  
`Reference 'FileId' is ambiguous, could be: FileId#1L, FileId#159L.`

You can't drop the duplicate columns or rename them because they have the same name and you can't reference them by index like in `pandas`.

So when you are merging on columns that have some matching and non-matching names, the best solution I can find is to rename the columns so that they are either all matching or all non-matching. You should also rename any column names that are the same in the Left and Right DataFrame that are not part of the merge condition otherwise you will run into the same issue.

## Appending Data
You can append two DataFrames using the `union` method. It only works if the two DataFrames have the same schema though.
```scala
val df_filter = df.filter("Latitude < 43")
val df_filter2 = df.filter("Latitude > 43")
val DF = df_filter.union(df_filter2)
```

## Grouping Data
To group by column values, use the `groupBy` method.
```scala
val counts = df.groupBy("RxDevice","FileId").count()
counts.show()
```

## Sorting Data
To sort by columns, use the `orderBy` method or its alias `sort`.
```scala
val counts_sorted = counts.orderBy($"count".asc, $"RxDevice".desc)
counts_sorted.show()
```

## Converting to DateTime Format
`Gentime` is in units of microseconds so we divide by a million to convert to seconds. The epoch for `Gentime` is in 2004 instead of 1970 so we add the necessary seconds to account for this.

`from_unixtime` will return a date as a string type and in the current system time zone
```scala
import org.apache.spark.sql.functions.from_unixtime

val timedf = df.withColumn("datetime", from_unixtime($"Gentime" / 1000000 + 1072929024))
timedf.show()
```

`to_timestamp` will return a date as a timestamp type
```scala
import org.apache.spark.sql.functions.to_timestamp

val timedf = df.withColumn("ts", to_timestamp(from_unixtime($"Gentime" / 1000000 + 1072929024)))
timedf.show()
```

`year`, `month`, `dayofmonth` are some of the functions available to extract specific time attributes.
```scala
import org.apache.spark.sql.functions.{year, month, dayofmonth}

val datepart = timedf.withColumn("year", year($"ts")).withColumn("month", month($"ts")).withColumn("day", dayofmonth($"ts"))
```

`from_utc_timestamp` can be used to convert to a specific timezone besides UTC. 
```scala
import org.apache.spark.sql.functions.from_utc_timestamp

val tz = timedf.withColumn("GentimeEST", from_utc_timestamp($"ts", "America/New_York"))
```
Reference for datetime format: https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html

## Binning Data
The Bucketizer function will bin your continuous data into ordinal data. 
```scala
import org.apache.spark.ml.feature.Bucketizer

val buck = new Bucketizer().setInputCol("Heading").setOutputCol("bins").setSplits(Array(-1, 45, 135, 225, 315, 361))
val dfbins = buck.transform(df)
dfbins.show()
val bins = dfbins.groupBy("bins").count().orderBy("bins")
bins.show()
```
**Note:** The imperfection of the bin size is due to data quality.

You can check the result with SparkSQL
```scala
dfbins.registerTempTable("table")
val records = spark.sql("SELECT bins, COUNT(*) as ct FROM table GROUP BY bins ORDER BY bins")
records.show()
```
Alternatively, you check the result with DataFrames by creating a contingency table using the `crosstab` method against a constant column.
```scala
import org.apache.spark.sql.DataFrameStatFunctions

val dfbins2 = dfbins.withColumn("constant', lit(77) )
val contingency = dfbins2.stat.crosstab("bins","constant")
contingency.show()
```

## Adding File Source Column
If you want to add the file source for each row when you read in several files at once, you can use the `input_file_name` function
```scala
import org.apache.spark.sql.functions.input_file_name

df.withColumn("filename", input_file_name).show(truncate=false)
```

## Physical Plan
You can use the `explain` method to look at the plan Scala has made. Different sets of code can result in the same plan.
Suppose we want to round all applicable columns to 1 decimal place.  
**Note:** DataFrame `int` columns are not affected by rounding. 

Here are several ways we can do it.
1. `for` loop with `withColumn`
2. `list` comprehension with `select`
3. `reduce` function with `withColumn`

This Scala code
```scala
import org.apache.spark.sql.functions.round

val df1 = df.columns.foldLeft(df){(newDF, column) => 
  newDF.withColumn(column, round(df(column), 1))
}
df1.show()
```

is equivalent to
```scala
val seq_comprehension = for (column <- df.columns) yield round(df(column),1).as(column)
val df2 = df.select(seq_comprehension: _*)
df2.show()
```

Looking at the plan Spark has laid out for each of the variables, it is equivalent in terms of its output (except column name) and execution plan.
```
df1.explain()
df2.explain()
```
So the takeaway is, sometime you don't have to worry about optimizing code because PySpark re-interprets it in the background. So good programming practice would dictate making the code as easy to read as possible.

## Miscellaneous Methods
There are a lot of methods available. A list of them are here https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html

**Tip:** The Spark version on Cavium is behind the latest version. When you look at the Spark documentation, make sure you are looking up docs for the same version (and not necessarily the latest version) or make sure the method was available in the Cavium version. 

## Official Guide to Spark SQL, DataFrames, and Datasets
Here is the official Apache Spark guide to Spark SQL, DataFrames, and Datasets.
https://spark.apache.org/docs/2.2.1/sql-programming-guide.html#datasets-and-dataframes

## Listing files in HDFS to iterate
If you want to get a list of files on HDFS, here's one example to list all the files from Jan 2020.
```scala
import org.apache.hadoop.fs.{FileSystem, Path}
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val jan20 = "hdfs://cavium-thunderx/data/twitter/decahose/2020"
val flist = fs.listStatus(new Path(s"${jan20}")).filter(_.isFile).map(d => d.getPath)

val filelist = flist.filter(d => d.toString.contains("2020-01")).map(_.toString)
println(s"files: ${filelist.length}")
```
We expect there to be 62 files (two per day) but we see 60 files. Closer inspection reveals Jan 28th is missing data.

# Exercises
1. Return the number of points in the area with latitude in [43,44] and longitude in [-84,-83].
2. Create a two column DataFrame that returns a unique set of device-trip ids (RxDevice, FileId) sorted by RxDevice in ascending order and then FileId in descending order.
3. Create a two column DataFrame that returns two columns (RxDevice, Trips) for RxDevices with more than 60 trips.

# Running a Scala Script
http://spark.apache.org/docs/latest/submitting-applications.html

If you don't run Scala through the interactive shell but rather as a Scala script. You will need some additional code at the top of your script.

To get to the same starting point as the interactive shell, you need to start with these additional lines.
```scala
import org.apache.spark.sql.SparkSession

val spark = sparkSession
  .builder()
  .appName("My App Name")
  .getOrCreate()
val sc = spark.sparkContext
```

Alternatively, you can create a `SparkContext()` object. This tells Spark how to access a cluster. `SparkConf()` is where you can set the configuration for your Spark application.
```scala
import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf().setAppName("Alex").setMaster("local")
val sc = new SparkContext(conf=conf)
val sqlContext = new SQLContext(sc)
```

It's actually a bit more complicated than that though. You need to define a class and a main function. Here is an example Scala script as a github gist https://gist.github.com/caocscar/9ad1e7ec7b12a654b10b04d458ade122.
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, FloatType}

object ReadTextWriteParquet { // class name
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("CSCAR Scala").getOrCreate()
        val sc = spark.sparkContext

        val filename = "/var/cscar-spark-workshop/bsm_sm.txt" // workshop directory
        val lines = sc.textFile(filename, sc.defaultParallelism)
        println("READ IN TEXT FILE")
        val columns = lines.map(line => line.split(","))
        val table = columns.map(cols => Row(cols(0).toInt, cols(1).toInt, cols(3).toLong, cols(7).toFloat,
            cols(8).toFloat, cols(9).toFloat, cols(10).toFloat, cols(11).toFloat, cols(15).toFloat
        ))

        val schema = StructType(Seq(
            StructField(name="RxDevice", dataType=IntegerType, nullable=false),
            StructField(name="FileId", dataType=IntegerType, nullable=false),
            StructField(name="Gentime", dataType=LongType, nullable=false),
            StructField(name="Latitude", dataType=FloatType, nullable=false),
            StructField(name="Longitude", dataType=FloatType, nullable=false),
            StructField(name="Elevation", dataType=FloatType, nullable=false),
            StructField(name="Speed", dataType=FloatType, nullable=false),
            StructField(name="Heading", dataType=FloatType, nullable=false),
            StructField(name="Yawrate", dataType=FloatType, nullable=false)
        ))
        val df = spark.createDataFrame(table, schema)
        val rows = df.count()
        println(s"ROWS = ${rows}")
        println("WRITING TO PARQUET")
        val folder = "uniqname"
        df.write.mode("overwrite").parquet(folder)
        spark.stop()
    }
}
```
However, you can't just submit this scala script like you would a Python script. You need to create a .jar file from the scala script using a Scala build tool. **sbt** is a popular scala build tool. Here are instructions on:
1. how to install sbt
2. setup your project directory
3. go through a "hello world" example
   
https://docs.scala-lang.org/overviews/scala-book/scala-build-tool-sbt.html

Here is a slightly less trivial example once you have **sbt** setup on your local machine.
http://spark.apache.org/docs/2.2.1/quick-start.html#self-contained-applications

Run `sbt package` or `sbt run` to create your jar file.

You will need to copy your created jar file from to Cavium via the `scp` command like so:  
`scp target/scala-2.11/*.jar cavium-thunderx.arc-ts.umich.edu:./`

Submit the Spark job through the command line like this using the class name specified in your scala script.
`spark-submit --master yarn --queue default --num-executors 5 --executor-cores 5 --executor-memory 1g --class ReadTextWriteParquet <jarFilename>.jar`

Recall that you can type `spark-submit --help` for more options and defaults.

# Spark UI
This is a GUI to see active and completed Spark jobs.

On campus Ports 4040-4150 open for spark UI
http://cavium-thunderx.arc-ts.umich.edu:4050

# Spark Version
You can check the current version of Spark using `sc.version` OR if you are outside of the PySpark interactive shell `spark-shell --version`.

