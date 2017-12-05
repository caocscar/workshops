# CSCAR Workshop on PySpark and SparkSQL

## Documentation
The latest official Spark documentation can be found at https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html

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
The goal is to get it into a RDD.

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

`table = columns.map(lambda x: Row(RxDevice=int(x[0]), FileId=int(x[1]), TxDevice=int(x[2]), Gentime=int(x[3]), Latitude=float(x[4]),      Longitude=float(x[5]), Elevation=float(x[6]), Speed=float(x[7]), Heading=float(x[8]), Yawrate=float(x[9])) )`

Create a DataFrame from RDD of `Row` objects
```
bsm = sqlContext.createDataFrame(table)
bsm.show(5)
bsm.dtypes
```

### Parquet Files
Parquet is an open-source column-store data format in Hadoop. That's all I'm going to say about that.

## Spark DataFrames

# Selecting Data

# Renaming Columns

# Filtering Rows

# Binning Data

# Miscellaneous Methods

# Merging Data

# Replacing Values

# Group By Method

# Adding and Deleting Columns

# Applying A Function to a Dataframe

# Duplicates

# Reshaping Data

# Converting to DateTime Format

# Crosstabs

# Plotting Data

----------------------------------------
# Set up Temp Table

# SparkSQL Commands

# Save DataFrame to File

# Exit PySpark Interactive Shell
Type `exit()` or press Ctrl-D




