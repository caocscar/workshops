# CSCAR Workshop on PySpark and SparkSQL

# Documentation
The latest official Spark documentation can be found at https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html

## Introduction: Spark Overview
Let's look at the *Overview* section. You should take away a couple of things from the Spark Overview:
1. RDD (Resilient Distributed Dataset). The *resilient* part alludes to the fact that it can automatically recover from node failures. The *distributed* part refers to the fact that your data is partitioned across nodes in the cluster and will be operated on in parallel.
2. Spark performs in-memory computation. It does not write/read intermediate results to disk.

## APIs
Spark has API bindings to Scala, Java, Python and R. The official documentation page shows code snippets for the first three languages (sorry , R users).

The Spark Python API documentation can be found at https://spark.apache.org/docs/2.2.0/api/python/index.html

# Pros/Cons
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

# Initializing Spark
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

# Set up SQLContext
Let's create a SQLContext
`sqlc = SQLContext(sc)`

# Data
As with all data analysis, you can either:
1. Create data from scratch
2. Read it in from an external data source
The goal is to get it into a RDD.

# Parallelized Collections
```python
data = range(1000000)
RDDdata = sc.parallelize(data)
total = RDDdata.reduce(lambda a,b: a+b)
```

# Read in Different Filetypes
PySpark can create distributed datasets from any storage source supported by Hadoop. This includes text files.

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




