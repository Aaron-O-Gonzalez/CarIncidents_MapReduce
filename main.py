from pyspark.sql import SparkSession

'''Instantiate a spark Session instance and use RDD API from sparkContext'''
spark = SparkSession.builder.master('local').appName('app').getOrCreate()
sc = spark.sparkContext
raw_rdd = sc.textFile("data.csv")

'''Transform input data into a paired RDD using vin_number as key
   and incident_type, make, and year as values'''
paired_rdd = raw_rdd.map(lambda x: x.split(",")).map(lambda x: (x[2], [x[1], x[3],x[5]]))


def select_non_null(a, b):
    if a is None or len(a) == 0:
         return b
    else:
         return a

#Replace missing/null make and year fields using information from full entry
enhanced_make_rdd = paired_rdd\
    .mapValues(lambda x: ([x[0]], x[1:]))\
    .reduceByKey(lambda a, b: (
            a[0] + b[0],
            [select_non_null(a[1][i], b[1][i]) for i in range(len(a[1])) ] 
     ) )\
    .flatMapValues(lambda x: [[k] + x[1]  for k in x[0]])\

#Filter records for incident "I"
filtered_enhanced_make_rdd = enhanced_make_rdd.filter(lambda x: x[1][0] == 'I')

#Create key-value combination using the make and year as key, and the count as value
make_year_kv = filtered_enhanced_make_rdd.map(lambda x: (tuple(x[1][1:3]), 1))

#Obtain the total counts for incidents associated with a specific make and year
total_counts = make_year_kv.reduceByKey(lambda a,b: a + b)

print(total_counts.collect())