#%%
from pyspark import SparkContext
from operator import add
from functions import extract_vin_key_value, populate_make, extract_make_key_value

# within the sandbox shell run: spark-submit AutoSaleExtractSpark.py

# Initialize SparkContext. Use the Spark context object to read the input file to create the input RDD.
sc = SparkContext("local", "My Application")
raw_rdd = sc.textFile("data/data.csv") 


#%%
# raw_rdd.collect()

# Use map operation to produce PairRDD
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

#%%
# vin_kv.collect()

#%% Populate make and year to all the records
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

#%%
# enhance_make.collect()

#%% Count the number of occurence for accidents of vehicle make and year
make_kv = enhance_make.map(lambda list_val: extract_make_key_value(list_val))

#%%
make_kv.collect()

#%% Assign final result
final_output = make_kv.reduceByKey(add).collect()
print(final_output)

# Export to text file
with open("data/final_output.txt", "w") as output:
    for val in final_output:
        print(val)
        output.write(str(val).strip("( )") + "\n")
        
# Stop Spark Application
# sc.stop()


