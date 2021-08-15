'''
get CREATE table and LOAD local data statement from csv
>>>spark-submit <path>/get_table_csv.py <source>
ensure that the source file is .csv and contains header
source file path should a single '/' as prefix
'''

import sys
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


#initialize the spark session 
if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: spark-submit <path>/get_table_csv.py <path_to_csv_file>", file=sys.stderr)
		sys.exit(-1)

	spark = SparkSession\
		.builder\
		.appName("get_table_csv")\
		.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR") 
	
	f = sys.argv[1]
	f1 = "file://" + f
	print("\nSource csv path: {}".format(f))
	
	df = spark.read.options(inferSchema='True', header='True').csv(f1)
	df.registerTempTable('df_table')
	c=spark.sql('desc df_table').select('col_name','data_type')
	l=c.collect()
	#print(l)
	
	res = "CREATE TABLE <table_name> ("
	res1 = 'LOAD DATA LOCAL INFILE "{}" INTO TABLE <table_name> FIELDS TERMINATED BY "," IGNORE 1 ROWS ('.format(f)
	d = {}
	count=1
	c,e = "",""

	for i in l:
	    i=str(i)
	    t=str(i.split('Row(')).split(',')
	    t1 = str(t[1]).replace('"col_name=','')
	    t2 = str(t[2][:-3]).replace('data_type=','')
	    if "\\'s" in t1:		#case if column name contains 's
	        t1 = t1[:-1].replace("\\'s","")
	        t1 = t1.replace('col_name="','')
	        t2 = t2.replace("\\","")
	    if '(' in t1:		#case if column name conatins parentheses
	        t1 = t1[:t1.index('(')-1]
	    
	    t1 = t1.replace("'","")
	    t2 = t2.replace("'","")
	    t1 = t1[1:].lower().replace(" ","_")
	    t2 = t2[1:].lower().replace(" ","_")
	    t2 = t2.replace('string','varchar(255)') #mysql doesn't have datatype string
	    d[t1] = t2
	    res = res + " " + t1 + " " + t2 + ","
	    c = c + "@c{}".format(count) + ","	
	    e = e + "{} = NULLIF(@c{},''), ".format(t1,count)
	    count+=1
	
	res = res[:-1] + ")"
	print("\nCreate table command: \n")
	print(res)

	res1 = res1 + c[:-1] + ")" 
	print("\nLoad local data command: \n")
	print(res1)
	#print(d)    

	res1 = res1 + " SET " + e[:-2]
	print("\nLoad local data command (extended form): \n") 	#replaces missing value with NULL
	print(res1)
	print('\n')
