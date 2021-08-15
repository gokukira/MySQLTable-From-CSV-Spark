# MySQLTable-From-CSV-Spark

### Get CREATE TABLE and LOAD LOCAL DATA MySQL commands from CSV file.
Useful when csv file contains large number of columns

#### Run command:
  spark-submit \<path>/get_table_csv.py \<source>

Example: 
  spark-submit get_table_csv.py /home/datasets/employee.csv

Ensure that the source file is .csv and contains header

Source file path should contains a single '/' as prefix

Requirements: 
spark,
python
