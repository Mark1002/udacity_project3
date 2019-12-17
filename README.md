# udacity project 3

 ## summary
This project is for simple ETL pipeline demo on aws redshift. first, create each table according to project requirement. Second, analysis and preprocess Song and Log Dataset to match database schema definition. finally, ececute ETL python script to insert the preprocessed data into each mapping database table.
 
 ## requirement
 This project need a aws redshift cluster exist frist. you should write your dwh.cfg file for aws connection infomation yourself. 
 
 ## file definition
 1. create_table.py
     reset and create db table. execute this file in terminal.
 ```
 $ python create_table.py
 ```
 2. sql_queries.py
    SQL command including create table, delete table, insert record and select record are all defined in this file. this file is a python module, imported by `etl.py` and `create_table.py`.

3.  etl.py
    python script for all ETL logic, including load raw data, data preprocess, and insert into database. execute this file in terminal.
```
$ python etl.py
```
