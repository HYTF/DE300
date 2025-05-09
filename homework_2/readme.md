This is homework 2 readme file. This file summarizes the expected outputs of the scripts in hw2.ipynb.

Part I
- download and import necessary packages (duckdb)
- Import csv files from hw2_data folder and put them in a duckdb. Print statements show the progress of the import.
- The following three short sql queries show the tables in the database, column names and data types in the table prescription, and column names and data types in the table admissions.
- Next query answers q1. Major function of the query is described in hw2_answers.pdf. The output is a table of top prescription by ethnicity.

- Onward to q2. The first two short queries output column names and data types of the procedures_icd table and the column names and data types of the patients table.
- The long query answers q2. Major function of the query is described in hw2_answers.pdf. The output is a table of top 3 procedure being done by ethnicity and age group. 

- Onward to q3. Major function of the query is described in hw2_answers.pdf. The query outputs a table of the average ICU stays by ethnicity and gender.

Part II
- download certificate and install cassandra-sigv4 and boto3
- First few blocks set up ssl, boto3 session, authorization with SigV4, and Cassandra cluster. Set and Established conenction to Keyspace mimic. 
- CSV files are pushed to Cassandra and tables are created. Print statements show the progress of creating tables and inserting data. 
- Next query checks the rows of the tables pushed to Cassandra. 
- Next chunck of code extracts data from prescription and admissions tables and then use pandas to conduct the analysis to answer q1 in Part I. Analysis results are printed. 
- Next chunck does similar things - extracts data and then use pandas to conduct analysis to answer q2 in Part I. Results are printed. 
- Next chunck extracts data and then use pandas to conduct analysis to answer q3 in Part I. Results are printed. 

Docker Commands
- docker build .
- docker run -p 8888:8888 \
  -v ~/.aws:/home/jovyan/.aws \
  -v /home/ec2-user/DE300:/home/jovyan/work \
  my_jupyter 
- Dockerfile is included in the directory

Gen AI Disclosure
- Gen AI (ChatGPT) is used in this assignment. Prompts are the following:
- How to use the JOIN function in sql?
- How to use PARTITION BY in sql?
- How to use datetime to strip time?
- Can you please help me debug T_T

