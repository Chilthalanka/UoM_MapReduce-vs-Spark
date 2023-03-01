# UoM_MapReduce-vs-Spark

In this assignment, I will analyze the [Airline Delay](https://online.uom.lk/pluginfile.php/624949/mod_assign/intro/DelayedFlights-updated.csv) dataset using MapReduce and Spark.

The airlines market has faced losses due to flight delays, and there are many reasons for delaying a flight. In this Analysis, I will be analysing the various delays happen in airlines per year for below scenarios.

    i. Year wise carrier delay from 2003-2010
    ii. Year wise NAS delay from 2003-2010
    iii. Year wise Weather delay from 2003-2010
    iv. Year wise late aircraft delay from 2003-2010
    v. Year wise security delay from 2003-2010


## MapReduce

Hive external table is created using the DDL script (sql_ddl_airline_delay.sql) available in the "MapReduce/sql/" directory. The queries developed for each of the above 5 scenarios can also be found in the same directory.


Below steps are followed:

1. Create a AWS S3 bucket - *airline-delay-analysis*
2. Create a folder named *sql* in the bucket and upload the SQL scripts
3. Create a folder named *input* in the bucket and upload the Airline Delay dataset (available in the "data/" directory)
4. Create a AWS EMR cluster with Hive and Spark installed - *chinthalanka-cluster-1*
5. SSH to the primary node and sync the SQL scripts in S3 to the node using below command:

    `aws s3 sync s3://airline-delay-analysis/sql/ ./sql/ --exclude 'folder'`

6. Run the Hive queries using below command and record the execution time:

    `hive -f <file_name>`