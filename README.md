## Project: Data Warehouse

### Overview

This database is an analytical database used to provide historical data analysis for Sparkify. It will enable Sparkify to identify what type of music appeals most to which users, so that they can tailor their app & music offering.
This project contains scripts to create and populate an AWS Redshift database as part of a data warehouse. The source of data is JSON files stored in S3. 


## Database Schema Design and ETL Process
**Database design**

The database is a traditional star schema where the fact table records songplay events, and the dimensional tables (users, artists, songs, time) all provide further information to help describe each songplay event.

The database was designed in order to optimize queries for songplay analysis. The physical data models are specified in the *sql_queries.py* file.

**ETL design**

The ETL process was designed to first copy all files from S3 into staging tables on redshift, then in a subsequent step data is copied from redshift staging tables to analytical tables. 

The project contains the following scripts:


[create_tables.py](create_tables.py) runs queries to drop and create tables in Redshift [sql_queries.py](sql_queries.py).

[etl.py](etl.py) extracts raw data from S3 into staging tables then loads it into the final tables.

[sql_queries.py](sql_queries.py) contains SQL table creation, copy and insertion commands.

[quality_check.py](quality_check.py) runs verification checks on the populated database tables.

[dwh.cfg](dwh.cfg) contains the required config necessary for the create_tables.py, etl.py files to execute successfully.


### How to run

**How to run this package**

In order to create the required database & execute the ETL pipeline:
1. Create an IAM role & user with admin access, able to access your AWS account programmatically
2. Populate the dwh.cfg config file with the required IAM info, DWH cluster info (i.e the required specs for the cluster to be created) and the S3 source info (Note: this is described in the 'Implementing datawarehouses on AWS' lesson)
3. Check via the Amazon Admin UI that the provisioned Redshift cluster is 'Available'
4. Execute the 'create_tables.py' script via the command line or a Jupyter Notebook
5. Execute the 'etl.py' script via the command line or a Jupyter Notebook
6. Execute the 'quality_check.py' script to validate the data
7. Delete the DWH cluster to avoid any unnessecary costs
