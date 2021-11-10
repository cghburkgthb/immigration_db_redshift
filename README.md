# US Non-permanent Resident Immigration Warehouse

## Content
- Summary
- Data Analysis
- Data Cleaning
- Database Diagram
- Enhancing the Load Daily Load Process
- Data File Descriptions
- Reasons for Infrastructure Choice
- AWS Infrastructure Setup Instructions
- Execution Instructions
- Addressing Other Scenarios
- References
    
## Summary

The purpose of this analytics data warehouse is to provide a way for you to understand the US immigration pattern for non-permanent resident immigrants.  From this database you will be able determine who is coming to US, when are they likely to coming, why are they traveling to the US, where they are going in the US and how are they traveling to the US. Also, the DB provided some additional information about the arrival airport such city demographics, average monthly temperature, in some cases etc.

## Data Analysis

Each source data was examined to determine how to integrated them. The "I94 SAS Labels Descriptions" file along with site of the original datasets were reviewed to understand the meaning of each field within the data files. While doing the above, ideas of how to combine the datasets were brainstormed.

It was not obvious how to integrated the temperature data, because it is clear of the added value obtained by knowing the arrival airport monthly average temperature. It makes more sense to know the immigrant final destination address temperature. Anyway, because the airport data file contained geographical locations and the fact that the temperature file did not contain the US state name, the decision was made to lookup the airport location temperature to avoid associating the wrong airport/temperature data. Additionally, a plan to estimate the temperature data for 2016 was formulated in order to integrate it with immigration data.

Next, each dataset was further reviewed to determine data anomalies. The data files were loaded into a Pandas data-frame and various Pandas functions (e.g., df.head, df.info, df.describe(), etc.) were used to understand the content of the files. Some of data were loaded into Excel and reviews. More analysis of the data was performed by loading the datasets into PostgreSQL. Using the knowledge gained for the above activity and using information from "I94 SAS Labels Descriptions" document help me determine which fields to clean or eliminate.

Initially the database was prototype using PostgreSQL on my local computer. While doing the above, I discovered that I could only load the immigration data files sequentially. Additionally, I discovered that the immigration data files contained characters that caused the load to fail, even through the DB was created for UTF-8 encoding. Thus, a "perl" command to remove clean files.

## Data Cleaning

### Immigration Data
First several of the fields that aren't being used in the final database design were dropped from the data files. Additionally, some of the port of entry code in the files were replaced by the new codes mentioned in the I94 SAS Labels Descriptions" file. NULL values in transportation mode field where replace with 9 --Not reported. Next some of the fields' data type we converted to proper data types. Finally, a perl command was used to remove bad characters from the files. The script "clean_imgrtn_data.py" was written to read, clean and create the csv files.

### Temperature Data
A script "write_us_temp_data.py" was written to generate a file content only the US cities temperature data for 2010-01-01 onwards.

To integrate the temperature data with immigration, the SQL code that loads the temperature dimension table estimates the 2016 average monthly temperature by using the average temperature of the previous three years for a given month.

### US Cities Demographics Data
The race and count fields within the data file were dropped, because the total of count values did match the total city population count.

### I94 SAS Labels Descriptions Data
The information with the "I94 SAS Labels Descriptions" file was used to create the following files manually: country.csv, port_of_entry.csv, transportion_mode.csv and visa_category.csv

## Database Diagram
The database consist of the one fact table and 9 dimension tables depicted in the diagram below. A star schema instead of a snowflake schema was used to simplify and decrease the runtime of the analytical queries. 

FYI, a default record is insert into each dimension table, as necessary, to enable all US immigration records to be loaded into the target fact table.

Please see the diagram below.

![Database Design](file:///us_imgrn_db_dsgn.png)

## Reasons for Infrastructure Choice
An RDMS database was choose because of the ability and flexible to access the data via SQL. In particularly a Redshift DB was chosen over a PostgreSQL DB, because of its ability rapidly load the large compress immigrations data files in parallel faster. Also, has the dataset and the number end-users increase, the AWS infrastructure resources (number of CPU and storage options) can be easily expand to accommodate the additional system demand.

Also, Redshift automatically assigns a distribution method based upon the table size (see: https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html). If end-user queries performance becomes a problem, the default distribution would be one area to review and change, if necessary. 

## Data File Descriptions

The immigration files (e.g., ../imgrtn_data/i94_apr16_sub.csv.gz, contain trip and personal information about each immigrant.

The airport file (../misc_data/airport_codes.csv.gz) contains location and various codes associated with the airport.

The city demographics file (../misc_data/us-cities-demographics.csv) contains population data for various groups of people, resident median age and average household size. It is a copy of us-cities-demographics.csv.

The country file (../misc_data/country.csv) contains the immigrant country of citizen/residency name and key referenced in the immigration files. It was created manually using information contained in I94_SAS_Labels_Descriptions.SAS file.

The port of entry file (../misc_data/port_of_entry.csv) contains the port name and key referenced in the immigration files. It was created manually using information contained in I94_SAS_Labels_Descriptions.SAS file.

The temperature file (misc_data/contains average monthly temperature for certain city US cities. It was generated from the cities of world temperature file GlobalLandTemperaturesByCity.csv which have been remove from the current work space.

The transportation mode file (../misc_data/transportion_mode.csv) contains the transport type and key referenced in the immigration files. It was created manually using information contained in I94_SAS_Labels_Descriptions.SAS file.

The visa category file (../misc_data/visa_category.csv) contains the type of visa the immigrant used to travel to the US and key referenced in the immigration files. It was created manually using information contained in I94_SAS_Labels_Descriptions.SAS file.

The US state file (../misc_data/us_state.csv) contains the US states' name and code. It was created manually using information from https://www.factmonster.com/us/postal-information/state-abbreviations-and-state-postal-codes.

## AWS Infrastructure Setup Instructions

### IAM User Creation
Logon to your AWS account and execute the following instructions:

> From the IAM User Console, create an IAM user with the following policies: **AmazonS3ReadOnlyAccess** and **AmazonRedshiftFullAccess**

-  IAM Console > Add User
-  User Name: airflow_redshift
-  Access Type: **Programmatic access** -- select this option
  
> On the Set Permissions dialog page, select the option: **Attach existing policies**. Then search for and add the policies mentioned above.
  
> On the Add tags (optional) page -- you do not need to specify any tags.
  
> From Add User success page, save the **Access Key ID** and **Secret Access Key**. Store the information above in the ../dwh.cfg file.

#### IAM Redshift DB Role Creation
> Next use the IAM Console to create Redshift Role: **myRedshiftRole** and attach the following policies: **AmazonRedshiftFullAccess**, **AmazonS3ReadOnlyAccess** and **AmazonRedshiftQueryEditor**

#### Redshift DB Cluster Creation  
> From the Redshift dashboard, use the "Quick Launch Cluster" button to create Redshift DB Cluster with default settings and the role: **myRedshiftRole** created above. Record and store the following attributes of the cluster in the ../dwh.cfg file:
    
- Host: redshift-cluster-1.cqlqgt8pufyq.us-west-2.redshift.amazonaws.com
- Schema: dev -- Redshift DB name
- Login: awsuser -- Redshift Master User Name
- Password: <**password**> -- replace with your real Redshift user password
- Port: 5439

Make sure to open the DB to public access.

> You can select the "Clusters" option from the dashboard to see the status of the cluster. Also, you can refresh the page to see the updated status of the cluster.

#### S3 Buckets Creation

Create the following two buckets or similar ones and update the ../dwh.cfg file.
- imgrtn-data
- other-misc-data

Make sure to open S3 buckets above to public access.

## Execution Instructions

Copy the Airport and US City Demographics data files to the ../misc_data. Rename and compress the airport data file with miscellaneous directory using gzip so that you end up with the file: ../airport_codes.csv.gz

To create a clean CSV file for each immigration source data file, please run the command below; this step is optional,
because I have already created the files in the workspace directory ../imgrtn_data:
- python clean_imgrtn_data.py

To select and write the US temperature records to a file, please run the command below; this step is optional,
because I have already create the file in the workspace directory ../misc_data:
- python write_us_temp_data.py

To upload the source data files to AWS S3 run the command below:
- python upload_file_to_aws.py

To create the tables please run the following command:
- python create_redshift_tbl.py

To load the tables, please run the command below:
- python etl_redshift.py

## Addressing Other Scenarios
You can eliminate more of the fields that are not being used in the final fact table from the immigration CSV file will speed up the loading of the data.

Next, the daily immigration file can be split and compressed to take advance of Redshift ability to load the split files in parallel, after the initial load.

Also, analyzing and compressing column values that are repeatedly used will reduce the I/O cost necessary to read the data from the storage media needed by end-users queries.

Depending upon the usage of the database, you can create materialized subset of data for a certain set of users. Furthermore, you can roll up the data to a higher level to enhance the performance of the system for a particular set of users/reports. 

Furthermore, you can load each month of immigration data into separate table, used a view to combine the tables and prune the dataset to ensure only the needed monthly data are being retained in the data warehouse. In fact, you can create multiple views to address the end-users performance issues (e.g., recent data view and all data view).

Overtime to tune the load daily load process, the AWS DB environment hardware can be changed to speed to the load process. For example, you change the storage media being use by the database from a hard-drive (HDD) to SSD to RA3 in order to increase throughput of the system.

Also, you can replicate the entire database and allow users to access a particular instance of the database.

If we needed to run the loads by 7 am every day, the ETL should be scheduled using Airflow.

## References
Working with SAS Dates:

https://support.sas.com/resources/papers/proceedings/proceedings/sugi24/Coders/p073-24.pdf

Convert a SAS datetime in Pandas:

https://stackoverflow.com/questions/36500348/convert-a-sas-datetime-in-pandas

pandas.to_timedelta:

https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_timedelta.html

Subprocess Management:

https://docs.python.org/3/library/subprocess.html

Change Data Type for one or more columns in Pandas Dataframe:

https://www.geeksforgeeks.org/change-data-type-for-one-or-more-columns-in-pandas-dataframe/

Pandas DataFrame.fillna() to replace Null values in dataframe:

https://www.geeksforgeeks.org/python-pandas-dataframe-fillna-to-replace-null-values-in-dataframe/

How do I fill NA values in multiple columns in pandas?:

https://stackoverflow.com/questions/36556256/how-do-i-fill-na-values-in-multiple-columns-in-pandas

How to Convert Latitude and Longitude to Map Coordinates:

https://www.cfa.harvard.edu/space_geodesy/ATLAS/cme_convert.html

How to convert number to month name in PostgreSQL?:

https://stackoverflow.com/questions/59267191/how-to-convert-number-to-month-name-in-postgresql
to_char(to_date(proforma_invoice_date, 'DD/MM/YYYY')), 'Month')

A Summary of Error Propagation:

http://ipl.physics.harvard.edu/wp-uploads/2013/03/PS3_Error_Propagation_sp13.pdf

Postgres Quarter Function:

https://stackoverflow.com/questions/43440249/postgres-quarter-function

Drop all data in a pandas dataframe:

https://stackoverflow.com/questions/39173992/drop-all-data-in-a-pandas-dataframe

How to show all of columns name on pandas dataframe?:

https://stackoverflow.com/questions/49188960/how-to-show-all-of-columns-name-on-pandas-dataframe

How to Upload a File to Amazon S3 in Python:

https://medium.com/bilesanmiahmad/how-to-upload-a-file-to-amazon-s3-in-python-68757a1867c6

Amazon Redshift Load CSV File using COPY and Example:

https://dwgeek.com/amazon-redshift-load-csv-file-using-copy-and-example.html/

Redshift Copy Command Data Format Parameters:

https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html

Amazon Redshift – Identity column SEED-STEP behavior with different INSERT statements:

http://www.sqlhaven.com/identity-column-seed-step-with-insert-statements/

Optimizing Schema and Data Types:

https://www.oreilly.com/library/view/high-performance-mysql/9781449332471/ch04.html

Best Practices for SQL VARCHAR Column Length:

https://stackoverflow.com/questions/8295131/best-practices-for-sql-varchar-column-length

Redshift Distribution Styles:

https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html

Redshift Tuning Table Design:
https://docs.aws.amazon.com/redshift/latest/dg/tutorial-tuning-tables.html

Airport Table Data Dictionary:

https://ourairports.com/help/data-dictionary.html

