# BigData project - A.A. 2021/22
Team member:
<br>Agostino Antonino, ---MATRICOLA---
<br>Andronico Giorgio, ---MATRICOLA---
<br>Gianfranco Sapia, 223954
  
## Introduction
From 1934 to 1963, San Francisco was infamous for housing some of the world's most notorious criminals on the inescapable island of Alcatraz.

Today, the city is known more for its tech scene than its criminal past. But, with rising wealth inequality, housing shortages, and a proliferation of expensive digital toys riding BART to work, there is no scarcity of crime in the city by the bay. The dataset given by [SF OpenData](https://data.sfgov.org/) provides nearly 12 years of crime reports from accross all of San Francisco's neighborhoods. The aim is to classify the category of crime that occurred given time and location.

## Cluster configuration
All the work done for this project will be running on a local cluster independent for each team's member. The cluster is composed by three machines: one master and two slaves. The master machine is set-up as follow:
- OS: Ubuntu 18.04 LTS
- RAM: 4GB
- Hard Drive: 20GB

Both slaves are set-up as follow:
- OS: Ubuntu 18.04 LTS
- RAM: 2GB
- Hard Drive: 20GB

The **Apache Hadoop** is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models. Both master and slave have configured the version *3.2.2*.

**Apache Hive** is a data warehouse software built on top of Apache Hadoop for providing data query and analysis. This software is set-up only on master with the version of *2.3.9*

**Apache Sqoop** is a tool design for efficiently transferring bulk data between Apache Hadoop and structured datastores such as relational databases. Also this software is set-up only on master machine with the version *1.4.7*

The last software set-up only on master is **Apache Spark**, with version *3.2.0*, which is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

## Dataset Loading
During this phase the file `train.csv` has been first uploaded on **MySQL Server** on the master machine. The first step was creating the database with the following query:
```
CREATE DATABASE crimes;
```
The next step was to create the table with this query
```
CREATE TABLE crimes(dates varchar(255), category varchar(255), descript varchar(255), dayoftheweek varchar(255), pddistrict varchar(255), resolution varchar(255), address varchar(255), longitude varchar(255), lat varchar(255));
```
After this the file located in `/var/lib/mysql-files` has been loaded inside the table crimes as follow:
```
CREATE TABLE crimes(dates varchar(255), category varchar(255), descript varchar(255), dayoftheweek varchar(255), pddistrict varchar(255), resolution varchar(255), address varchar(255), longitude varchar(255), lat varchar(255));
```
The last step that has been performed is due to presence of comma in the fields of *Description* and *Resolution*. These fields have been concatainated within double quotes to in a better way during the MapReduce jobs. The query is the following:
```
UPDATE crimes c SET c.descript = CONCAT('\"', c.descript, '\"'), c.resolution = CONCAT('\"', c.resolution, '\"');
```

Now, the dataset is ready to be moved from MySql to HDFS. For this purpose the command `sqoop-import` has been executed.
```
sqoop-import --connect jdbc:mysql://master/crimes --username hive -P --table crimes -m 1
```
Note that the option `-m 1` is necessary due to the absence of a primary key in the dataset.

The dataset now is ready on the HDFS for MapReduce Jobs. After the dataset is cleaned with the corresponding MapReduce cleaning Job, an external table on hive has been created as follow:
```
CREATE EXTERNAL TABLE crimes(crimedate DATE, category STRING, description STRING, dayoftheweek STRING, district STRING, resolution STRING, address STRING, longitude FLOAT, latitude FLOAT, timeoftheday STRING, month STRING, year INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' LOCATION '/user/hadoop/cleandata';
```
## Dataset description
The dataset is structured in 878048 rows and 12 columns. The columns are enumerated as follow:
- *Dates* - timestamp of the crime incident
- *Category* - category of the crime incident (only in train.csv). This is the target variable you are going to predict.
- *Descript* - detailed description of the crime incident (only in train.csv)
- *DayOfWeek* - the day of the week
- *PdDistrict* - name of the Police Department District
- *Resolution* - how the crime incident was resolved (only in train.csv)
- *Address* - the approximate street address of the crime incident  
- *X* - Longitude
- *Y* - Latitude

*X* and *Y* are the only numerical columns, *Dates* is a date columns and all others are categorical columns.

***GRAFICI E FEATURE SELECTION***
## MapReduce Jobs
Different MapReduce Jobs has been implemented: a first one that help cleaning the dataset and other two to calculate statistics relative to the dataset.
### Cleaning Job
This job helps to clean up the dataset from noisy data inside the dataset. It first takes the data from the `crimes` folder inside the HDFS the data and output the cleaned result in a new folder `cleandata` on HDFS. The cleaning is perfomed on both Mapper and Reducer of the job.

The mapper at first receive a row from the dataset and it perfoms different operation on some fields of it:
1. Stop-word removal from address column ("OAK ST / LAGUNA ST" to "OAK / LAGUNA") 
2. Discretization of the hour after the split of date column (hours in range 5 and 11 become "Morning", in range from 12 to 17 become "Afternoon" and so on...)
3. 
