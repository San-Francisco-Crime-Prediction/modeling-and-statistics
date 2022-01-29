# BigData project - A.A. 2021/22
Team member:
<br>Agostino Antonino, 223958
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
LOAD DATA INFILE '/var/lib/mysql-files/train.csv' INTO TABLE crimes FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES;
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
Different MapReduce Jobs has been implemented: a first one that helps cleaning the dataset and other two to calculate statistics relative to the dataset.
### Cleaning Job
This job helps to clean up the dataset from noisy data inside the dataset. It first takes the data from the `crimes` folder inside the HDFS the data and output the cleaned result in a new folder `cleandata` on HDFS. The cleaning is perfomed on both Mapper and Reducer of the job.

The mapper at first receive a row from the dataset and it perfoms different operation on some fields of it:
1. Stop-word removal from address column ("OAK ST / LAGUNA ST" to "OAK / LAGUNA") 
2. Discretization of the hour after the split of *Dates* column in a new one (hours in range 5 and 11 become "Morning", in range from 12 to 17 become "Afternoon" and so on...)
3. Correction on the field *Category* ("TREA" is the same of "TRESPASS")
4. Creation of two new fields *Month* and *Year* after the split of columns *Dates*

Then, this new row is sent to the reducer with as key the discrict and as value the whole row. Some values in the latitude column were in the range of 90 and 93 (latitude of the North Pole), so they have been corrected with a new value representing the median of that discrict.

### Statistic Job
This MapReduce Job has a twofold use: obtain the top-k occurences of a given column and then know their distribution against another column.

The first job takes as input the output of the cleaning data from the previous MapReduce Job. This job works as a word count: the mapper just write on the context as key the value of the column that we are interested in the top-k occurencies and as key "1", the reducers sum up all these values received as value, keeping track only of the top-k occurrences in a [tree-map](https://docs.oracle.com/javase/8/docs/api/java/util/TreeMap.html). The output of this job will be in a new folder called "*top-k-out*".

This second job works as a word count too, but this time the counting is done from some grouped values to know the distribution with respect to another parameter. This job takes as input the output of the previous job. On the mapper, each of them read the file output of the previous job and it writes on the context as key the value of the column of the top-k occurences and the value of the corresponding column choosen from the parameter that has been set and as value "1". The reducer sum up all these values and output the result on a new folder "*distribution_out*". 

## Modelling
The modelling part has been perfomed with the use of Spark and Python language with the help of the library SparkMLlib. The script has been done by initially loading the data from the HDFS to a dataframe with RDD format. Two different model has been used for the prediction task: Random Forest Classifier and Naive Bayes Classifier. The dataset needs to be prepared before their use for the model, so some previous step perfomed this operations:
- Removal of column *Address*, since redundant because of the presence of similar columns such as *Timeoftheday*, *Month* and *Year*
- Convertion of data type to their properly ones, i.e., *Longitude* and *Latitude* on float.
- Indexing of column *Category* since the modelling wants to have a prediction on this.

Different configuration has been applied to the models trying to achieve an high percentage of accuracy. All the different configuration before being trained has been transformed using the function OneHotEncoder. Then one last step was to split the dataset into a train set (70% of the dataset) and a test set (30% of the dataset).
### Random Forest Classifier
Starting from the previous preparation of the dataset the model has been trained considering or removing other columns. The following has been perfomed:
- Removal of *Description* and *Address* columns: 20% of accuracy achieved;
- Removal of *Address* columns: 40% of accuracy;
- Removal of *Latitude* and *Longitude* columns: 44% of accuracy;
- Removal of *Latitude*, *Longitude* and *Dayoftheweek* columns: 39.77% of accuracy.

### Naive Bayes Classifier
The Naive Bayes Classifier has been perfomed with the following configuration:
- Removal of *Address*, *Longitude* and *Latitude* columns: 43% of accuracy;
- Removal of  *Address*, *Longitude*, *Latitude* and *Dayoftheweek* columns: 99% of accuracy achieved.


Then the columns *Description*,*Address* and *Crimedate* has been drop since redudant with other columns. All the remaining columns then have been casted to their corresponding type. All the categorical data has been transformed with `OneHotEncoder`. The column category is the one where the model is going to train. A random forest classifier has been used to train the dataset. The accuracy obtained is about 22%. 
If we add the description as column in the OneHotEncoder, the model reach an accuracy of 40%. 
By removing the columns latitude and longitude the accuracy goes up to 44%. 
Using the same columns, but with the NaiveBayes Classifier the accuracy is of 43%. 
Starting from these last columns and removing `dayoftheweek` an accuracy of 39.77% on randomforestclassifier. 
Using Naive Bayes classifier removing crimedate, address, longitude, latitude and dayofweek  99%
