# BigData project - A.A. 2021/22
Team member:
<br>Agostino Antonino, 223958
<br>Andronico Giorgio, 227815
<br>Gianfranco Sapia, 223954
  
## Introduction
The project deals with a dataset taken from [Kaggle](https://www.kaggle.com/c/sf-crime/), providing nearly 12 years of crime reports from across all of San Francisco's neighborhoods. The aim is to classify the category of crime that occurred given time, location, and other features such as how the crime was resolved (i.e., if the subject was arrested or released).

## Cluster configuration
Data cleaning, elaboration and modeling will be run on an Hadoop cluster, composed by three machines: one master and two slaves. The master machine is set-up as follows:
- OS: Ubuntu 18.04 LTS
- RAM: 4GB
- Hard Drive: 20GB

Both slaves are set-up as follows:
- OS: Ubuntu 18.04 LTS
- RAM: 2GB
- Hard Drive: 20GB

### Technologies used:

**Apache Hadoop** is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models. Both master and slave have configured the version *3.2.2*.

**Apache Hive** is a data warehouse software built on top of Apache Hadoop for providing data query and analysis. This software is set-up only on master with the version of *2.3.9*

**Apache Sqoop** is a tool design for efficiently transferring bulk data between Apache Hadoop and structured datastores such as relational databases. Also this software is set-up only on master machine with the version *1.4.7*

The last software set-up only on master is **Apache Spark**, with version *3.2.0*, which is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

## Data ingestion
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
This last step was necessary due to presence of comma in the fields of *Description* and *Resolution*. For example, one such case was a row where the field *Resolution* was *"ARREST, BOOKED"*. We noticed that MySQL eliminates quotes, so the field *"ARREST, BOOKED"* will appear as *ARREST, BOOKED*. However, in the MapReduce jobs, often we will need to split the row on the comma; to avoid malformed splits, we must keep the air quotes in. The query is the following:
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
## Data Understanding
The dataset is structured in 878048 rows and 12 columns. The columns are named as follows:
- *Dates* - timestamp of the crime incident
- **Category** - category of the crime incident (the target variable)
- *Descript* - detailed description of the crime incident
- *DayOfWeek* - the day of the week
- *PdDistrict* - name of the Police Department District
- *Resolution* - how the crime incident was resolved
- *Address* - the approximate street address of the crime incident  
- *X* - Longitude
- *Y* - Latitude

*X* and *Y* are the only numerical columns, *Dates* is a date column and all others are categorical columns.

## Data cleaning and transformation
Before trying to understand the distribution of data by making statistics, we had to clean the dataset from null values and malformed rows.

### Cleaning and feature engineering job
The job first takes the data from the `crimes` folder inside the HDFS the data and output the cleaned result in a new folder `cleandata` on HDFS. 

#### Mapper
For each row of the dataset, these operations are performed:
1. Stop-word removal from address column (e.g. "OAK ST / LAGUNA ST" to "OAK / LAGUNA"). Stopwords include "LN", "AVE", "ST", "Block", "of". 
2. Creation of a categorical column called "TimeOfTheDay", which varies according to the hour in which the crime occurs. i.e., hours in range 5 and 11 become "Morning", in range from 12 to 17 become "Afternoon", etc.
3. Typo correction on the field *Category* ("TREA" is the same as "TRESPASS", most likely this was due to a typo in the dataset)
4. Creation of two new fields *Month* and *Year* after the split of column *Date*
The mapper writes to the context the district as key and as value the whole row. This is because part of the cleaning process happens in the reducer as well. 
#### Reducer
Indeed, some values in the latitude column were in the range of 90 and 93, which are obviously wrong (90 is the latitude of the North Pole). So, for each district, the reducer computer the median values of both latitude and longitude, and in case it finds an outlier, it substitutes that value with the median for that district. So for example, if a "Robbery" crime happened in the "Mission" district at a latitude of 92, and the median for the district is 82, then the new value will have 82 as latitude instead of 92. The output of the reducer is the completely cleaned dataset.

## Data analysis
In this step, we extracted some statistics from the dataset to identify trends, highest values for different columns, etc. This was performed by two separate (but chained) jobs.

### First Job
Objective: obtaining the top-k obtain the top-k occurences of a given column.

The first job takes as input the output of the cleaning data from the previous MapReduce Job. This job works as a word count: the mapper just writes on the context the value of the column as key, and 1 as value, the reducers sum up all these values received as value, keeping track only of the top-k occurrences in a [TreeMap](https://docs.oracle.com/javase/8/docs/api/java/util/TreeMap.html). The output of this job (of the form "column occurrence, number of occurrences") will be in a new folder called "top-k-out".

### Second job
This job is chained to the first, and its objective is to know the distribution of one column w.r.t. another column. For example, one might be interested in knowing how the rate of each crimes (or, in our case, of the top-k crimes) increases or decreases year-by-year.
#### Mapper
Firstly, the mapper reads the output of the first job, as it needs to know which are the top-k occurring values for a given column. Continuing from the last example, it needs to know which are the top-k occurring crimes, to know their distribution against time.
Then, as a second input, it reads again the cleaned dataset, and considers only the rows where one of the top-k occurring values appears.
Concluding the example, the mapper will ultimately output all pairs with ("type of crime", "year in which it happened") as key and 1 as value.

We have performed the following analyses:

1. How many times each crime occurs
2. How many times a crime is registered in each district
3. Extract the top-5 occurring crimes, and plot their distribution by day and month 
4. Extract the top-4 occurring district, and plot their crime rate year-by-year
5. For each district, extract the top-4 crimes for each district and their relative occurrences

#### Reducer
The reducer simply aggregates by both columns and sums up the occurrences, similarly to what a word count would do. Following again from the previous example, the output will be of the form ("type of crime", "year in which it happened", "how many times it occurred").

This job reads both from the output of the first job, and again from the cleaned dataset. This is because 
     
This second job works as a word count too, but this time the counting is done from some grouped values to know the distribution with respect to another parameter. This job takes as input the output of the previous job. On the mapper, each of them read the file output of the previous job and it writes on the context as key the value of the column of the top-k occurences and the value of the corresponding column choosen from the parameter that has been set and as value "1". The reducer sum up all these values and output the result on a new folder "distribution_out". 

### Validation of the output
To debug and verify correctness of these jobs, for each of the analyses we have performed hive queries and compared results.

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
