-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Preparation

-- COMMAND ----------

-- The code for all 3 clinical trila data is present in this fine.
-- if ran without any modification, it will display the results for the 2021 file
-- to view the results of another year, just comment off the  codes of other year.

-- COMMAND ----------

DROP DATABASE IF EXISTS bdtt_coursework;
CREATE DATABASE  bdtt_coursework ;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/ct2019/', True)
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/ct2020/', True)
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/ct2021/', True)
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/pharma/', True)
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/mesh/', True)

-- COMMAND ----------

--TABLE CREATION
DROP TABLE IF EXISTS ct2019;
DROP TABLE IF EXISTS ct2020;
DROP TABLE IF EXISTS ct2021;
DROP TABLE IF EXISTS mesh;
DROP TABLE IF EXISTS pharma;

--CREATING THE CLINICAL TRIAL TABLE
CREATE TABLE ct2019
(
 Id STRING,
 Sponsor STRING,
 Status STRING,
 Start STRING,
 Completion STRING,
 Type STRING,
 Submission STRING,
 Conditions STRING,
 Interventions STRING
 )
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|';
 
 CREATE TABLE ct2020
(
 Id STRING,
 Sponsor STRING,
 Status STRING,
 Start STRING,
 Completion STRING,
 Type STRING,
 Submission STRING,
 Conditions STRING,
 Interventions STRING
 )
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|';
 
 CREATE TABLE ct2021
(
 Id STRING,
 Sponsor STRING,
 Status STRING,
 Start STRING,
 Completion STRING,
 Type STRING,
 Submission STRING,
 Conditions STRING,
 Interventions STRING
 )
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|';
 
 --CREATING THE PHARMA TABLE
 CREATE TABLE PHARMA
(
 Company STRING,
 Parent_Company STRING,
 col1 STRING,
 col2 STRING,
 c0l3 STRING,
 col4 STRING
 )
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '","';
 
 --CREATING THE MESH TABLE
 CREATE TABLE MESH
( 
 term STRING,
 tree STRING,
 col1 STRING,
 col2 STRING,
 col3 STRING,
  col4 STRING
 )
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';

-- COMMAND ----------

--LOADING DATA INTO THE TABLES
LOAD DATA INPATH 'dbfs:/FileStore/tables/clinicaltrial_2019.csv'
INTO TABLE ct2019 ;
LOAD DATA INPATH 'dbfs:/FileStore/tables/clinicaltrial_2020.csv'
INTO TABLE ct2020 ;
LOAD DATA INPATH 'dbfs:/FileStore/tables/clinicaltrial_2021.csv'
INTO TABLE ct2021 ;
LOAD DATA INPATH 'dbfs:/FileStore/tables/pharma.csv'
INTO TABLE PHARMA ;
LOAD DATA INPATH 'dbfs:/FileStore/tables/mesh.csv'
INTO TABLE MESH ;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #COPYING THE DATA FROM HIVE WAREHOUSE TO FILESTORE FOR LATER USE.
-- MAGIC 
-- MAGIC dbutils.fs.cp('dbfs:/user/hive/warehouse/ct2019/clinicaltrial_2019.csv','dbfs:/FileStore/tables/clinicaltrial_2019.csv');
-- MAGIC dbutils.fs.cp('dbfs:/user/hive/warehouse/ct2020/clinicaltrial_2020.csv','dbfs:/FileStore/tables/clinicaltrial_2020.csv');
-- MAGIC dbutils.fs.cp('dbfs:/user/hive/warehouse/ct2021/clinicaltrial_2021.csv','dbfs:/FileStore/tables/clinicaltrial_2021.csv');
-- MAGIC dbutils.fs.cp('dbfs:/user/hive/warehouse/pharma/pharma.csv','dbfs:/FileStore/tables/pharma.csv');
-- MAGIC dbutils.fs.cp('dbfs:/user/hive/warehouse/mesh/mesh.csv','dbfs:/FileStore/tables/mesh.csv');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Problem Statement 1
-- MAGIC Find the number of distinct studies in the dataset.

-- COMMAND ----------

select count(Id) from ct2019 where Id != 'Id';
select count(Id) from ct2020 where Id != 'Id';
select count(Id) from ct2021 where Id != 'Id';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Problem Statement 2
-- MAGIC List all the types (as contained in the Type column) of studies in the dataset along with
-- MAGIC the frequencies of each type ordered from most frequent to least frequent.

-- COMMAND ----------

select distinct(Type),count(*) as frequency from ct2019 where Type !='Type' group by Type sort by frequency desc;
select distinct(Type),count(*) as frequency from ct2020 where Type !='Type' group by Type sort by frequency desc;
select distinct(Type),count(*) as frequency from ct2021 where Type !='Type' group by Type sort by frequency desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Problem Statement 3
-- MAGIC Select top 5 conditions (from Conditions) with their frequencies.

-- COMMAND ----------

DROP VIEW IF EXISTS disease_frequency2019;
DROP VIEW IF EXISTS disease_frequency2020;
DROP VIEW IF EXISTS disease_frequency2021;
--2019
CREATE VIEW  disease_frequency2019
as select distinct(uniqueVal),count(*) as frequency 
from(
  select  split(Conditions, ',')  as valArray from ct2019 ) as d
  lateral view explode(d.valArray) exploded as uniqueVal where uniqueVal !='' group by uniqueVal sort by frequency desc;
--2020
CREATE VIEW  disease_frequency2020
as select distinct(uniqueVal),count(*) as frequency 
from(
  select  split(Conditions, ',')  as valArray from ct2020 ) as d
  lateral view explode(d.valArray) exploded as uniqueVal where uniqueVal !='' group by uniqueVal sort by frequency desc;
--2021
CREATE VIEW  disease_frequency2021
as select distinct(uniqueVal),count(*) as frequency 
from(
  select  split(Conditions, ',')  as valArray from ct2021 ) as d
  lateral view explode(d.valArray) exploded as uniqueVal where uniqueVal !='' group by uniqueVal sort by frequency desc;
  
--2019
select * from disease_frequency2019  limit 5;
--2020
select * from disease_frequency2020  limit 5;
--2021
select * from disease_frequency2021  limit 5;




-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Problem Statement 4
-- MAGIC Select the 5 most frequent roots.

-- COMMAND ----------

DROP VIEW IF EXISTS disease;
DROP VIEW IF EXISTS frequency_code_2019;
DROP VIEW IF EXISTS frequency_code_2020;
DROP VIEW IF EXISTS frequency_code_2021;

CREATE VIEW  disease
  AS select
    case 
      when col3 !='null' then replace(concat(term,tree,col1,col2),'"','')
      when col2!='null' then replace(concat(term,tree,col1),'"','')
      when col1!='null' then replace(concat(term,tree),'"','')
      else term
    end as term,
    case
      when col3!='null' then  SUBSTR(col3,0,3)
      when col2!='null' then  SUBSTR(col2,0,3)
      when col1!='null' then  SUBSTR(col1,0,3)
      else SUBSTR(tree,0,3)
    end as tree  
  from mesh where tree != 'tree';   

CREATE VIEW  frequency_code_2019 as
SELECT f.frequency , d.tree
FROM disease d JOIN  disease_frequency2019 f
ON (d.term = f.uniqueVal);

CREATE VIEW  frequency_code_2020 as
SELECT f.frequency , d.tree
FROM disease d JOIN  disease_frequency2020 f
ON (d.term = f.uniqueVal);

CREATE VIEW  frequency_code_2021 as
SELECT f.frequency , d.tree
FROM disease d JOIN  disease_frequency2021 f
ON (d.term = f.uniqueVal);

--2019
select distinct(tree),sum(frequency) as f from frequency_code_2019  group by tree sort by f desc limit 5;
--2020
select distinct(tree),sum(frequency) as f from frequency_code_2020  group by tree sort by f desc limit 5;
--2021
select distinct(tree),sum(frequency) as f from frequency_code_2021  group by tree sort by f desc limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Problem Statement 5
-- MAGIC Find the 10 most common sponsors that are not pharmaceutical companies, along with the number
-- MAGIC of clinical trials they have sponsored.

-- COMMAND ----------

--2019
SELECT distinct(c.Sponsor), count(*)  as number_of_projects from  ct2019  c
LEFT OUTER JOIN pharma p
ON (c.Sponsor = p.col2) where  p.col2 is null group by c.Sponsor sort by number_of_projects  desc;

--2020
SELECT distinct(c.Sponsor), count(*)  as number_of_projects from  ct2020  c
LEFT OUTER JOIN pharma p
ON (c.Sponsor = p.col2) where  p.col2 is null group by c.Sponsor sort by number_of_projects  desc;

----2021
SELECT distinct(c.Sponsor), count(*)  as number_of_projects from  ct2021  c
LEFT OUTER JOIN pharma p
ON (c.Sponsor = p.col2) where  p.col2 is null group by c.Sponsor sort by number_of_projects  desc limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Problem Statement 6
-- MAGIC Plot number of completed studies each month in a given year.

-- COMMAND ----------

--2019
select Completion, count(*) from ct2019 where Status='Completed' and Completion like '%2019' group by Completion  order by from_unixtime(unix_timestamp(Completion, 'MMM yyyy')) asc;
--2020
select Completion, count(*) from ct2020 where Status='Completed' and Completion like '%2020' group by Completion  order by from_unixtime(unix_timestamp(Completion, 'MMM yyyy')) asc;
--2021
select Completion, count(*) from ct2021 where Status='Completed' and Completion like '%2021' group by Completion  order by from_unixtime(unix_timestamp(Completion, 'MMM yyyy')) asc;

-- COMMAND ----------

--2019
select Completion, count(*) from ct2019 where Status='Completed' and Completion like '%2019' group by Completion  order by from_unixtime(unix_timestamp(Completion, 'MMM yyyy')) asc;
--2020
select Completion, count(*) from ct2020 where Status='Completed' and Completion like '%2020' group by Completion  order by from_unixtime(unix_timestamp(Completion, 'MMM yyyy')) asc;
--2021
select Completion, count(*) from ct2021 where Status='Completed' and Completion like '%2021' group by Completion  order by from_unixtime(unix_timestamp(Completion, 'MMM yyyy')) asc;
