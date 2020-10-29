-- Create database if not existing
CREATE DATABASE IF NOT EXISTS weblogDB;

-- Switch to the database
USE weblogDB;

-- Drop pre-existing tables
DROP TABLE IF EXISTS weblogdata;
DROP TABLE IF EXISTS weblogdata_sub;

-- Create new table from file
CREATE TABLE weblogdata (
	ip STRING,
	date_d STRING,
	time_d STRING,
	zone INT,
	cik INT,
	accession STRING,
	extention STRING,
	code INT,
	size INT,
	idx INT,
	norefer INT,
	noagent INT,
	find INT,
	crawler INT,
	browser STRING)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- Load data into table
LOAD DATA LOCAL INPATH '/home/cloudera/workspace/VM_Shared/Data_sec_gov_Reduced.csv'
	OVERWRITE INTO TABLE weblogdata;

-- Subtables with meaningful columns
CREATE TABLE weblogdata_sub
	AS SELECT ip, extention, size
	FROM weblogdata;

	
-- 1) NUMBER OF REQUESTS FOR IP [IP WITH MOST REQUESTS AND NUMBER OF BYTES REQUESTED]
INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/workspace/Hadoop/Hive/out1' 
SELECT ip, count(*) as cnt, sum(size) FROM weblogdata_sub
	GROUP BY ip
	SORT BY cnt DESC;


-- 2) NUMBER OF REQUEST FOR DOCUMENT [MOST SEARCHED DOCUMENT]
INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/workspace/Hadoop/Hive/out2' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT extention, count(*) as cnt2 FROM weblogdata_sub
	GROUP BY extention
	SORT BY cnt2 DESC;


-- 3) NUMBER REQUESTS FOR SPECIFIC DOCUMENT FOR IP 
INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/workspace/Hadoop/Hive/out3' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT ip, extention, count(*) as cnt3 FROM weblogdata_sub
	GROUP BY ip, extention
	SORT BY cnt3 DESC;

