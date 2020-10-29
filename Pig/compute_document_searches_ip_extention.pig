-- Load data from HDFS specifying a schema
data_input = LOAD '/home/cloudera/workspace/VM_Shared/Data_sec_gov_Reduced.csv' USING PigStorage(',') as(ip:chararray, date_d:chararray, time_d:chararray, zone:int, cik:int, accession:chararray, extention:chararray, code:int, size:int, idx:int, 		norefer:int, noagent:int, find:int, crawler:int, browser:chararray);

-- Delete attributes from initial dataset I'm not interested into for the analysis
data_filtered = FOREACH data_input GENERATE ip, extention, size;


-- COMPUTE NUMBER OF REQUESTS FOR EACH IP ADDRESS [IP WITH HIGHEST REQUESTS NUMBER AND HIGHEST BYTES NUMBER]
-- Grouping tuples with same IP
data_grouped = GROUP data_filtered BY ip;
-- Compute number of searches and number of total bytes for IP 
number_searches = FOREACH data_grouped GENERATE group as ip, COUNT(data_filtered) as count, SUM(data_filtered.size) as sum;
num_ric = FOREACH number_searches GENERATE ip, count, sum;
-- Sorting
num_ric_ordered = ORDER num_ric BY count desc;
STORE num_ric_ordered INTO '/home/cloudera/workspace/Hadoop/Pig/output1/' USING PigStorage(',');


-- COMPUTE NUMBER OF SEARCHES FOR DOCUMENT [MOST SEARCHED DOCUMENT]
-- Group tuples with same searched document 
documents_requested = GROUP data_filtered BY extention;
-- Compute the number of searches for document for IP
number_requests = FOREACH documents_requested GENERATE group as extention, COUNT(data_filtered) as count2;
num_req = FOREACH number_requests GENERATE extention, count2;
-- Sorting 
num_req_ordered = ORDER num_req BY count2 desc;
STORE num_req_ordered INTO '/home/cloudera/workspace/Hadoop/Pig/output2/' USING PigStorage(',');


-- COMPUTE NUMBER OF SEACRHES FOR SPECIFIC DOCUMENT FOR IP
-- Group data (already grouped per IP) for extention
data_grouped2 = GROUP data_filtered BY (ip, extention);
-- Compute number of searches
number_searches2 = FOREACH data_grouped2 GENERATE FLATTEN(group) as (ip, extention), COUNT(data_filtered) as count3;
STORE number_searches2 INTO '/home/cloudera/workspace/Hadoop/Pig/output3/' USING PigStorage(',');


