# yarn-stats-collection

Python scripts used to collect YARN job and task statistics and counters from a Hadoop cluster and store the information in a MySQL database.

## Prerequisites

The following tables need to be created in MySQL in a database named 'jobstats':

	USE jobstats;
	CREATE TABLE jobs 
	(
	job_id varchar(255) PRIMARY KEY
	,job_sumbitTime datetime
	,job_name varchar(255)
	,queue varchar(255)
	,job_user varchar(255)
	,job_state varchar(255)
	,queueTime int
	,runTime int
	,mapsTotal int
	,mapsCompleted int
	,reducesTotal int
	,reducesCompleted int
	);
	
	CREATE TABLE job_info
	(
	job_id varchar(255) PRIMARY KEY
	,avgMapTime int
	,avgReduceTime int
	,avgShuffleTime int
	,avgMergeTime int
	,failedReduceAttempts int
	,killedReduceAttempts int
	,successfulReduceAttempts int
	,failedMapAttempts int
	,killedMapAttempts int
	,successfulMapAttempts int
	);
	
	CREATE TABLE tasks
	(
	task_id varchar(255) PRIMARY KEY
	,task_startTime datetime
	,runTime int
	,progress int
	,state varchar(255) 
	,type varchar(255)
	,numberOfAttempts int
	);
	
	CREATE TABLE task_counters
	(
	job_id varchar(255)
	,task_id varchar(255)
	,counterGroupName varchar(255)
	,counter varchar(255)
	,value int
	);

	ALTER TABLE task_counters ADD INDEX task_id_idx (task_id);
	ALTER TABLE task_counters ADD INDEX job_id_idx (job_id);

## Usage

	sh start-here.sh <job_history_server_uri>

eg

	sh start-here.sh resourcemanager.hadoop.local:19888