# yarn-stats-collection

Python scripts used to collect YARN job and task statistics and counters from a Hadoop cluster, store the information in a MySQL database and post defined metrics to DataDog.

## Prerequisites

The following tables need to be created in MySQL in a database named 'jobstats':

	USE jobstats;
	CREATE TABLE IF NOT EXISTS jobs 
	(
	job_id varchar(255) PRIMARY KEY
	,job_sumbitTime datetime
	,job_sumbitTime_date date
	,job_sumbitTime_hour int
	,job_sumbitTime_hour_ts bigint
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
	
	CREATE TABLE IF NOT EXISTS job_info
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

	CREATE TABLE IF NOT EXISTS tasks
	(
	task_id varchar(255) PRIMARY KEY
	,job_id varchar(255)
	,task_startTime datetime
	,runTime int
	,progress int
	,state varchar(255) 
	,type varchar(255)
	,numberOfAttempts int
	);

	CREATE TABLE IF NOT EXISTS task_counters
	(
	job_id varchar(255)
	,task_id varchar(255)
	,counterGroupName varchar(255)
	,counter varchar(255)
	,value bigint
	,PRIMARY KEY (job_id, task_id, counter)
	);

	ALTER TABLE task_counters ADD INDEX task_id_idx (task_id);
	ALTER TABLE task_counters ADD INDEX job_id_idx (job_id);
	ALTER TABLE task_counters ADD INDEX counter_idx (counter);

	CREATE TABLE IF NOT EXISTS job_conf
	(
	job_id varchar(255)
	,propertyName varchar(512)
	,propertyValue varchar(61440)
	,PRIMARY KEY (job_id, propertyName)
	);
	
	ALTER TABLE job_conf ADD INDEX job_conf_job_id_idx (job_id);
	
	CREATE TABLE metrics
	(
	metric_name VARCHAR(255)
	,metric_date date
	,metric_hour tinyint
	,metric_timestamp bigint
	,metric_value bigint
	,posted boolean NOT NULL default 0
	);

	CREATE FUNCTION get_median_run_time (v_job_sumbitTime_hour_ts BIGINT)
	RETURNS FLOAT DETERMINISTIC
	RETURN 
	(
	SELECT AVG(t1.runTime) FROM (
	SELECT @rownum:=@rownum+1 as `row_number`, d.runTime, d.job_sumbitTime_hour_ts
	  FROM jobs d,  (SELECT @rownum:=0) r
	  WHERE job_sumbitTime_hour_ts = v_job_sumbitTime_hour_ts
	  ORDER BY d.runTime
	) as t1, 
	(
	  SELECT count(*) as total_rows
	  FROM jobs d WHERE job_sumbitTime_hour_ts = v_job_sumbitTime_hour_ts
	) as t2
	WHERE t1.job_sumbitTime_hour_ts = v_job_sumbitTime_hour_ts AND
	t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) )
	);	
	
	CREATE FUNCTION get_median_queue_time (v_job_sumbitTime_hour_ts BIGINT)
	RETURNS FLOAT DETERMINISTIC
	RETURN 
	(
	SELECT AVG(t1.queueTime) FROM (
	SELECT @rownum:=@rownum+1 as `row_number`, d.queueTime, d.job_sumbitTime_hour_ts
	  FROM jobs d,  (SELECT @rownum:=0) r
	  WHERE job_sumbitTime_hour_ts = v_job_sumbitTime_hour_ts
	  ORDER BY d.queueTime
	) as t1, 
	(
	  SELECT count(*) as total_rows
	  FROM jobs d WHERE job_sumbitTime_hour_ts = v_job_sumbitTime_hour_ts
	) as t2
	WHERE t1.job_sumbitTime_hour_ts = v_job_sumbitTime_hour_ts AND
	t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) )
	);
	
## Usage

	sh start-here.sh <job_history_server_uri>

eg

	sh start-here.sh resourcemanager.hadoop.local:19888