#	
# Main code	
#
import sys, os, json, datetime, time, urllib2, MySQLdb, calendar
db = MySQLdb.connect(host="localhost",
	user="jobstats",
	passwd="jobstats",
	db="jobstats")
cur = db.cursor()

mysql> describe metrics;
+------------------+--------------+------+-----+---------+-------+
| Field            | Type         | Null | Key | Default | Extra |
+------------------+--------------+------+-----+---------+-------+
| metric_name      | varchar(255) | YES  |     | NULL    |       |
| metric_date      | date         | YES  |     | NULL    |       |
| metric_hour      | tinyint(4)   | YES  |     | NULL    |       |
| metric_timestamp | bigint(20)   | YES  |     | NULL    |       |
| metric_value     | bigint(20)   | YES  |     | NULL    |       |
| posted           | tinyint(1)   | NO   |     | 0       |       |
+------------------+--------------+------+-----+---------+-------+


/* all jobs by the hour */
yarn.jobs.submitted

INSERT INTO metrics
SELECT 'yarn.jobs.submitted'
,job_sumbitTime_date
,job_sumbitTime_hour
,job_sumbitTime_hour_ts
,COUNT(job_id)
,0
FROM jobs
WHERE job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.jobs.submitted')
GROUP BY job_sumbitTime_hour_ts;


/* failed jobs by the hour */
yarn.jobs.failed

INSERT INTO metrics
SELECT 'yarn.jobs.failed'
,job_sumbitTime_date
,job_sumbitTime_hour
,job_sumbitTime_hour_ts
,COUNT(job_id)
,0
FROM jobs
WHERE job_state = 'FAILED' AND job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.jobs.failed')
GROUP BY job_sumbitTime_hour_ts;

/* killed jobs by the hour */
yarn.jobs.killed

INSERT INTO metrics
SELECT 'yarn.jobs.killed'
,job_sumbitTime_date
,job_sumbitTime_hour
,job_sumbitTime_hour_ts
,COUNT(job_id)
,0
FROM jobs
WHERE job_state IN ('KILLED', 'KILL_WAIT') AND job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.jobs.killed')
GROUP BY job_sumbitTime_hour_ts;

/* median run time by the hour */
yarn.jobs.median_run_time

INSERT INTO metrics
SELECT 'yarn.jobs.median_run_time'
,job_sumbitTime_date
,job_sumbitTime_hour
,job_sumbitTime_hour_ts
,get_median_run_time(job_sumbitTime_hour_ts)
,0
FROM jobs
WHERE job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.jobs.median_run_time')
GROUP BY job_sumbitTime_hour_ts;


/* median queue time by the hour */
yarn.jobs.median_queue_time

INSERT INTO metrics
SELECT 'yarn.jobs.median_queue_time'
,job_sumbitTime_date
,job_sumbitTime_hour
,job_sumbitTime_hour_ts
,get_median_queue_time(job_sumbitTime_hour_ts)
,0
FROM jobs
WHERE job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.jobs.median_queue_time')
GROUP BY job_sumbitTime_hour_ts;

/* max run time by the hour */
yarn.jobs.max_run_time

INSERT INTO metrics
SELECT 'yarn.jobs.max_run_time'
,job_sumbitTime_date
,job_sumbitTime_hour
,job_sumbitTime_hour_ts
,MAX(runTime)
,0
FROM jobs
WHERE job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.jobs.max_run_time')
GROUP BY job_sumbitTime_hour_ts;


/* max queue time by the hour */
yarn.jobs.max_queue_time

INSERT INTO metrics
SELECT 'yarn.jobs.max_queue_time'
,job_sumbitTime_date
,job_sumbitTime_hour
,job_sumbitTime_hour_ts
,MAX(queueTime)
,0
FROM jobs
WHERE job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.jobs.max_queue_time')
GROUP BY job_sumbitTime_hour_ts;

/* tasks submitted by the hour */
yarn.tasks.submitted

INSERT INTO metrics
SELECT 'yarn.tasks.submitted'
,j.job_sumbitTime_date
,j.job_sumbitTime_hour
,j.job_sumbitTime_hour_ts
,COUNT(*)
,0
FROM jobs j
INNER JOIN tasks t ON t.job_id = j.job_id
WHERE j.job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.tasks.submitted')
GROUP BY j.job_sumbitTime_hour_ts;

/* tasks completed by the hour */
yarn.tasks.completed

INSERT INTO metrics
SELECT 'yarn.tasks.completed'
,j.job_sumbitTime_date
,j.job_sumbitTime_hour
,j.job_sumbitTime_hour_ts
,COUNT(*)
,0
FROM jobs j
INNER JOIN tasks t ON t.job_id = j.job_id
WHERE t.state = 'SUCCEEDED' AND
j.job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.tasks.completed')
GROUP BY j.job_sumbitTime_hour_ts;

/* map tasks by the hour */
yarn.tasks.MAP

INSERT INTO metrics
SELECT 'yarn.tasks.MAP'
,j.job_sumbitTime_date
,j.job_sumbitTime_hour
,j.job_sumbitTime_hour_ts
,COUNT(*)
,0
FROM jobs j
INNER JOIN tasks t ON t.job_id = j.job_id
WHERE t.type = 'MAP' AND
j.job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.tasks.MAP')
GROUP BY j.job_sumbitTime_hour_ts;

/* reduce tasks by the hour */
yarn.tasks.REDUCE

INSERT INTO metrics
SELECT 'yarn.tasks.REDUCE'
,j.job_sumbitTime_date
,j.job_sumbitTime_hour
,j.job_sumbitTime_hour_ts
,COUNT(*)
,0
FROM jobs j
INNER JOIN tasks t ON t.job_id = j.job_id
WHERE t.type = 'REDUCE' AND
j.job_sumbitTime_hour_ts NOT IN (SELECT metric_timestamp FROM metrics WHERE metric_name = 'yarn.tasks.REDUCE')
GROUP BY j.job_sumbitTime_hour_ts;