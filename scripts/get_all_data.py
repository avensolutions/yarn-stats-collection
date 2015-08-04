def get_job_info( job_id ):
	job_info_req = "http://" + jobhist_uri + "/ws/v1/history/mapreduce/jobs/" + job_id
	job_info_resp = urllib2.urlopen(job_info_req)
	job_info_json_obj = json.load(job_info_resp)
	avgMapTime = job_info_json_obj['job']['avgMapTime']
	avgReduceTime = job_info_json_obj['job']['avgReduceTime']
	avgShuffleTime = job_info_json_obj['job']['avgShuffleTime']
	avgMergeTime = job_info_json_obj['job']['avgMergeTime']
	failedReduceAttempts = job_info_json_obj['job']['failedReduceAttempts']
	killedReduceAttempts = job_info_json_obj['job']['killedReduceAttempts']
	successfulReduceAttempts = job_info_json_obj['job']['successfulReduceAttempts']
	failedMapAttempts = job_info_json_obj['job']['failedMapAttempts']
	killedMapAttempts = job_info_json_obj['job']['killedMapAttempts']
	successfulMapAttempts = job_info_json_obj['job']['successfulMapAttempts']
	# insert results into job_info table
	sql = "INSERT IGNORE INTO job_info SELECT '" + job_id + "'," + str(avgMapTime) + "," + str(avgReduceTime) + "," + str(avgShuffleTime) + "," + str(avgMergeTime) + "," + str(failedReduceAttempts) + "," + str(killedReduceAttempts) + "," + str(successfulReduceAttempts) + "," + str(failedMapAttempts) + "," + str(killedMapAttempts) + "," + str(successfulMapAttempts)
	cur.execute(sql)
	return;

def get_job_conf( job_id ):
	conf_req = "http://" + jobhist_uri + "/ws/v1/history/mapreduce/jobs/" + job_id + "/conf"
	conf_resp = urllib2.urlopen(conf_req)
	conf_json_obj = json.load(conf_resp)
	if conf_json_obj.has_key('conf'):
		conf_obj = conf_json_obj['conf']
		if conf_obj.has_key('property'):
			property_obj = conf_obj['property']
			if property_obj is not None:
				for i in property_obj:
					name = i['name']
					value = i['value']
					# insert results into task_counters table
					sql = "INSERT IGNORE INTO task_counters SELECT '" + job_id + "','" + task_id + "','" + counterGroupName + "','" + counter + "'," + str(value)
					cur.execute(sql)
	return;

	CREATE TABLE IF NOT EXISTS job_conf
	(
	job_id varchar(255) PRIMARY KEY
	,propertyName varchar(1024)
	,propertyValue varchar(61440)
	);


	
def get_task_info( job_id ):	
	tasks_req = "http://" + jobhist_uri + "/ws/v1/history/mapreduce/jobs/" + job_id + "/tasks"
	tasks_resp = urllib2.urlopen(tasks_req)
	tasks_json_obj = json.load(tasks_resp)
	if tasks_json_obj.has_key('tasks'):
		tasks_obj = tasks_json_obj['tasks']
		if tasks_obj is not None:
			if tasks_obj.has_key('task'):
				task_obj = tasks_obj['task']
				if task_obj is not None:
					for i in task_obj:
						task_id = i['id']
						task_startTime = i['startTime']
						task_startTime_HR = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(task_startTime/1000))
						elapsedTime = i['elapsedTime']
						runTime = elapsedTime/1000
						progress = i['progress']
						state = i['state']
						type = i['type']
						successfulAttempt = i['successfulAttempt']
						numberOfAttemptsList = successfulAttempt.split('_')
						if len(numberOfAttemptsList) > 4:
							numberOfAttempts = numberOfAttemptsList[5]
						else:
							numberOfAttempts = -1
						# insert results into tasks table
						sql = "INSERT IGNORE INTO tasks SELECT '" + task_id + "','" + task_startTime_HR + "'," + str(runTime) + "," + str(progress) + ",'" + state + "','" + type + "'," + str(numberOfAttempts)
						cur.execute(sql)
						# get task_counters
						task_req = "http://" + jobhist_uri + "/ws/v1/history/mapreduce/jobs/" + job_id + "/tasks/" + task_id + '/counters'
						task_resp = urllib2.urlopen(task_req)
						task_json_obj = json.load(task_resp)

						if task_json_obj.has_key('jobTaskCounters'):
							jobTaskCounters = task_json_obj['jobTaskCounters']
							if jobTaskCounters.has_key('taskCounterGroup'):
								taskCounterGroup = jobTaskCounters['taskCounterGroup']
								for i in taskCounterGroup:
									counterGroupName = i['counterGroupName']
									for ii in i['counter']:
										counter = ii['name']
										value = ii['value']
										# insert results into task_counters table
										sql = "INSERT IGNORE INTO task_counters SELECT '" + job_id + "','" + task_id + "','" + counterGroupName + "','" + counter + "'," + str(value)
										cur.execute(sql)
	return;	
	
	
#	
# Main code	
#
import sys, os, json, time, urllib2, MySQLdb
db = MySQLdb.connect(host="localhost",
	user="jobstats",
	passwd="jobstats",
	db="jobstats")
cur = db.cursor()
# get jobs
jobhist_uri = str(sys.argv[1])
show_progress = str(sys.argv[2])
jobs_req = "http://" + jobhist_uri + "/ws/v1/history/mapreduce/jobs"
jobs_resp = urllib2.urlopen(jobs_req)
jobs_json_obj = json.load(jobs_resp)
no_jobs = len(jobs_json_obj['jobs']['job'])
counter = 1
for i in jobs_json_obj['jobs']['job']:
	job_state = i['state']
	valid_states = ["SUCCEEDED", "FAILED", "KILL_WAIT", "KILLED"]
	if job_state in valid_states:
		job_submitTime = i['submitTime']
		job_sumbitTime_HR = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job_submitTime/1000))
		job_startTime = i['startTime']
		job_finishTime = i['finishTime']
		job_id = i['id']
		job_name = i['name'].replace("'", "")
		queue = i['queue']
		user = i['user']
		mapsTotal = i['mapsTotal']
		mapsCompleted = i['mapsCompleted']
		reducesTotal = i['reducesTotal']
		reducesCompleted = i['reducesCompleted']
		queueTime = (job_startTime - job_submitTime)/1000
		runTime = (job_finishTime - job_startTime)/1000
		# insert results into jobs table
		sql = "INSERT IGNORE INTO jobs SELECT '" + job_id + "','" + job_sumbitTime_HR + "','" + job_name + "','" + queue + "','" + user + "','" + job_state + "'," + str(queueTime) + "," + str(runTime) + "," + str(mapsTotal) + "," + str(mapsCompleted) + "," + str(reducesTotal) + "," + str(reducesCompleted)
		cur.execute(sql)
		# get job_info
		get_job_info(job_id)
		# get job conf
		#get_job_conf(job_id)
		# get tasks and task counters
		get_task_info(job_id)
		if show_progress == 'true':
			sys.stdout.write("\r" + str(counter) + "/" + str(no_jobs))
			sys.stdout.flush()
		counter = counter + 1
cur.close()
db.close()
