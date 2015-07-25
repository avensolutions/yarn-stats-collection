import sys, json, time, urllib2, MySQLdb, subprocess
db = MySQLdb.connect(host="localhost",
                     user="jobstats",
                      passwd="jobstats",
                      db="jobstats")
cur = db.cursor()
jobhist_uri = str(sys.argv[1])
jobs_req = "http://" + jobhist_uri + "/ws/v1/history/mapreduce/jobs"
jobs_resp = urllib2.urlopen(jobs_req)
jobs_json_obj = json.load(jobs_resp)
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
		subprocess.Popen(["python", "job_info.py", jobhist_uri, job_id])
		# get tasks
		subprocess.Popen(["python", "tasks.py", jobhist_uri, job_id])